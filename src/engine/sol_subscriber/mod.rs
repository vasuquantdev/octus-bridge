use std::collections::VecDeque;
use std::str::FromStr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use futures::StreamExt;
use solana_account_decoder::{UiAccountData, UiAccountEncoding};
use tokio::sync::{mpsc, Notify};
use tokio::time::timeout;

use solana_client::nonblocking::pubsub_client::PubsubClient;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_client::rpc_config::{RpcAccountInfoConfig, RpcProgramAccountsConfig};
use solana_client::rpc_response::{Response, RpcKeyedAccount};
use solana_sdk::account::{Account, ReadableAccount};
use solana_sdk::hash::Hash;
use solana_sdk::program_pack::Pack;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signature;
use solana_sdk::transaction::Transaction;

use crate::config::*;
use crate::engine::bridge::*;
use crate::utils::*;

pub struct SolSubscriber {
    config: SolConfig,
    rpc_client: Arc<RpcClient>,
    pubsub_client: Arc<PubsubClient>,
    pending_events: tokio::sync::Mutex<VecDeque<(Pubkey, PendingEvent)>>,
    pending_event_count: AtomicUsize,
    new_events_notify: Notify,
}

impl SolSubscriber {
    pub async fn new(config: SolConfig) -> Result<Arc<Self>> {
        let rpc_client = Arc::new(RpcClient::new_with_commitment(
            config.url.clone(),
            config.commitment_config,
        ));

        let pubsub_client = Arc::new(PubsubClient::new(&config.ws_url).await?);

        let subscriber = Arc::new(Self {
            config,
            rpc_client,
            pubsub_client,
            pending_events: Default::default(),
            pending_event_count: Default::default(),
            new_events_notify: Notify::new(),
        });

        Ok(subscriber)
    }

    pub fn metrics(&self) -> SolSubscriberMetrics {
        SolSubscriberMetrics {
            pending_confirmation_count: self.pending_event_count.load(Ordering::Acquire),
        }
    }

    pub fn start(self: &Arc<Self>) {
        let (tx, rx) = mpsc::unbounded_channel();

        for program_id in self
            .config
            .program_ids
            .iter()
            .map(|program_id| Pubkey::from_str(program_id).unwrap())
        {
            let tx = tx.clone();
            let subscriber = Arc::downgrade(self);

            tokio::spawn(async move {
                let subscriber = match subscriber.upgrade() {
                    Some(subscriber) => subscriber,
                    None => return,
                };

                if let Err(e) = subscriber.subscribe(program_id, tx).await {
                    log::error!("Error occurred during Solana subscribe: {:?}", e);
                }
            });
        }

        let subscriber = Arc::downgrade(self);

        tokio::spawn(async move {
            let subscriber = match subscriber.upgrade() {
                Some(subscriber) => subscriber,
                None => return,
            };

            if let Err(e) = subscriber.handle(rx).await {
                log::error!("Error occurred during Solana event handle: {:?}", e);
            }
        });
    }

    async fn subscribe(&self, program_id: Pubkey, tx: SubscribeResponseTx) -> Result<()> {
        let (mut program_notifications, program_unsubscribe) = self
            .pubsub_client
            .program_subscribe(
                &program_id,
                Some(RpcProgramAccountsConfig {
                    account_config: RpcAccountInfoConfig {
                        commitment: Some(self.config.commitment_config),
                        encoding: Some(UiAccountEncoding::Base64),
                        ..RpcAccountInfoConfig::default()
                    },
                    ..RpcProgramAccountsConfig::default()
                }),
            )
            .await?;

        while let Some(response) = program_notifications.next().await {
            tx.send(response)?;
        }

        program_unsubscribe().await;

        Ok(())
    }

    async fn handle(&self, mut rx: SubscribeResponseRx) -> Result<()> {
        while let Some(response) = rx.recv().await {
            if let UiAccountData::Binary(s, UiAccountEncoding::Base64) = response.value.account.data
            {
                if let Ok(bytes) = base64::decode(s) {
                    if let Ok(account_data) = solana_bridge::bridge_state::Proposal::unpack(&bytes)
                    {
                        if account_data.account_kind
                            == solana_bridge::bridge_state::AccountKind::Proposal
                        {
                            let account_id = match Pubkey::from_str(&response.value.pubkey) {
                                Ok(account_id) => account_id,
                                Err(err) => {
                                    log::error!(
                                        "Failed to parse Solana account {}: {:?}",
                                        &response.value.pubkey,
                                        err
                                    );
                                    continue;
                                }
                            };

                            let mut pending_events = self.pending_events.lock().await;

                            pending_events.push_back((
                                account_id,
                                PendingEvent {
                                    author: account_data.pda.author,
                                    settings: account_data.pda.settings,
                                    event_timestamp: account_data.pda.event_timestamp,
                                    event_transaction_lt: account_data.pda.event_transaction_lt,
                                    event_configuration: account_data.pda.event_configuration,
                                    event_data: account_data.event,
                                },
                            ));

                            self.pending_event_count
                                .store(pending_events.len(), Ordering::Release);

                            self.new_events_notify.notify_waiters();
                        }
                    }
                }
            }
        }

        Ok(())
    }

    async fn get_account_with_commitment(
        &self,
        account_pubkey: &Pubkey,
    ) -> Result<Option<Account>> {
        self.rpc_client
            .get_account_with_commitment(account_pubkey, self.config.commitment_config)
            .await
            .map(|response| response.value)
            .map_err(anyhow::Error::new)
    }

    async fn get_latest_blockhash(&self) -> Result<Hash> {
        self.rpc_client
            .get_latest_blockhash()
            .await
            .map_err(anyhow::Error::new)
    }

    async fn send_and_confirm_transaction(&self, transaction: &Transaction) -> Result<Signature> {
        self.rpc_client
            .send_and_confirm_transaction(transaction)
            .await
            .map_err(anyhow::Error::new)
    }

    async fn get_account(&self, pubkey: &Pubkey) -> Result<Option<Account>> {
        let account = {
            retry(
                || {
                    timeout(
                        Duration::from_secs(self.config.get_timeout_sec),
                        self.get_account_with_commitment(pubkey),
                    )
                },
                generate_default_timeout_config(Duration::from_secs(
                    self.config.maximum_failed_responses_time_sec,
                )),
                "get account",
            )
            .await
            .context("Timed out getting account")?
            .context("Failed getting account")?
        };

        Ok(account)
    }

    pub async fn pending_events_notified(&self) {
        self.new_events_notify.notified().await
    }

    pub async fn pending_events_is_empty(&self) -> bool {
        self.pending_events.lock().await.is_empty()
    }

    pub async fn pending_events_pop_front(&self) -> Option<(Pubkey, PendingEvent)> {
        self.pending_events.lock().await.pop_front()
    }

    pub async fn pending_events_push_back(&self, value: (Pubkey, PendingEvent)) {
        self.pending_events.lock().await.push_back(value)
    }

    pub async fn verify(
        &self,
        seed: u128,
        program_id: Pubkey,
        settings_address: Pubkey,
        event_data: Vec<u8>,
    ) -> Result<VerificationStatus> {
        let account_pubkey = solana_bridge::token_proxy::get_associated_deposit_address(
            &program_id,
            seed,
            &settings_address,
        );

        let result = self.get_account(&account_pubkey).await?;
        let account = match result {
            Some(account) => account,
            None => {
                log::error!("Solana account 0x{} not exist", account_pubkey);
                return Ok(VerificationStatus::NotExists);
            }
        };

        let account_data = solana_bridge::token_proxy::Deposit::unpack(account.data())?;
        if event_data != account_data.event {
            return Ok(VerificationStatus::NotExists);
        }

        Ok(VerificationStatus::Exists)
    }

    pub async fn get_recent_blockhash(&self) -> Result<Hash> {
        let api_request_strategy = generate_fixed_timeout_config(
            Duration::from_secs(self.config.get_timeout_sec),
            Duration::from_secs(self.config.maximum_failed_responses_time_sec),
        );

        let latest_blockhash = match retry(
            || self.get_latest_blockhash(),
            api_request_strategy,
            "get latest blockhash",
        )
        .await
        {
            Ok(latest_blockhash) => latest_blockhash,
            Err(e) => {
                return Err(e).with_context(|| "Failed to get latest Solana blockhash".to_string())
            }
        };

        Ok(latest_blockhash)
    }

    pub async fn send_transaction(&self, transaction: Transaction) -> Result<()> {
        // Prepare tryhard config
        let api_request_strategy = generate_fixed_timeout_config(
            Duration::from_secs(self.config.get_timeout_sec),
            Duration::from_secs(self.config.maximum_failed_responses_time_sec),
        );

        match retry(
            || self.send_and_confirm_transaction(&transaction),
            api_request_strategy,
            "send solana transaction",
        )
        .await
        {
            Ok(_) => {}
            Err(e) => {
                return Err(e).with_context(|| "Failed to send Solana transaction".to_string())
            }
        };

        Ok(())
    }
}

#[derive(Debug, Copy, Clone)]
pub struct SolSubscriberMetrics {
    pub pending_confirmation_count: usize,
}

/*struct PendingEvents {
    events: tokio::sync::Mutex<VecDeque<(Pubkey, PendingEvent)>>,
    new_events_notify: Notify,
    count: AtomicUsize,
}

impl PendingEvents {
    async fn notified(&self) {
        self.new_events_notify.notified().await
    }

    async fn is_empty(&self) -> bool {
        self.events.lock().await.is_empty()
    }

    async fn pop_front(&self) -> Option<(Pubkey, PendingEventsData)> {
        self.events.lock().await.pop_front()
    }

    async fn push_back(&self, value: (Pubkey, PendingEventsData)) {
        self.events.lock().await.push_back(value)
    }
}*/

pub struct PendingEvent {
    pub author: Pubkey,
    pub settings: Pubkey,
    pub event_timestamp: u32,
    pub event_transaction_lt: u64,
    pub event_configuration: Pubkey,
    pub event_data: Vec<u8>,
}

type SubscribeResponseTx = mpsc::UnboundedSender<Response<RpcKeyedAccount>>;
type SubscribeResponseRx = mpsc::UnboundedReceiver<Response<RpcKeyedAccount>>;
