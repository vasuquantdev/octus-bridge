use std::collections::hash_map;
use std::str::FromStr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use futures::StreamExt;
use tokio::sync::{mpsc, oneshot, Notify};
use tokio::time::timeout;

use solana_account_decoder::{UiAccountData, UiAccountEncoding};
use solana_client::nonblocking::pubsub_client::PubsubClient;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_client::rpc_config::{RpcAccountInfoConfig, RpcProgramAccountsConfig};
use solana_sdk::account::{Account, ReadableAccount};
use solana_sdk::hash::Hash;
use solana_sdk::program_pack::Pack;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signature;
use solana_sdk::transaction::Transaction;
use tiny_adnl::utils::FxHashMap;

use crate::config::*;
use crate::engine::bridge::*;
use crate::engine::ton_subscriber::*;
use crate::utils::*;

pub struct SolSubscriber {
    config: SolConfig,
    rpc_client: Arc<RpcClient>,
    pending_events: tokio::sync::Mutex<FxHashMap<Pubkey, PendingEvent>>,
    pending_events_count: AtomicUsize,
    new_events_notify: Notify,
}

impl SolSubscriber {
    pub async fn new(config: SolConfig) -> Result<Arc<Self>> {
        let rpc_client = Arc::new(RpcClient::new_with_commitment(
            config.endpoint.clone(),
            config.commitment,
        ));

        let subscriber = Arc::new(Self {
            config,
            rpc_client,
            pending_events: Default::default(),
            pending_events_count: Default::default(),
            new_events_notify: Notify::new(),
        });

        Ok(subscriber)
    }

    pub fn start(self: &Arc<Self>) {
        // Subscribe to programs and start listening events
        let (event_tx, event_rx) = mpsc::unbounded_channel();

        for program_id in &self.config.programs {
            let tx = event_tx.clone();
            self.subscribe(*program_id, tx);
        }

        start_listening_events(self, "SolProgram", event_rx, Self::process_program_event);

        // Start node subscriber update
        let subscriber = Arc::downgrade(self);

        tokio::spawn(async move {
            let subscriber = match subscriber.upgrade() {
                Some(subscriber) => subscriber,
                None => return,
            };

            if let Err(e) = subscriber.update().await {
                log::error!(
                    "Error occurred during Solana node subscriber update: {:?}",
                    e
                );
            }
        });
    }

    fn subscribe(self: &Arc<Self>, program_id: Pubkey, tx: SubscribeResponseTx) {
        let subscriber = Arc::downgrade(self);

        tokio::spawn(async move {
            loop {
                let subscriber = match subscriber.upgrade() {
                    Some(subscriber) => subscriber,
                    None => return,
                };

                if let Err(e) = subscriber.program_subscribe(&program_id, &tx).await {
                    log::error!("Error occurred during Solana subscribe: {:?}", e);

                    let timeout = Duration::from_secs(subscriber.config.reconnect_timeout_sec);
                    tokio::time::sleep(timeout).await;
                }
            }
        });
    }

    async fn program_subscribe(&self, program_id: &Pubkey, tx: &SubscribeResponseTx) -> Result<()> {
        let pubsub_client = Arc::new(PubsubClient::new(&self.config.ws_endpoint).await?);

        let (mut program_notifications, program_unsubscribe) = pubsub_client
            .program_subscribe(
                program_id,
                Some(RpcProgramAccountsConfig {
                    account_config: RpcAccountInfoConfig {
                        commitment: Some(self.config.commitment),
                        encoding: Some(UiAccountEncoding::Base64),
                        ..RpcAccountInfoConfig::default()
                    },
                    ..RpcProgramAccountsConfig::default()
                }),
            )
            .await?;

        log::info!("Start listening Solana program {}", program_id);

        while let Some(response) = program_notifications.next().await {
            if let UiAccountData::Binary(s, UiAccountEncoding::Base64) = response.value.account.data
            {
                if let Ok(bytes) = base64::decode(s) {
                    if let Ok(account_data) =
                        solana_bridge::bridge_state::Proposal::unpack_from_slice(&bytes)
                    {
                        if account_data
                            .signers
                            .iter()
                            .filter(|vote| **vote != solana_bridge::bridge_types::Vote::None)
                            .count()
                            > 0
                        {
                            let account_id = match Pubkey::from_str(&response.value.pubkey) {
                                Ok(account_id) => account_id,
                                Err(err) => {
                                    log::error!(
                                        "Failed to parse Solana account {} received from program {}: {:?}",
                                        response.value.pubkey,
                                        program_id,
                                        err
                                    );
                                    continue;
                                }
                            };

                            tx.send((account_id, account_data.event)).ok();
                        }
                    }
                }
            }
        }

        program_unsubscribe().await;

        log::warn!("Stop listening Solana program {}", program_id);

        Ok(())
    }

    async fn process_program_event(
        self: Arc<Self>,
        (account, event_data): (Pubkey, Vec<u8>),
    ) -> Result<()> {
        let mut pending_events = self.pending_events.lock().await;
        if let Some(pending_event) = pending_events.get_mut(&account) {
            if pending_event.status == PendingEventStatus::InProcess {
                pending_event.status = pending_event.check(event_data).into();
            }
        }

        Ok(())
    }

    async fn update(&self) -> Result<()> {
        if self.pending_events.lock().await.is_empty() {
            // Wait until new events appeared or idle poll interval passed.
            tokio::select! {
                _ = self.new_events_notify.notified() => {},
                _ = tokio::time::sleep(Duration::from_secs(self.config.poll_interval_sec)) => {},
            }
        }

        log::info!(
            "TON->SOL pending events: {}",
            self.pending_events_count.load(Ordering::Acquire)
        );

        let time = chrono::Utc::now().timestamp() as u64;

        let mut pending_events = self.pending_events.lock().await;

        let accounts_to_check = futures::stream::FuturesUnordered::new();
        pending_events.retain(|&account, event| {
            let status = match event.status {
                PendingEventStatus::InProcess => {
                    if time > event.time {
                        event.time = time + 3600; // Shift to 1 hour
                        accounts_to_check.push(async move {
                            let result = self.get_account(&account).await;
                            (account, result)
                        });
                    }
                    return true;
                }
                PendingEventStatus::Valid => VerificationStatus::Exists,
                PendingEventStatus::Invalid => VerificationStatus::NotExists,
            };

            log::info!("Confirmation status: {:?}", status);

            if let Some(tx) = event.status_tx.take() {
                tx.send(status).ok();
            }

            false
        });

        let accounts_to_check = accounts_to_check
            .collect::<Vec<(Pubkey, Result<Option<Account>>)>>()
            .await;

        log::info!("Accounts to check: {:?}", accounts_to_check);

        for (account, result) in accounts_to_check {
            if let hash_map::Entry::Occupied(mut entry) = pending_events.entry(account) {
                let status = match result {
                    Ok(Some(account)) => {
                        let account_data =
                            solana_bridge::bridge_state::Proposal::unpack_from_slice(
                                account.data(),
                            )?;
                        entry.get().check(account_data.event)
                    }
                    Ok(None) => VerificationStatus::NotExists,
                    Err(e) => {
                        log::error!("Failed to check Solana event: {:?}", e);
                        continue;
                    }
                };

                if let Some(tx) = entry.get_mut().status_tx.take() {
                    tx.send(status).ok();
                }

                entry.remove();
            }
        }

        self.pending_events_count
            .store(pending_events.len(), Ordering::Release);

        drop(pending_events);

        Ok(())
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

    async fn get_account_with_commitment(
        &self,
        account_pubkey: &Pubkey,
    ) -> Result<Option<Account>> {
        self.rpc_client
            .get_account_with_commitment(account_pubkey, self.config.commitment)
            .await
            .map(|response| response.value)
            .map_err(anyhow::Error::new)
    }

    pub async fn verify_ton_sol_event(
        &self,
        account_pubkey: Pubkey,
        event_data: Vec<u8>,
    ) -> Result<VerificationStatus> {
        let rx = {
            let mut pending_events = self.pending_events.lock().await;

            let (tx, rx) = oneshot::channel();

            pending_events.insert(
                account_pubkey,
                PendingEvent {
                    event_data,
                    status_tx: Some(tx),
                    status: PendingEventStatus::InProcess,
                    time: 0,
                },
            );

            self.pending_events_count
                .store(pending_events.len(), Ordering::Release);

            self.new_events_notify.notify_waiters();

            rx
        };

        let status = rx.await?;
        Ok(status)
    }

    pub async fn verify_sol_ton_event(
        &self,
        account_pubkey: Pubkey,
        event_data: Vec<u8>,
    ) -> Result<VerificationStatus> {
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

struct PendingEvent {
    event_data: Vec<u8>,
    status: PendingEventStatus,
    status_tx: Option<VerificationStatusTx>,
    time: u64,
}

impl PendingEvent {
    fn check(&self, event_data: Vec<u8>) -> VerificationStatus {
        if self.event_data == event_data {
            VerificationStatus::Exists
        } else {
            VerificationStatus::NotExists
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
enum PendingEventStatus {
    InProcess,
    Valid,
    Invalid,
}

impl From<VerificationStatus> for PendingEventStatus {
    fn from(status: VerificationStatus) -> Self {
        match status {
            VerificationStatus::Exists => Self::Valid,
            VerificationStatus::NotExists => Self::Invalid,
        }
    }
}

type VerificationStatusTx = oneshot::Sender<VerificationStatus>;

type SubscribeResponseTx = mpsc::UnboundedSender<(Pubkey, Vec<u8>)>;
