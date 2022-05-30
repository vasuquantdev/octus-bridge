use std::collections::hash_map;
use std::str::FromStr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use futures::{StreamExt, TryFutureExt};
use tokio::sync::{mpsc, oneshot, Notify, Semaphore};
use tokio::time::timeout;

use solana_account_decoder::{UiAccountData, UiAccountEncoding};
use solana_client::client_error::{ClientError, ClientErrorKind};
use solana_client::nonblocking::pubsub_client::PubsubClient;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_client::rpc_config::{
    RpcAccountInfoConfig, RpcProgramAccountsConfig, RpcTransactionConfig,
};
use solana_sdk::account::{Account, ReadableAccount};
use solana_sdk::clock::{Slot, UnixTimestamp};
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::message::{Message, VersionedMessage};
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signature;
use solana_transaction_status::{EncodedConfirmedTransactionWithStatusMeta, UiTransactionEncoding};
use tiny_adnl::utils::FxHashMap;

use crate::config::*;
use crate::engine::bridge::*;
use crate::engine::keystore::*;
use crate::engine::ton_subscriber::*;
use crate::utils::*;

pub struct SolSubscriber {
    config: SolConfig,
    rpc_client: Arc<RpcClient>,
    pool: Arc<Semaphore>,
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

        let pool = Arc::new(Semaphore::new(config.pool_size));

        let subscriber = Arc::new(Self {
            config,
            rpc_client,
            pool,
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
            loop {
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
            }
        });
    }

    pub async fn verify_ton_sol_event(
        &self,
        account_pubkey: Pubkey,
        event_data: Vec<u8>,
    ) -> Result<(VerificationStatus, u32)> {
        let rx = {
            let mut pending_events = self.pending_events.lock().await;

            let (tx, rx) = oneshot::channel();

            pending_events.insert(
                account_pubkey,
                PendingEvent {
                    event_data,
                    round_number: None,
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

        let res = rx.await?;
        Ok(res)
    }

    pub async fn verify_sol_ton_event(
        &self,
        transaction_data: SolTonTransactionData,
        account_data: SolTonAccountData,
    ) -> Result<VerificationStatus> {
        if self.verify_sol_ton_transaction(transaction_data).await? == VerificationStatus::NotExists
        {
            return Ok(VerificationStatus::NotExists);
        }

        self.verify_sol_ton_account(account_data).await
    }

    pub async fn send_message(&self, message: Message, keystore: &Arc<KeyStore>) -> Result<()> {
        let _ = {
            let _permit = self.pool.acquire().await;

            retry(
                || {
                    timeout(Duration::from_secs(self.config.get_timeout_sec), async {
                        let message = message.clone();
                        self.send_and_confirm_message(message, keystore).await
                    })
                    .map_err(|err| {
                        ClientError::from(ClientErrorKind::Custom(format!(
                            "Timeout sending solana message: {}",
                            err
                        )))
                    })
                },
                generate_sol_rpc_backoff_config(Duration::from_secs(
                    self.config.maximum_failed_responses_time_sec,
                )),
                "send solana transaction",
            )
            .await
            .context("Timed out send solana message")?
            .context("Failed sending solana message")?
        };

        Ok(())
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
                        if account_data.is_initialized
                            && account_data.account_kind
                                == solana_bridge::bridge_state::AccountKind::Proposal
                            && account_data
                                .signers
                                .iter()
                                .all(|vote| *vote == solana_bridge::bridge_types::Vote::None)
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

                            tx.send((account_id, account_data.round_number, account_data.event))
                                .ok();
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
        (account, round_number, event_data): (Pubkey, u32, Vec<u8>),
    ) -> Result<()> {
        let mut pending_events = self.pending_events.lock().await;
        if let Some(pending_event) = pending_events.get_mut(&account) {
            if pending_event.status == PendingEventStatus::InProcess {
                pending_event.round_number = Some(round_number);
                pending_event.status = pending_event.check(event_data).into();
            }
        }

        Ok(())
    }

    async fn update(&self) -> Result<()> {
        // Wait until new events appeared or idle poll interval passed.
        tokio::select! {
            _ = self.new_events_notify.notified() => {},
            _ = tokio::time::sleep(Duration::from_secs(self.config.poll_interval_sec)) => {},
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
                // Always is initialized
                if let Some(round_number) = event.round_number {
                    tx.send((status, round_number)).ok();
                }
            }

            false
        });

        let accounts_to_check = accounts_to_check
            .collect::<Vec<(Pubkey, Result<Option<Account>>)>>()
            .await;

        log::info!("Accounts to check: {:?}", accounts_to_check);

        for (account, result) in accounts_to_check {
            if let hash_map::Entry::Occupied(mut entry) = pending_events.entry(account) {
                let (status, round_number) = match result {
                    Ok(Some(account)) => {
                        let account_data =
                            solana_bridge::bridge_state::Proposal::unpack_from_slice(
                                account.data(),
                            )?;
                        (
                            entry.get().check(account_data.event),
                            account_data.round_number,
                        )
                    }
                    Ok(None) => {
                        log::info!("Solana proposal account doesn't exist yet: {}", account);
                        continue;
                    }
                    Err(e) => {
                        log::error!("Failed to check Solana event: {:?}", e);
                        continue;
                    }
                };

                if let Some(tx) = entry.get_mut().status_tx.take() {
                    tx.send((status, round_number)).ok();
                }

                entry.remove();
            }
        }

        self.pending_events_count
            .store(pending_events.len(), Ordering::Release);

        drop(pending_events);

        Ok(())
    }

    async fn get_account(&self, account_pubkey: &Pubkey) -> Result<Option<Account>> {
        let account = {
            let _permit = self.pool.acquire().await;

            retry(
                || {
                    timeout(
                        Duration::from_secs(self.config.get_timeout_sec),
                        self.get_account_with_commitment(account_pubkey, self.config.commitment),
                    )
                },
                generate_default_timeout_config(Duration::from_secs(
                    self.config.maximum_failed_responses_time_sec,
                )),
                "get account",
            )
            .await
            .context("Timed out getting solana account")?
            .context("Failed getting solana account")?
        };

        Ok(account)
    }

    async fn get_transaction(
        &self,
        signature: &Signature,
    ) -> Result<EncodedConfirmedTransactionWithStatusMeta> {
        let transaction = {
            let _permit = self.pool.acquire().await;

            let config = RpcTransactionConfig {
                commitment: Some(self.config.commitment),
                encoding: Some(UiTransactionEncoding::Base64),
                ..Default::default()
            };

            retry(
                || {
                    timeout(
                        Duration::from_secs(self.config.get_timeout_sec),
                        self.get_transaction_with_config(signature, config),
                    )
                },
                generate_default_timeout_config(Duration::from_secs(
                    self.config.maximum_failed_responses_time_sec,
                )),
                "get solana transaction",
            )
            .await
            .context("Timed out getting solana transaction")?
            .context("Failed getting solana transaction")?
        };

        Ok(transaction)
    }

    async fn get_account_with_commitment(
        &self,
        account_pubkey: &Pubkey,
        commitment_config: CommitmentConfig,
    ) -> Result<Option<Account>, ClientError> {
        self.rpc_client
            .get_account_with_commitment(account_pubkey, commitment_config)
            .await
            .map(|response| response.value)
    }

    async fn get_transaction_with_config(
        &self,
        signature: &Signature,
        config: RpcTransactionConfig,
    ) -> Result<EncodedConfirmedTransactionWithStatusMeta, ClientError> {
        self.rpc_client
            .get_transaction_with_config(signature, config)
            .await
    }

    async fn verify_sol_ton_transaction(
        &self,
        data: SolTonTransactionData,
    ) -> Result<VerificationStatus> {
        let result = self.get_transaction(&data.signature).await?;

        if result.slot != data.slot || result.block_time != Some(data.block_time) {
            return Ok(VerificationStatus::NotExists);
        }

        let transaction = result.transaction.transaction.decode().ok_or_else(|| {
            SolSubscriberError::DecodeTransactionError(data.signature.to_string())
        })?;

        let (account_keys, instructions) = match transaction.message {
            VersionedMessage::Legacy(message) => (message.account_keys, message.instructions),
            VersionedMessage::V0(message) => (message.account_keys, message.instructions),
        };

        for ix in instructions {
            if account_keys[ix.program_id_index as usize] == data.program_id {
                let deposit_seed = u128::from_le_bytes(ix.data[1..17].try_into()?);
                if deposit_seed == data.seed {
                    return Ok(VerificationStatus::Exists);
                }
            }
        }

        Ok(VerificationStatus::NotExists)
    }

    async fn verify_sol_ton_account(&self, data: SolTonAccountData) -> Result<VerificationStatus> {
        let account_pubkey = solana_bridge::token_proxy::get_associated_deposit_address(
            &data.program_id,
            data.seed,
            &data.settings,
        );

        let result = self.get_account(&account_pubkey).await?;

        let account = match result {
            Some(account) => account,
            None => {
                log::error!("Solana account {} not exist", account_pubkey);
                return Ok(VerificationStatus::NotExists);
            }
        };

        let account_data = solana_bridge::token_proxy::Deposit::unpack_from_slice(account.data())?;
        if data.event_data != account_data.event {
            return Ok(VerificationStatus::NotExists);
        }

        Ok(VerificationStatus::Exists)
    }

    async fn send_and_confirm_message(
        &self,
        message: Message,
        keystore: &Arc<KeyStore>,
    ) -> Result<Signature, ClientError> {
        let transaction = keystore
            .sol
            .sign(message, self.rpc_client.get_latest_blockhash().await?)
            .map_err(|err| {
                ClientError::from(ClientErrorKind::Custom(format!(
                    "Failed to sign sol message: {}",
                    err
                )))
            })?;

        self.rpc_client
            .send_and_confirm_transaction(&transaction)
            .await
    }
}

struct PendingEvent {
    event_data: Vec<u8>,
    round_number: Option<u32>,
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

type VerificationStatusTx = oneshot::Sender<(VerificationStatus, u32)>;

type SubscribeResponseTx = mpsc::UnboundedSender<(Pubkey, u32, Vec<u8>)>;

pub struct SolTonAccountData {
    pub program_id: Pubkey,
    pub settings: Pubkey,
    pub seed: u128,
    pub event_data: Vec<u8>,
}

pub struct SolTonTransactionData {
    pub program_id: Pubkey,
    pub signature: Signature,
    pub slot: Slot,
    pub block_time: UnixTimestamp,
    pub seed: u128,
}

#[derive(thiserror::Error, Debug)]
enum SolSubscriberError {
    #[error("Failed to decode solana transaction: `{0}`")]
    DecodeTransactionError(String),
}
