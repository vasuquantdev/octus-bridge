use std::collections::{hash_map, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use rustc_hash::FxHashMap;
use tokio::sync::{oneshot, Semaphore};

use solana_account_decoder::{UiAccountEncoding, UiDataSliceConfig};
use solana_bridge::bridge_state::{AccountKind, Proposal};
use solana_client::client_error::{ClientError, ClientErrorKind};
use solana_client::rpc_client::RpcClient;
use solana_client::rpc_config::{
    RpcAccountInfoConfig, RpcProgramAccountsConfig, RpcTransactionConfig,
};
use solana_client::rpc_filter::{Memcmp, MemcmpEncodedBytes, RpcFilterType};
use solana_sdk::account::{Account, ReadableAccount};
use solana_sdk::bs58;
use solana_sdk::clock::{Slot, UnixTimestamp};
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::message::Message;
use solana_sdk::program_pack::Pack;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signature;
use solana_transaction_status::{EncodedConfirmedTransaction, UiTransactionEncoding};

use crate::config::*;
use crate::engine::bridge::*;
use crate::engine::keystore::*;
use crate::utils::*;

pub struct SolSubscriber {
    config: SolConfig,
    rpc_client: Arc<RpcClient>,
    pool: Arc<Semaphore>,
    programs_to_subscribe: parking_lot::RwLock<Vec<Pubkey>>,
    pending_events: tokio::sync::Mutex<FxHashMap<Pubkey, PendingEvent>>,
    pending_events_count: AtomicUsize,
    unrecognized_proposals_count: AtomicUsize,
}

impl SolSubscriber {
    pub async fn new(config: SolConfig) -> Result<Arc<Self>> {
        let rpc_client = Arc::new(RpcClient::new_with_timeout_and_commitment(
            config.endpoint.clone(),
            Duration::from_secs(config.connection_timeout_sec),
            config.commitment,
        ));

        let block_height = rpc_client.get_block_height()?;
        tracing::info!(block_height, "created SOL subscriber");

        let pool = Arc::new(Semaphore::new(config.pool_size));

        let subscriber = Arc::new(Self {
            config,
            rpc_client,
            pool,
            programs_to_subscribe: Default::default(),
            pending_events: Default::default(),
            pending_events_count: Default::default(),
            unrecognized_proposals_count: Default::default(),
        });

        Ok(subscriber)
    }

    pub fn config(&self) -> &SolConfig {
        &self.config
    }

    pub fn start(self: &Arc<Self>) {
        let subscriber = Arc::downgrade(self);

        tokio::spawn(async move {
            loop {
                let subscriber = match subscriber.upgrade() {
                    Some(subscriber) => subscriber,
                    None => return,
                };

                if let Err(e) = subscriber.update().await {
                    tracing::error!("error occurred during SOL subscriber update: {e:?}");
                }

                tokio::time::sleep(Duration::from_secs(subscriber.config.poll_interval_sec)).await;
            }
        });
    }

    pub fn subscribe(&self, program_pubkey: Pubkey) {
        let mut programs = self.programs_to_subscribe.write();
        if !programs.contains(&program_pubkey) {
            tracing::info!(%program_pubkey, "subscriber to Solana program");
            programs.push(program_pubkey);
        }
    }

    pub fn metrics(&self) -> SolSubscriberMetrics {
        SolSubscriberMetrics {
            unrecognized_proposals_count: self.unrecognized_proposals_count.load(Ordering::Acquire),
        }
    }

    pub async fn verify_ton_sol_event(
        &self,
        account_pubkey: Pubkey,
        event_data: Vec<u8>,
    ) -> Result<VerificationStatus> {
        let rx = {
            let mut pending_events = self.pending_events.lock().await;

            let (tx, rx) = oneshot::channel();

            let created_at = chrono::Utc::now().timestamp() as u64;

            const INIT_INTERVAL_DELAY_SEC: u32 = 300;

            pending_events.insert(
                account_pubkey,
                PendingEvent {
                    event_data,
                    status_tx: Some(tx),
                    created_at,
                    delay: INIT_INTERVAL_DELAY_SEC,
                    time: Default::default(),
                },
            );

            self.pending_events_count
                .store(pending_events.len(), Ordering::Release);

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
        match self.verify_sol_ton_transaction(transaction_data).await? {
            VerificationStatus::Exists => self.verify_sol_ton_account(account_data).await,
            status @ VerificationStatus::NotExists { .. } => Ok(status),
        }
    }

    pub async fn send_message(
        &self,
        message: Message,
        keystore: &Arc<KeyStore>,
    ) -> Result<(), ClientError> {
        let _ = {
            retry(
                || async {
                    let message = message.clone();
                    self.send_and_confirm_message(message, keystore).await
                },
                generate_sol_rpc_backoff_config(Duration::from_secs(
                    self.config.maximum_failed_responses_time_sec,
                )),
                NetworkType::SOL,
                "send solana transaction",
            )
            .await?
        };

        Ok(())
    }

    pub async fn is_already_voted(
        &self,
        round_number: u32,
        proposal_pubkey: &Pubkey,
        voter_pubkey: &Pubkey,
    ) -> Result<bool> {
        let relay_round_pubkey = solana_bridge::round_loader::get_relay_round_address(round_number);
        let relay_round_account = self.get_account(&relay_round_pubkey).await?;
        let relay_round_account_data = match relay_round_account {
            Some(account) => solana_bridge::round_loader::RelayRound::unpack(account.data())?,
            None => {
                return Err(
                    SolSubscriberError::InvalidRoundAccount(relay_round_pubkey.to_string()).into(),
                )
            }
        };

        let proposal_account = self.get_account(proposal_pubkey).await?;
        let proposal_data = match proposal_account {
            Some(account) => Proposal::unpack_from_slice(account.data())?,
            None => {
                return Err(
                    SolSubscriberError::InvalidProposalAccount(proposal_pubkey.to_string()).into(),
                )
            }
        };

        let index = relay_round_account_data
            .relays
            .iter()
            .position(|pubkey| pubkey == voter_pubkey)
            .ok_or(SolSubscriberError::InvalidRound(round_number))?;

        let vote = proposal_data
            .signers
            .get(index)
            .ok_or(SolSubscriberError::InvalidVotePosition(index))?;

        Ok(*vote != solana_bridge::bridge_types::Vote::None)
    }

    async fn get_account(&self, account_pubkey: &Pubkey) -> Result<Option<Account>> {
        let account = {
            retry(
                || self.get_account_with_commitment(account_pubkey, self.config.commitment),
                generate_default_timeout_config(Duration::from_secs(
                    self.config.maximum_failed_responses_time_sec,
                )),
                NetworkType::SOL,
                "get account",
            )
            .await?
        };

        Ok(account)
    }

    async fn healthcheck(&self) -> Result<()> {
        retry(
            || self.get_health(),
            generate_default_timeout_config(Duration::from_secs(
                self.config.maximum_failed_responses_time_sec,
            )),
            NetworkType::SOL,
            "healthcheck",
        )
        .await?;

        Ok(())
    }

    async fn update(&self) -> Result<()> {
        tracing::info!(
            pending_events = self.pending_events_count.load(Ordering::Acquire),
            "updating SOL subscriber",
        );

        let mut accounts_to_check = HashSet::new();

        // Get pending Solana proposals to check
        let programs_to_subscribe = self.programs_to_subscribe.read().clone();
        for program_pubkey in programs_to_subscribe {
            let mut pending_proposals = self.get_pending_proposals(&program_pubkey).await?;
            tracing::info!(?pending_proposals, "found withdrawal proposals to vote");

            let pending_events = self.pending_events.lock().await;

            let unrecognized_proposals = pending_proposals
                .iter()
                .filter(|account| !pending_events.contains_key(account))
                .count();
            tracing::info!(
                %program_pubkey,
                ?unrecognized_proposals,
                "found unrecognized proposals",
            );

            self.unrecognized_proposals_count
                .store(unrecognized_proposals, Ordering::Release);

            // Get rid of unrecognized proposals
            pending_proposals.retain(|account| pending_events.contains_key(account));

            accounts_to_check.extend(pending_proposals.into_iter().collect::<HashSet<Pubkey>>());
        }

        // Get pending TON events to check
        let time = chrono::Utc::now().timestamp() as u64;

        let mut pending_events = self.pending_events.lock().await;
        for (account, event) in pending_events.iter_mut() {
            if !accounts_to_check.contains(account) && time > event.time {
                tracing::info!(
                    account_pubkey = %account,
                    "adding proposal account from TON->SOL pending events to checklist",
                );

                let time_diff = time - event.created_at;
                match time_diff {
                    // First 5 min
                    0..=300 => event.time = time,
                    // Starting from 5 min until 1 hour, double interval
                    301..=3600 => {
                        event.time = time + event.delay as u64;
                        event.delay *= 2;
                    }
                    // After 1 hour poll using interval from config (Default: 1 hour)
                    _ => event.time = time + self.config.poll_proposals_interval_sec,
                };

                accounts_to_check.insert(*account);
            }
        }

        if !accounts_to_check.is_empty() {
            tracing::info!(?accounts_to_check);
        }

        // Check accounts
        for account_pubkey in accounts_to_check {
            match self.get_account(&account_pubkey).await {
                Ok(Some(account)) => {
                    if let hash_map::Entry::Occupied(mut entry) =
                        pending_events.entry(account_pubkey)
                    {
                        let account_data = match Proposal::unpack_from_slice(account.data()) {
                            Ok(proposal) => proposal,
                            Err(_) => {
                                entry.remove();
                                anyhow::bail!("Failed to unpack {} proposal", account_pubkey);
                            }
                        };

                        let status = entry.get().check(account_data.event);

                        let round_pubkey = solana_bridge::round_loader::get_relay_round_address(
                            account_data.round_number,
                        );
                        match self.get_account(&round_pubkey).await {
                            Ok(None) => {
                                entry.remove();

                                anyhow::bail!(
                                    "Round {} in solana doesn't exist",
                                    account_data.round_number
                                );
                            }
                            Err(_) => {
                                // Break handling and repeat later
                                continue;
                            }
                            _ => {
                                // Do nothing and continue handing
                            }
                        }

                        if account_data.is_initialized {
                            if let Some(tx) = entry.get_mut().status_tx.take() {
                                tx.send(status).ok();
                            }

                            entry.remove();
                        }
                    }
                }
                Ok(None) => {
                    tracing::info!(
                        %account_pubkey,
                        "Solana proposal account doesn't exist yet",
                    );
                }
                Err(e) => {
                    tracing::error!(
                        %account_pubkey,
                        "failed to check solana proposal: {e:?}",
                    );
                }
            }
        }

        self.pending_events_count
            .store(pending_events.len(), Ordering::Release);

        drop(pending_events);

        Ok(())
    }

    async fn get_pending_proposals(&self, program_pubkey: &Pubkey) -> Result<Vec<Pubkey>> {
        let accounts = {
            retry(
                || async {
                    let mem: Vec<u8> = vec![
                        true as u8,                                           // is_initialized
                        AccountKind::Proposal(Default::default()).to_value(), // account_kind
                    ];
                    let memcmp = MemcmpEncodedBytes::Base58(bs58::encode(mem).into_string());

                    let exec_status = vec![false as u8]; // is_executed
                    let memcmp_exec_status =
                        MemcmpEncodedBytes::Base58(bs58::encode(exec_status).into_string());
                    let status_offset = 3;

                    let config = RpcProgramAccountsConfig {
                        filters: Some(vec![
                            RpcFilterType::Memcmp(Memcmp {
                                offset: 0,
                                bytes: memcmp,
                                encoding: None,
                            }),
                            RpcFilterType::Memcmp(Memcmp {
                                offset: status_offset,
                                bytes: memcmp_exec_status,
                                encoding: None,
                            }),
                        ]),
                        account_config: RpcAccountInfoConfig {
                            encoding: Some(UiAccountEncoding::Base64),
                            commitment: Some(self.config.commitment),
                            data_slice: Some(UiDataSliceConfig {
                                offset: 0,
                                length: 0,
                            }),
                        },
                        ..Default::default()
                    };

                    self.get_program_accounts_with_config(program_pubkey, config)
                        .await
                },
                generate_default_timeout_config(Duration::from_secs(
                    self.config.maximum_failed_responses_time_sec,
                )),
                NetworkType::SOL,
                "get program accounts",
            )
            .await?
        };

        Ok(accounts.into_iter().map(|(pubkey, _)| pubkey).collect())
    }

    async fn get_transaction(&self, signature: &Signature) -> Result<EncodedConfirmedTransaction> {
        let transaction = {
            retry(
                || async {
                    let config = RpcTransactionConfig {
                        commitment: Some(self.config.commitment),
                        encoding: Some(UiTransactionEncoding::Base64),
                    };

                    self.get_transaction_with_config(signature, config).await
                },
                generate_default_timeout_config(Duration::from_secs(
                    self.config.maximum_failed_responses_time_sec,
                )),
                NetworkType::SOL,
                "get solana transaction",
            )
            .await?
        };

        Ok(transaction)
    }

    async fn get_account_with_commitment(
        &self,
        account_pubkey: &Pubkey,
        commitment_config: CommitmentConfig,
    ) -> Result<Option<Account>, ClientError> {
        let _permit = self.pool.acquire().await;

        tokio::task::spawn_blocking({
            let account_pubkey = *account_pubkey;
            let rpc_client = self.rpc_client.clone();
            move || -> Result<Option<Account>, ClientError> {
                rpc_client
                    .get_account_with_commitment(&account_pubkey, commitment_config)
                    .map(|response| response.value)
            }
        })
        .await
        .map_err(|err| {
            ClientError::from(ClientErrorKind::Custom(format!(
                "Failed to get solana account: {err}"
            )))
        })?
    }

    async fn get_program_accounts_with_config(
        &self,
        program_pubkey: &Pubkey,
        config: RpcProgramAccountsConfig,
    ) -> Result<Vec<(Pubkey, Account)>, ClientError> {
        let _permit = self.pool.acquire().await;

        tokio::task::spawn_blocking({
            let program_pubkey = *program_pubkey;
            let rpc_client = self.rpc_client.clone();
            move || -> Result<Vec<(Pubkey, Account)>, ClientError> {
                rpc_client.get_program_accounts_with_config(&program_pubkey, config)
            }
        })
        .await
        .map_err(|err| {
            ClientError::from(ClientErrorKind::Custom(format!(
                "Failed to get solana program accounts: {err}",
            )))
        })?
    }

    async fn get_transaction_with_config(
        &self,
        signature: &Signature,
        config: RpcTransactionConfig,
    ) -> Result<EncodedConfirmedTransaction, ClientError> {
        let _permit = self.pool.acquire().await;

        tokio::task::spawn_blocking({
            let signature = *signature;
            let rpc_client = self.rpc_client.clone();
            move || -> Result<EncodedConfirmedTransaction, ClientError> {
                rpc_client.get_transaction_with_config(&signature, config)
            }
        })
        .await
        .map_err(|err| {
            ClientError::from(ClientErrorKind::Custom(format!(
                "Failed to get solana transaction: {err}",
            )))
        })?
    }

    async fn get_health(&self) -> Result<(), ClientError> {
        let _permit = self.pool.acquire().await;

        tokio::task::spawn_blocking({
            let rpc_client = self.rpc_client.clone();
            move || -> Result<(), ClientError> { rpc_client.get_health() }
        })
        .await
        .map_err(|err| {
            ClientError::from(ClientErrorKind::Custom(format!(
                "Failed to get solana health: {err}",
            )))
        })?
    }

    async fn verify_sol_ton_transaction(
        &self,
        data: SolTonTransactionData,
    ) -> Result<VerificationStatus> {
        let result = self.get_transaction(&data.signature).await?;

        if result.slot != data.slot || result.block_time != Some(data.block_time) {
            return Ok(VerificationStatus::NotExists {
                reason: "Block slot or time mismatch".to_owned(),
            });
        }

        let transaction = result.transaction.transaction.decode().ok_or_else(|| {
            SolSubscriberError::DecodeTransactionError(data.signature.to_string())
        })?;

        for ix in transaction.message.instructions {
            if transaction.message.account_keys[ix.program_id_index as usize] == data.program_id {
                let deposit_seed = u128::from_le_bytes(ix.data[1..17].try_into()?);
                if deposit_seed == data.seed {
                    return Ok(VerificationStatus::Exists);
                }
            }
        }

        Ok(VerificationStatus::NotExists {
            reason: "Deposit seed not found".to_owned(),
        })
    }

    async fn verify_sol_ton_account(&self, data: SolTonAccountData) -> Result<VerificationStatus> {
        let account_pubkey =
            solana_bridge::token_proxy::get_associated_deposit_address(&data.program_id, data.seed);

        let now = std::time::Instant::now();

        let result = loop {
            self.healthcheck().await?;

            if let Some(account) = self.get_account(&account_pubkey).await? {
                break Some(account);
            }

            if now.elapsed().as_secs() > self.config.poll_deposits_timeout_sec {
                break None;
            }

            tokio::time::sleep(Duration::from_secs(self.config.poll_deposits_interval_sec)).await;
        };

        let account = match result {
            Some(account) => account,
            None => {
                tracing::error!(%account_pubkey, "Solana account doesn't exist");
                return Ok(VerificationStatus::NotExists {
                    reason: "Solana account doesn't exist".to_owned(),
                });
            }
        };

        let account_data = solana_bridge::token_proxy::Deposit::unpack_from_slice(account.data())?;
        if data.event_data != account_data.event {
            return Ok(VerificationStatus::NotExists {
                reason: "Event data mismatch".to_owned(),
            });
        }

        Ok(VerificationStatus::Exists)
    }

    async fn send_and_confirm_message(
        &self,
        message: Message,
        keystore: &Arc<KeyStore>,
    ) -> Result<Signature, ClientError> {
        let _permit = self.pool.acquire().await;

        tokio::task::spawn_blocking({
            let rpc_client = self.rpc_client.clone();
            let keystore = keystore.clone();
            move || -> Result<Signature, ClientError> {
                let transaction = keystore
                    .sol
                    .sign(message, rpc_client.get_latest_blockhash()?)
                    .map_err(|err| {
                        ClientError::from(ClientErrorKind::Custom(format!(
                            "Failed to sign sol message: {err}",
                        )))
                    })?;

                rpc_client.send_and_confirm_transaction(&transaction)
            }
        })
        .await
        .map_err(|err| {
            ClientError::from(ClientErrorKind::Custom(format!(
                "Failed to send solana request: {err}",
            )))
        })?
    }
}

struct PendingEvent {
    event_data: Vec<u8>,
    status_tx: Option<VerificationStatusTx>,
    created_at: u64,
    delay: u32,
    time: u64,
}

impl PendingEvent {
    fn check(&self, event_data: Vec<u8>) -> VerificationStatus {
        if self.event_data == event_data {
            VerificationStatus::Exists
        } else {
            VerificationStatus::NotExists {
                reason: "Event data mismatch".to_owned(),
            }
        }
    }
}

type VerificationStatusTx = oneshot::Sender<VerificationStatus>;

pub struct SolTonAccountData {
    pub program_id: Pubkey,
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

#[derive(Debug, Copy, Clone)]
pub struct SolSubscriberMetrics {
    pub unrecognized_proposals_count: usize,
}

#[derive(thiserror::Error, Debug)]
enum SolSubscriberError {
    #[error("Failed to decode solana transaction `{0}`")]
    DecodeTransactionError(String),
    #[error("Relay is not in the round `{0}`")]
    InvalidRound(u32),
    #[error("Invalid vote position `{0}`")]
    InvalidVotePosition(usize),
    #[error("Relay round `{0}` doesn't exist")]
    InvalidRoundAccount(String),
    #[error("Proposal `{0}` doesn't exist")]
    InvalidProposalAccount(String),
}
