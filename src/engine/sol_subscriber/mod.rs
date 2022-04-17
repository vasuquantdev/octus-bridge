use std::collections::hash_map;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use futures::StreamExt;
use tiny_adnl::utils::*;
use tokio::sync::{oneshot, Notify};
use tokio::time::timeout;

use solana_bridge::bridge_types::UInt256;
use solana_client::nonblocking::rpc_client::RpcClient;
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
    pending_confirmations: tokio::sync::Mutex<FxHashMap<AccountId, PendingConfirmation>>,
    pending_confirmation_count: AtomicUsize,
    new_events_notify: Notify,
}

impl SolSubscriber {
    pub async fn new(config: SolConfig) -> Result<Arc<Self>> {
        let rpc_client = Arc::new(RpcClient::new_with_commitment(
            config.url.clone(),
            config.commitment_config,
        ));

        let subscriber = Arc::new(Self {
            config,
            rpc_client,
            pending_confirmations: Default::default(),
            pending_confirmation_count: Default::default(),
            new_events_notify: Notify::new(),
        });

        Ok(subscriber)
    }

    pub fn metrics(&self) -> SolSubscriberMetrics {
        SolSubscriberMetrics {
            pending_confirmation_count: self.pending_confirmation_count.load(Ordering::Acquire),
        }
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
                    log::error!("Error occurred during Solana event update: {:?}", e);
                }
            }
        });
    }

    async fn update(&self) -> Result<()> {
        if self.pending_confirmations.lock().await.is_empty() {
            // Wait until new events appeared or idle poll interval passed.
            tokio::select! {
                _ = self.new_events_notify.notified() => {},
                _ = tokio::time::sleep(Duration::from_secs(self.config.poll_interval_sec)) => {},
            }
        }

        log::info!(
            "TON->SOL pending confirmations: {}",
            self.pending_confirmation_count.load(Ordering::Acquire)
        );

        let mut pending_confirmations = self.pending_confirmations.lock().await;

        let accounts_to_check = futures::stream::FuturesUnordered::new();
        for (&account_id, confirmation) in pending_confirmations.iter() {
            accounts_to_check.push(async move {
                let account_pubkey = solana_bridge::bridge_helper::get_associated_proposal_address(
                    &confirmation.program,
                    account_id.0,
                    account_id.1,
                );

                let result = self.get_account(&account_pubkey).await;
                (account_id, result)
            });
        }

        let accounts_to_check = accounts_to_check
            .collect::<Vec<(AccountId, Result<Option<Account>>)>>()
            .await;

        log::info!("Accounts to check: {:?}", accounts_to_check);

        for (account_id, result) in accounts_to_check {
            if let hash_map::Entry::Occupied(mut entry) = pending_confirmations.entry(account_id) {
                let status = match result {
                    Ok(Some(account)) => {
                        let account_data =
                            solana_bridge::bridge_state::Proposal::unpack(account.data())?;
                        entry.get().check(account_data)
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

        self.pending_confirmation_count
            .store(pending_confirmations.len(), Ordering::Release);

        drop(pending_confirmations);

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

    pub async fn verify_withdrawal_event(
        &self,
        configuration: UInt256,
        event_transaction_lt: u64,
        program: Pubkey,
        event_data: Vec<u8>,
    ) -> Result<VerificationStatus> {
        let rx = {
            let (tx, rx) = oneshot::channel();

            let account_id = (configuration, event_transaction_lt);

            let mut pending_confirmations = self.pending_confirmations.lock().await;
            pending_confirmations.insert(
                account_id,
                PendingConfirmation {
                    program,
                    event_data,
                    status_tx: Some(tx),
                },
            );

            self.pending_confirmation_count
                .store(pending_confirmations.len(), Ordering::Release);

            self.new_events_notify.notify_waiters();

            rx
        };

        let status = rx.await?;
        Ok(status)
    }

    pub async fn verify_deposit_event(
        &self,
        seed: u64,
        program: Pubkey,
        event_data: Vec<u8>,
    ) -> Result<VerificationStatus> {
        let account_pubkey =
            solana_bridge::token_proxy::get_associated_deposit_address(&program, seed);

        let result = self.get_account(&account_pubkey).await;
        let account = match result {
            Ok(Some(account)) => account,
            Ok(None) => return Ok(VerificationStatus::NotExists),
            Err(err) => {
                log::error!(
                    "Failed to get Deposit Solana Account 0x{}: {}",
                    account_pubkey,
                    err
                );
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

struct PendingConfirmation {
    program: Pubkey,
    event_data: Vec<u8>,
    status_tx: Option<VerificationStatusTx>,
}

impl PendingConfirmation {
    fn check(&self, account_data: solana_bridge::bridge_state::Proposal) -> VerificationStatus {
        if self.event_data != account_data.event {
            return VerificationStatus::NotExists;
        }

        VerificationStatus::Exists
    }
}

type VerificationStatusTx = oneshot::Sender<VerificationStatus>;

pub type AccountId = (UInt256, u64);
