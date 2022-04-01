use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use tiny_adnl::utils::*;
use tokio::sync::{mpsc, oneshot, Notify};

use solana_client::nonblocking::pubsub_client::PubsubClient;
use solana_client::rpc_client::RpcClient;
use solana_client::rpc_response::{Response, RpcKeyedAccount};
use solana_program::program_pack::Pack;
use solana_sdk::account::{Account, ReadableAccount};
use solana_sdk::hash::Hash;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signature;
use solana_sdk::transaction::Transaction;
use token_proxy::WithdrawalPattern;

use crate::config::*;
use crate::engine::bridge::*;
use crate::engine::keystore::*;
use crate::engine::ton_contracts::*;
use crate::utils::*;

pub struct SolSubscriber {
    config: SolConfig,
    rpc_client: Arc<RpcClient>,
    pubsub_client: Arc<PubsubClient>,
    ton_sol_pending_confirmations: tokio::sync::Mutex<FxHashMap<Hash, TonSolPendingConfirmation>>,
    ton_sol_pending_confirmation_count: AtomicUsize,
    ton_sol_new_events_notify: Notify,
}

impl SolSubscriber {
    pub async fn new(config: SolConfig) -> Result<Arc<Self>> {
        let rpc_client = Arc::new(RpcClient::new_with_commitment(
            &config.url,
            config.commitment_config,
        ));
        let pubsub_client = Arc::new(PubsubClient::new(&config.ws_url).await?);

        let subscriber = Arc::new(Self {
            config,
            rpc_client,
            pubsub_client,
            ton_sol_pending_confirmations: Default::default(),
            ton_sol_pending_confirmation_count: Default::default(),
            ton_sol_new_events_notify: Notify::new(),
        });

        Ok(subscriber)
    }

    pub fn metrics(&self) -> SolSubscriberMetrics {
        SolSubscriberMetrics {
            ton_sol_pending_confirmation_count: self
                .ton_sol_pending_confirmation_count
                .load(Ordering::Acquire),
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

                tokio::select! {
                    _ = subscriber.ton_sol_new_events_notify.notified() => {},
                    _ = tokio::time::sleep(Duration::from_secs(subscriber.config.poll_interval_sec)) => {},
                };

                if let Err(e) = subscriber.ton_sol_update().await {
                    log::error!("Error occurred during Solana event update: {:?}", e);
                }
            }
        });
    }

    async fn ton_sol_update(&self) -> Result<()> {
        log::info!(
            "Updating Solana subscriber. (TON->SOL pending confirmations: {})",
            self.ton_sol_pending_confirmation_count
                .load(Ordering::Acquire)
        );

        let pending_confirmations = self.ton_sol_pending_confirmations.lock().await;
        let payload_ids: Vec<Hash> = pending_confirmations
            .iter()
            .map(|(hash, _)| *hash)
            .collect();
        drop(pending_confirmations);

        for payload_id in payload_ids {
            let account_pubkey = token_proxy::get_associated_withdrawal_address(&payload_id);

            // Prepare tryhard config
            let api_request_strategy = generate_fixed_timeout_config(
                Duration::from_secs(self.config.get_timeout_sec),
                Duration::from_secs(self.config.maximum_failed_responses_time_sec),
            );

            let account = match retry(
                || self.get_account(&account_pubkey),
                api_request_strategy,
                "get solana account",
            )
            .await
            {
                Ok(account) => account,
                Err(e) if is_account_not_found(&e, &account_pubkey) => {
                    log::info!(
                        "Withdrawal Solana Account 0x{} not created yet",
                        account_pubkey
                    );
                    continue;
                }
                Err(e) => {
                    log::error!(
                        "Failed to get Withdrawal Solana Account 0x{}: {}",
                        account_pubkey,
                        e
                    );
                    continue;
                }
            };

            let account_data = token_proxy::WithdrawalPattern::unpack(account.data())?;

            let mut pending_confirmations = self.ton_sol_pending_confirmations.lock().await;
            if let Some(confirmation) = pending_confirmations.get_mut(&payload_id) {
                let status = confirmation.check(account_data);

                log::info!("Confirmation status: {:?}", status);

                if let Some(tx) = confirmation.status_tx.take() {
                    tx.send(status).ok();
                }
            }
            drop(pending_confirmations);
        }

        Ok(())
    }

    async fn get_account(&self, account_pubkey: &Pubkey) -> Result<Account> {
        self.rpc_client
            .get_account(account_pubkey)
            .map_err(anyhow::Error::new)
    }

    async fn get_latest_blockhash(&self) -> Result<Hash> {
        self.rpc_client
            .get_latest_blockhash()
            .map_err(anyhow::Error::new)
    }

    async fn send_and_confirm_transaction(&self, transaction: &Transaction) -> Result<Signature> {
        self.rpc_client
            .send_and_confirm_transaction(transaction)
            .map_err(anyhow::Error::new)
    }

    pub async fn verify_ton_sol_event(
        &self,
        vote_data: TonSolEventVoteData,
    ) -> Result<VerificationStatus> {
        let rx = {
            let mut pending_confirmations = self.ton_sol_pending_confirmations.lock().await;

            let payload_id = Hash::new_from_array(vote_data.payload_id.inner());

            let (tx, rx) = oneshot::channel();

            pending_confirmations.insert(
                payload_id,
                TonSolPendingConfirmation {
                    vote_data,
                    status_tx: Some(tx),
                },
            );

            self.ton_sol_pending_confirmation_count
                .store(pending_confirmations.len(), Ordering::Release);

            self.ton_sol_new_events_notify.notify_waiters();

            rx
        };

        let status = rx.await?;
        Ok(status)
    }

    pub async fn vote_for_withdraw_request(
        &self,
        ton_signer: &TonSigner,
        payload_id: Hash,
        round_number: u32,
    ) -> Result<()> {
        let pubkey = Pubkey::new_from_array(ton_signer.raw_public_key().to_bytes());
        let ix = token_proxy::confirm_withdrawal_request(&pubkey, payload_id, round_number);

        // Prepare tryhard config
        let api_request_strategy = generate_fixed_timeout_config(
            Duration::from_secs(self.config.get_timeout_sec),
            Duration::from_secs(self.config.maximum_failed_responses_time_sec),
        );

        let latest_blockhash = match retry(
            || self.get_latest_blockhash(),
            api_request_strategy,
            "get solana latest blockhash",
        )
        .await
        {
            Ok(latest_blockhash) => latest_blockhash,
            Err(e) => {
                return Err(e).with_context(|| "Failed to get latest Solana blockhash".to_string())
            }
        };

        let mut transaction = Transaction::new_with_payer(&[ix], Some(&pubkey));
        ton_signer.sign_solana_transaction(&mut transaction, latest_blockhash)?;

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
    pub ton_sol_pending_confirmation_count: usize,
}

struct TonSolPendingConfirmation {
    vote_data: TonSolEventVoteData,
    status_tx: Option<VerificationStatusTx>,
}

impl TonSolPendingConfirmation {
    fn check(&self, _account: WithdrawalPattern) -> VerificationStatus {
        todo!()
    }
}

type VerificationStatusTx = oneshot::Sender<VerificationStatus>;

type SubscribeResponseTx = mpsc::UnboundedSender<(Pubkey, Response<RpcKeyedAccount>)>;

fn is_account_not_found(error: &anyhow::Error, pubkey: &Pubkey) -> bool {
    error
        .to_string()
        .contains(format!("AccountNotFound: pubkey={}", pubkey).as_str())
}
