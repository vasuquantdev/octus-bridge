use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use tokio::sync::oneshot;
use tokio::time::{interval, timeout};

use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::account::{Account, ReadableAccount};
use solana_sdk::commitment_config::CommitmentConfig;
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
}

impl SolSubscriber {
    pub async fn new(config: SolConfig) -> Result<Arc<Self>> {
        let rpc_client = Arc::new(RpcClient::new_with_commitment(
            config.endpoint.clone(),
            config.commitment,
        ));

        let subscriber = Arc::new(Self { config, rpc_client });

        Ok(subscriber)
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

    pub async fn verify_ton_sol_event(
        &self,
        account_pubkey: Pubkey,
        event_data: Vec<u8>,
    ) -> Result<VerificationStatus> {
        let rx = {
            let (tx, rx) = oneshot::channel();

            let rpc_client = self.rpc_client.clone();
            let get_timeout_sec = self.config.get_timeout_sec;
            let commitment_config = self.config.commitment;
            let maximum_failed_responses_time_sec = self.config.maximum_failed_responses_time_sec;

            tokio::spawn(async move {
                let mut interval = interval(Duration::from_secs(10));

                loop {
                    interval.tick().await;

                    let account = get_account(
                        &rpc_client,
                        &account_pubkey,
                        commitment_config,
                        get_timeout_sec,
                        maximum_failed_responses_time_sec,
                    )
                    .await;

                    match account {
                        Ok(Some(account)) => {
                            let status =
                                match solana_bridge::bridge_state::Proposal::unpack_from_slice(
                                    account.data(),
                                ) {
                                    Ok(account_data) => {
                                        if account_data.event == event_data {
                                            VerificationStatus::Exists
                                        } else {
                                            VerificationStatus::NotExists
                                        }
                                    }
                                    Err(err) => {
                                        log::error!(
                                            "Failed to unpack Solana Proposal Account 0x{}: {:?}",
                                            account_pubkey,
                                            err
                                        );
                                        VerificationStatus::NotExists
                                    }
                                };

                            tx.send(status).ok();

                            return;
                        }
                        Ok(None) => {
                            log::debug!(
                                "Solana Proposal Account 0x{} hasn't created yet",
                                account_pubkey
                            );
                        }
                        Err(err) => {
                            log::error!(
                                "Failed to get Solana Proposal Account 0x{}: {:?}",
                                account_pubkey,
                                err
                            );
                        }
                    };
                }
            });

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
        let result = get_account(
            &self.rpc_client,
            &account_pubkey,
            self.config.commitment,
            self.config.get_timeout_sec,
            self.config.maximum_failed_responses_time_sec,
        )
        .await?;

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

async fn get_account(
    rpc_client: &Arc<RpcClient>,
    pubkey: &Pubkey,
    commitment_config: CommitmentConfig,
    get_timeout_sec: u64,
    maximum_failed_responses_time_sec: u64,
) -> Result<Option<Account>> {
    let account = {
        retry(
            || {
                timeout(
                    Duration::from_secs(get_timeout_sec),
                    get_account_with_commitment(rpc_client, pubkey, commitment_config),
                )
            },
            generate_default_timeout_config(Duration::from_secs(maximum_failed_responses_time_sec)),
            "get account",
        )
        .await
        .context("Timed out getting account")?
        .context("Failed getting account")?
    };

    Ok(account)
}

async fn get_account_with_commitment(
    rpc_client: &Arc<RpcClient>,
    account_pubkey: &Pubkey,
    commitment_config: CommitmentConfig,
) -> Result<Option<Account>> {
    rpc_client
        .get_account_with_commitment(account_pubkey, commitment_config)
        .await
        .map(|response| response.value)
        .map_err(anyhow::Error::new)
}
