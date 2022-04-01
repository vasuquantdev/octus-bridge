use std::collections::hash_map;
use std::convert::TryFrom;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use dashmap::DashMap;
use either::Either;
use eth_ton_abi_converter::*;
use futures::StreamExt;
use tiny_adnl::utils::*;
use tokio::sync::{mpsc, oneshot, Notify, Semaphore};
use tokio::time::timeout;
use ton_types::UInt256;

use solana_account_decoder::UiAccountEncoding;
use solana_client::nonblocking::pubsub_client::PubsubClient;
use solana_client::rpc_client::RpcClient;
use solana_client::rpc_config::{RpcAccountInfoConfig, RpcProgramAccountsConfig};
use solana_client::rpc_response::{Response, RpcKeyedAccount};
use solana_program::program_pack::Pack;
use solana_sdk::account::{Account, ReadableAccount};
use solana_sdk::hash::Hash;
use solana_sdk::pubkey::Pubkey;
use solana_transaction_status::UiTransactionEncoding;
use token_proxy::WithdrawalPattern;

//use self::models::*;
use crate::config::*;
use crate::engine::keystore::*;
use crate::engine::ton_contracts::*;
use crate::utils::*;

//mod contracts;
//mod models;

pub struct SolSubscriber {
    config: SolConfig,
    rpc_client: Arc<RpcClient>,
    pubsub_client: Arc<PubsubClient>,
    pending_confirmations: tokio::sync::Mutex<FxHashMap<Hash, PendingConfirmation>>,
    pending_confirmation_count: AtomicUsize,
    new_events_notify: Notify,
    subscribe_tx: parking_lot::RwLock<Option<SubscribeResponseTx>>,
}

impl SolSubscriber {
    async fn new(config: SolConfig) -> Result<Arc<Self>> {
        let rpc_client = Arc::new(RpcClient::new_with_commitment(
            &config.url,
            config.commitment_config,
        ));
        let pubsub_client = Arc::new(PubsubClient::new(&config.ws_url).await?);

        let subscriber = Arc::new(Self {
            config,
            rpc_client,
            pubsub_client,
            pending_confirmations: Default::default(),
            pending_confirmation_count: Default::default(),
            new_events_notify: Notify::new(),
            subscribe_tx: Default::default(),
        });

        Ok(subscriber)
    }

    pub fn metrics(&self) -> SolSubscriberMetrics {
        SolSubscriberMetrics {
            pending_confirmation_count: self.pending_confirmation_count.load(Ordering::Acquire),
        }
    }

    pub async fn start(self: &Arc<Self>) -> Result<()> {
        let subscriber = Arc::downgrade(self);

        tokio::spawn(async move {
            let subscriber = match subscriber.upgrade() {
                Some(subscriber) => subscriber,
                None => return,
            };

            let (tx, mut rx) = mpsc::unbounded_channel();
            subscriber.subscribe_tx.write().insert(tx);

            while let Some((program_pubkey, response)) = rx.recv().await {
                if let Err(e) = subscriber.handle_response(program_pubkey, response).await {
                    log::error!("Error occurred during SOL node subscriber handle: {:?}", e);
                }
            }
        });

        Ok(())
    }

    pub fn subscribe(&self, program_pubkey: Pubkey) -> Result<()> {
        let pubsub_client = Arc::clone(&self.pubsub_client);
        let commitment_config = self.config.commitment_config;

        let tx = self
            .subscribe_tx
            .read()
            .as_ref()
            .ok_or(SolSubscriberError::EmptySubscribeTx)?
            .clone();

        tokio::spawn({
            async move {
                match pubsub_client
                    .program_subscribe(
                        &program_pubkey,
                        Some(RpcProgramAccountsConfig {
                            account_config: RpcAccountInfoConfig {
                                commitment: Some(commitment_config),
                                encoding: Some(UiAccountEncoding::Base64),
                                ..RpcAccountInfoConfig::default()
                            },
                            ..RpcProgramAccountsConfig::default()
                        }),
                    )
                    .await
                {
                    Ok((mut program_notifications, program_unsubscribe)) => {
                        while let Some(response) = program_notifications.next().await {
                            tx.send((program_pubkey, response)).unwrap();
                        }
                        program_unsubscribe().await;
                    }
                    Err(e) => {
                        log::error!(
                            "Failed to subscribe to Solana address 0x{}: {:?}",
                            program_pubkey,
                            e
                        );
                    }
                }
            }
        });

        Ok(())
    }

    async fn handle_response(
        &self,
        program_pubkey: Pubkey,
        response: Response<RpcKeyedAccount>,
    ) -> Result<()> {
        let block = self
            .rpc_client
            .get_block_with_encoding(response.context.slot, UiTransactionEncoding::Base64)?;

        for transaction in block.transactions {
            if let Some(transaction) = transaction.transaction.decode() {
                let account_keys = transaction.message.static_account_keys();
                for instruction in transaction.message.instructions() {
                    if let Some(token_proxy::TokenProxyInstruction::WithdrawRequest {
                        payload_id,
                        ..
                    }) =
                        decode_token_proxy_instruction(program_pubkey, instruction, account_keys)
                    {
                        let mut pending_confirmations = self.pending_confirmations.lock().await;

                        let is_ready_to_verify =
                            pending_confirmations
                                .iter_mut()
                                .any(|(hash, confirmation)| {
                                    if *hash == payload_id
                                        && confirmation.status == PendingConfirmationStatus::New
                                    {
                                        confirmation.status = PendingConfirmationStatus::InProcess;
                                        true
                                    } else {
                                        false
                                    }
                                });

                        drop(pending_confirmations);

                        if is_ready_to_verify {
                            let account_pubkey =
                                token_proxy::get_associated_withdrawal_address(&payload_id);

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
                                Err(e) => {
                                    log::error!(
                                        "Failed to get solana account 0x{}: {}",
                                        account_pubkey,
                                        e
                                    );
                                    continue;
                                }
                            };

                            let account_data =
                                token_proxy::WithdrawalPattern::unpack(account.data())?;

                            // TODO: validation event
                        }
                    }
                }
            }
        }

        Ok(())
    }

    async fn update(&self) -> Result<()> {
        log::info!(
            "Updating Solana subscriber. (Pending confirmations: {})",
            self.pending_confirmation_count.load(Ordering::Acquire)
        );

        let pending_confirmations = self.pending_confirmations.lock().await;
        let pending_payload_ids: Vec<Hash> = pending_confirmations
            .iter()
            .filter(|(hash, confirmation)| confirmation.status == PendingConfirmationStatus::New)
            .map(|(hash, _)| *hash)
            .collect();
        drop(pending_confirmations);

        for pending_payload_id in pending_payload_ids {
            let account_pubkey =
                token_proxy::get_associated_withdrawal_address(&pending_payload_id);

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
                        "Withdrawal Solana account 0x{} not created yet",
                        account_pubkey
                    );
                    continue;
                }
                Err(e) => {
                    log::error!("Failed to get solana account 0x{}: {}", account_pubkey, e);
                    continue;
                }
            };

            let account_data = token_proxy::WithdrawalPattern::unpack(account.data())?;

            // TODO: verify event
        }

        Ok(())
    }

    async fn get_account(&self, account_pubkey: &Pubkey) -> Result<Account> {
        self.rpc_client
            .get_account(account_pubkey)
            .map_err(anyhow::Error::new)
    }

    /*pub async fn verify_relay_staker_address(
        &self,
        settings: &AddressVerificationConfig,
        eth_signer: EthSignerHandle,
        relay_address: &ethabi::Address,
        staker_address: UInt256,
        verifier_address: &ethabi::Address,
    ) -> Result<()> {
        const GWEI: u64 = 1000000000;

        let clear_state = || {
            if let Err(e) = std::fs::remove_file(&settings.state_path) {
                log::info!("Failed to reset address verification state: {:?}", e);
            }
        };

        // Restore previous state
        match AddressVerificationState::try_load(&settings.state_path)? {
            // Ignore state for different address
            Some(state) if state.address != relay_address.0 => {
                log::warn!("Address verification state created for the different relay address. It will be ignored");
                clear_state();
            }
            // Wait until transaction is found
            Some(state) => {
                let transaction_id = hex::encode(state.transaction_hash);

                loop {
                    // Find transaction
                    match self
                        .api
                        .transaction(web3::types::TransactionId::Hash(
                            state.transaction_hash.into(),
                        ))
                        .await
                        .context("Failed to find ETH address verification transaction")?
                    {
                        // Check if found transaction was included in block
                        Some(transaction) => match transaction.block_hash {
                            // If it was included, consider that the address is confirmed
                            Some(block) => {
                                log::info!(
                                    "ETH transaction {} found in block {}",
                                    transaction_id,
                                    hex::encode(block.as_bytes())
                                );
                                clear_state();
                                return Ok(());
                            }
                            // If it wasn't included, poll
                            None => {
                                log::info!("ETH transaction {} is still pending", transaction_id);
                                tokio::time::sleep(Duration::from_secs(10)).await;
                            }
                        },
                        // Ignore state for non-existing transaction
                        None => {
                            log::warn!("Address verification state contains non-existing transaction. It will be ignored");
                            clear_state();
                            break; // continue verification
                        }
                    };
                }
            }
            None => { /* continue verification */ }
        };

        // Prepare params
        let min_balance: U256 = U256::from(settings.min_balance_gwei * GWEI);
        let gas_price: U256 = U256::from(settings.gas_price_gwei * GWEI);

        let verifier_contract = contracts::staking_contract(self.api.clone(), *verifier_address)?;
        let workchain_id = ethabi::Token::Int(U256::from(0));
        let address_body = ethabi::Token::Uint(U256::from_big_endian(staker_address.as_slice()));

        // Wait minimal balance
        loop {
            let balance = retry(
                || self.get_balance(*relay_address),
                crate::utils::generate_default_timeout_config(Duration::from_secs(60)),
                "Failed getting balance",
            )
            .await?;

            if balance < min_balance {
                log::info!("Insufficient balance ({}/{})", balance, min_balance);
                tokio::time::sleep(Duration::from_secs(10)).await;
            } else {
                break;
            }
        }

        // Prepare transaction
        let fn_data = verifier_contract
            .abi()
            .function("verify_relay_staker_address")
            .and_then(|function| function.encode_input(&[workchain_id, address_body]))
            .map_err(|err| web3::error::Error::Decoder(format!("{:?}", err)))
            .context("Failed to prepare address verification transaction")?;

        let accounts = web3::api::Accounts::new(self.api.transport().clone());
        let tx = web3::types::TransactionParameters {
            to: Some(*verifier_address),
            gas_price: Some(gas_price),
            data: web3::types::Bytes(fn_data),
            ..Default::default()
        };

        let signed = accounts
            .sign_transaction(tx, eth_signer.secret_key())
            .await
            .context("Failed to sign address verification transaction")?;

        AddressVerificationState {
            transaction_hash: signed.transaction_hash.0,
            address: relay_address.0,
        }
        .save(&settings.state_path)
        .context("Failed to save address verification state")?;

        self.api
            .send_raw_transaction(signed.raw_transaction)
            .await
            .context("Failed to send raw ETH transaction")?;

        Ok(())
    }*/

    /*pub fn subscribe(
        &self,
        address: ethabi::Address,
        topic_hash: [u8; 32],
        configuration_account: UInt256,
    ) {
        self.topics
            .write()
            .add_entry(address, topic_hash, configuration_account);
    }*/

    /*pub fn unsubscribe<I>(&self, subscriptions: I)
    where
        I: IntoIterator<Item = (ethabi::Address, [u8; 32], UInt256)>,
    {
        self.topics.write().remove_entries(subscriptions)
    }*/

    /*pub async fn verify(
        &self,
        vote_data: EthTonEventVoteData,
        event_emitter: [u8; 20],
        event_abi: Arc<EthEventAbi>,
        blocks_to_confirm: u16,
    ) -> Result<VerificationStatus> {
        let rx = {
            let mut pending_confirmations = self.pending_confirmations.lock().await;

            let event_id = (
                H256::from(vote_data.event_transaction.as_slice()),
                vote_data.event_index,
            );

            let (tx, rx) = oneshot::channel();

            let target_block = vote_data.event_block_number as u64 + blocks_to_confirm as u64;

            pending_confirmations.insert(
                event_id,
                PendingConfirmation {
                    event_emitter,
                    vote_data,
                    status_tx: Some(tx),
                    event_abi,
                    target_block,
                    status: PendingConfirmationStatus::InProcess,
                },
            );

            self.pending_confirmation_count
                .store(pending_confirmations.len(), Ordering::Release);

            self.new_events_notify.notify_waiters();

            rx
        };

        let status = rx.await?;
        Ok(status)
    }*/

    /*fn start(self: &Arc<Self>) {
        let subscriber = Arc::downgrade(self);

        tokio::spawn(async move {
            loop {
                let subscriber = match subscriber.upgrade() {
                    Some(subscriber) => subscriber,
                    None => return,
                };

                if let Err(e) = subscriber.update().await {
                    log::error!("Error occurred during EVM node subscriber update: {:?}", e);
                }
            }
        });
    }*/

    /*async fn update(&self) -> Result<()> {
        if self.pending_confirmations.lock().await.is_empty() {
            // Wait until new events appeared or idle poll interval passed.
            // NOTE: Idle polling is needed there to prevent large intervals from occurring (e.g. BSC)
            tokio::select! {
                _ = self.new_events_notify.notified() => {},
                _ = tokio::time::sleep(Duration::from_secs(self.config.poll_interval_sec)) => {},
            }
        }

        log::info!(
            "Updating EVM-{} subscriber. (Pending confirmations: {})",
            self.chain_id,
            self.pending_confirmation_count.load(Ordering::Acquire)
        );

        // Prepare tryhard config
        let api_request_strategy = generate_fixed_timeout_config(
            Duration::from_secs(self.config.get_timeout_sec),
            Duration::from_secs(self.config.maximum_failed_responses_time_sec),
        );

        // Get latest ETH block
        let mut current_block = match retry(
            || self.get_current_block_number(),
            api_request_strategy,
            "get actual ethereum height",
        )
        .await
        {
            Ok(height) => height,
            Err(e) if is_incomplete_message(&e) => return Ok(()),
            Err(e) => {
                return Err(e)
                    .with_context(|| format!("Failed to get actual EVM-{} height", self.chain_id))
            }
        };

        log::info!("Current EVM-{} block: {}", self.chain_id, current_block);

        // Check last processed block
        let last_processed_block = self.last_processed_block.load(Ordering::Acquire);
        if last_processed_block >= current_block {
            // NOTE: tokio::select is not used here because it will retry requests immediately if
            // there are some events in queue but the block is still the same
            tokio::time::sleep(Duration::from_secs(self.config.get_timeout_sec)).await;
            return Ok(());
        }

        if let Some(max_block_range) = self.config.max_block_range {
            if last_processed_block + max_block_range < current_block {
                current_block = last_processed_block + max_block_range;
                log::warn!(
                    "Querying at most {} blocks. New current EVM-{} block: {}",
                    max_block_range,
                    self.chain_id,
                    current_block
                );
            }
        }

        // Get all events since last processed block
        let events = match retry(
            || {
                timeout(
                    Duration::from_secs(self.config.blocks_processing_timeout_sec),
                    self.process_blocks(last_processed_block, current_block),
                )
            },
            api_request_strategy,
            "process block",
        )
        .await
        {
            Ok(Ok(events)) => events,
            Ok(Err(e)) => {
                return Err(e).with_context(|| {
                    format!(
                        "Failed processing EVM-{} block in the time range from {} to {}",
                        self.chain_id, last_processed_block, current_block
                    )
                })
            }
            Err(_) => {
                log::warn!("Timed out processing EVM-{} blocks.", self.chain_id);
                return Ok(());
            }
        };

        // Update pending confirmations
        let mut pending_confirmations = self.pending_confirmations.lock().await;
        for event in events {
            if let Some(confirmation) = pending_confirmations.get_mut(&event.event_id()) {
                match event {
                    ParsedEthEvent::Removed(_) => {
                        log::info!("Log removed");
                        confirmation.status = PendingConfirmationStatus::Invalid
                    }
                    ParsedEthEvent::Received(event) => {
                        log::info!("Log received");
                        confirmation.status = confirmation.check(event).into();
                    }
                }
            }
        }

        // Resolve verified confirmations
        let events_to_check = futures::stream::FuturesUnordered::new();
        pending_confirmations.retain(|&event_id, confirmation| {
            if confirmation.target_block > current_block {
                return true;
            }

            log::info!("Found pending confirmation to resolve");

            let status = match confirmation.status {
                PendingConfirmationStatus::InProcess | PendingConfirmationStatus::Valid => {
                    log::info!("Confirmation status: {:?}", confirmation.status);
                    events_to_check.push(async move {
                        let result = self.find_event(&event_id).await;
                        (event_id, result)
                    });
                    return true;
                }
                PendingConfirmationStatus::Invalid => VerificationStatus::NotExists,
            };

            log::info!("Confirmation status: {:?}", status);

            if let Some(tx) = confirmation.status_tx.take() {
                tx.send(status).ok();
            }

            false
        });

        let events_to_check = events_to_check
            .collect::<Vec<(EventId, Result<Option<ParsedEthEvent>>)>>()
            .await;

        log::info!("Events to check: {:?}", events_to_check);

        for (event_id, result) in events_to_check {
            if let hash_map::Entry::Occupied(mut entry) = pending_confirmations.entry(event_id) {
                let status = match result {
                    Ok(Some(ParsedEthEvent::Received(event))) => entry.get_mut().check(event),
                    Ok(_) => VerificationStatus::NotExists,
                    Err(e) => {
                        log::error!("Failed to check EVM-{} event: {:?}", self.chain_id, e);
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

        // Update last processed block
        if let Some(mut entry) = self.last_block_numbers.get_mut(&self.chain_id) {
            *entry.value_mut() = current_block;
            self.last_processed_block
                .store(current_block, Ordering::Release);
        }

        Ok(())
    }*/

    /*async fn process_blocks(
        &self,
        from: u64,
        to: u64,
    ) -> Result<impl Iterator<Item = ParsedEthEvent>> {
        let filter = match self.topics.read().make_filter(from, to) {
            Some(filter) => filter,
            None => return Ok(Either::Left(std::iter::empty())),
        };

        let logs = {
            let _permit = self.pool.acquire().await;
            retry(
                || {
                    let transport = self.api.transport();
                    let request = transport.execute("eth_getLogs", vec![filter.clone()]);
                    web3::helpers::CallFuture::new(request)
                },
                generate_default_timeout_config(Duration::from_secs(
                    self.config.maximum_failed_responses_time_sec,
                )),
                "get contract logs",
            )
            .await
            .context("Failed getting eth logs")?
        };

        Ok(Either::Right(parse_transaction_logs(logs)))
    }*/

    /*async fn find_event(
        &self,
        (transaction_hash, event_index): &EventId,
    ) -> Result<Option<ParsedEthEvent>> {
        let receipt = {
            let _permission = self.pool.acquire().await;

            retry(
                || {
                    timeout(
                        Duration::from_secs(self.config.get_timeout_sec),
                        self.api.transaction_receipt(*transaction_hash),
                    )
                },
                generate_default_timeout_config(Duration::from_secs(
                    self.config.maximum_failed_responses_time_sec,
                )),
                "get transaction receipt",
            )
            .await
            .context("Timed out getting receipt")?
            .context("Failed getting logs")?
        };

        Ok(receipt.and_then(|receipt| {
            if matches!(receipt.status, Some(status) if status.as_u64() == 1) {
                parse_transaction_logs(receipt.logs).rev().find(|event| {
                    event.transaction_hash() == transaction_hash
                        && event.event_index() == *event_index
                })
            } else {
                None
            }
        }))
    }*/

    /*async fn get_balance(&self, address: ethabi::Address) -> Result<U256> {
        Ok(self.api.balance(address, None).await?)
    }*/

    /*async fn get_current_block_number(&self) -> Result<u64> {
        let result = timeout(
            Duration::from_secs(self.config.get_timeout_sec),
            self.api.block_number(),
        )
        .await
        .context("Timeout getting height")??;
        Ok(result.as_u64())
    }*/
}

#[derive(Debug, Copy, Clone)]
pub struct SolSubscriberMetrics {
    pub pending_confirmation_count: usize,
}

#[derive(Default)]
struct TopicsMap {
    subscribe_tx: Option<mpsc::UnboundedSender<Response<RpcKeyedAccount>>>,
}

/*#[derive(Default)]
struct TopicsMap {
    entries: FxHashSet<TopicsMapEntry>,
    unique_addresses: Vec<ethabi::Address>,
    unique_topics: Vec<H256>,
}

impl TopicsMap {
    fn make_filter(&self, from: u64, to: u64) -> Option<serde_json::Value> {
        if self.unique_addresses.is_empty() || self.unique_topics.is_empty() {
            return None;
        }

        let filter = FilterBuilder::default()
            .address(self.unique_addresses.clone())
            .topics(Some(self.unique_topics.clone()), None, None, None)
            .from_block(BlockNumber::Number(U64::from(from)))
            .to_block(BlockNumber::Number(U64::from(to)))
            .build();
        Some(web3::helpers::serialize(&filter))
    }

    fn add_entry(
        &mut self,
        address: ethabi::Address,
        topic_hash: [u8; 32],
        configuration_account: UInt256,
    ) {
        if self.entries.insert(TopicsMapEntry {
            address,
            topic_hash,
            configuration_account,
        }) {
            self.update();
        }
    }

    fn remove_entries<I>(&mut self, entries: I)
    where
        I: IntoIterator<Item = (ethabi::Address, [u8; 32], UInt256)>,
    {
        let mut should_update = false;

        for (address, topic_hash, configuration_account) in entries {
            should_update |= self.entries.remove(&TopicsMapEntry {
                address,
                topic_hash,
                configuration_account,
            });
        }

        if should_update {
            self.update();
        }
    }

    fn update(&mut self) {
        let capacity = self.entries.len();

        let mut unique_addresses =
            FxHashSet::with_capacity_and_hasher(capacity, Default::default());
        let mut unique_topics = FxHashSet::with_capacity_and_hasher(capacity, Default::default());

        self.entries.iter().for_each(|item| {
            unique_addresses.insert(item.address);
            unique_topics.insert(item.topic_hash);
        });

        self.unique_addresses = unique_addresses.into_iter().collect();
        self.unique_topics = unique_topics.into_iter().map(H256).collect();
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
struct TopicsMapEntry {
    address: ethabi::Address,
    topic_hash: [u8; 32],
    configuration_account: UInt256,
}

type EthApi = web3::api::Eth<Http>;
type LastBlockNumbersMap = FxDashMap<u32, u64>;*/

struct PendingConfirmation {
    vote_data: SolTonEventVoteData,
    status_tx: Option<VerificationStatusTx>,
    status: PendingConfirmationStatus,
}

impl PendingConfirmation {
    fn check(&self /*event: ReceivedEthEvent*/) -> VerificationStatus {
        todo!()

        /*let vote_data = &self.vote_data;

        // NOTE: event_index and transaction_hash are already checked while searching
        // ETH event log, but here they are also checked just in case.
        if event.address.0 != self.event_emitter
            || &event.topic_hash != self.event_abi.get_eth_topic_hash()
            || event.event_index != vote_data.event_index
            || &event.transaction_hash.0 != vote_data.event_transaction.as_slice()
            || event.block_number != vote_data.event_block_number as u64
            || &event.block_hash.0 != vote_data.event_block.as_slice()
        {
            return VerificationStatus::NotExists;
        }

        // Event data is checked last, because it is quite expensive. `topic_hash`
        // is checked earlier to also skip this without decoding data.
        match self.event_abi.decode_and_map(&event.data) {
            Ok(data) if data.repr_hash() == vote_data.event_data.repr_hash() => {
                VerificationStatus::Exists
            }
            _ => VerificationStatus::NotExists,
        }*/
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
enum PendingConfirmationStatus {
    New,
    Exist,
    InProcess,
    Valid,
    Invalid,
}

type VerificationStatusTx = oneshot::Sender<VerificationStatus>;

type SubscribeResponseTx = mpsc::UnboundedSender<(Pubkey, Response<RpcKeyedAccount>)>;

/*fn parse_transaction_logs(
    logs: Vec<web3::types::Log>,
) -> impl Iterator<Item = ParsedEthEvent> + DoubleEndedIterator {
    logs.into_iter()
        .map(ParsedEthEvent::try_from)
        .filter_map(|event| match event {
            Ok(event) => Some(event),
            Err(e) => {
                log::error!("Failed to parse transaction log: {:?}", e);
                None
            }
        })
}*/

#[derive(Debug, Copy, Clone, Hash, Eq, PartialEq)]
pub enum VerificationStatus {
    Exists,
    NotExists,
}

fn is_account_not_found(error: &anyhow::Error, pubkey: &Pubkey) -> bool {
    error
        .to_string()
        .contains(format!("AccountNotFound: pubkey={}", pubkey).as_str())
}

#[derive(thiserror::Error, Debug)]
enum SolSubscriberError {
    #[error("Service hasn't started yet")]
    EmptySubscribeTx,
}
