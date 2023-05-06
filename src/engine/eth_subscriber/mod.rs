use std::collections::hash_map;
use std::convert::TryFrom;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use dashmap::DashMap;
use either::Either;
use eth_ton_abi_converter::*;
use futures_util::StreamExt;
use rustc_hash::{FxHashMap, FxHashSet};
use tokio::sync::{oneshot, Notify, Semaphore};
use tokio::time::timeout;
use ton_types::UInt256;
use web3::api::Namespace;
use web3::types::{BlockNumber, FilterBuilder, H256, U256, U64};
use web3::{transports::Http, Transport};

use self::models::*;
use crate::config::*;
use crate::engine::bridge::*;
use crate::engine::keystore::*;
use crate::engine::ton_contracts::*;
use crate::utils::*;

mod contracts;
mod models;

/// A collection of EVM chain subscribers
pub struct EthSubscriberRegistry {
    /// EVM subscribers by chain id
    subscribers: DashMap<u32, Arc<EthSubscriber>>,
    /// Shared last block numbers
    last_block_numbers: Arc<LastBlockNumbersMap>,
}

impl EthSubscriberRegistry {
    /// Creates registry from configs
    pub async fn new<I>(networks: I) -> Result<Arc<Self>>
    where
        I: IntoIterator<Item = EthConfig>,
    {
        let registry = Arc::new(Self {
            subscribers: Default::default(),
            last_block_numbers: Arc::new(LastBlockNumbersMap::default()),
        });

        for config in networks {
            registry.new_subscriber(config).await?;
        }

        Ok(registry)
    }

    /// Starts all subscribers
    pub fn start(&self) {
        for subscriber in &self.subscribers {
            tracing::info!(chain_id = subscriber.chain_id, "starting subscriber");
            subscriber.start();
        }
    }

    pub fn get_subscriber(&self, chain_id: u32) -> Option<Arc<EthSubscriber>> {
        // Not cloning will deadlock
        self.subscribers.get(&chain_id).map(|x| x.clone())
    }

    pub fn subscribers(&self) -> &DashMap<u32, Arc<EthSubscriber>> {
        &self.subscribers
    }

    pub fn get_last_block_number(&self, chain_id: u32) -> Result<u64> {
        self.last_block_numbers
            .get(&chain_id)
            .map(|item| *item)
            .ok_or_else(|| EthSubscriberError::UnknownChainId.into())
    }

    pub fn get_last_block_numbers(&self) -> &Arc<FxDashMap<u32, u64>> {
        &self.last_block_numbers
    }

    async fn new_subscriber(&self, config: EthConfig) -> Result<()> {
        use dashmap::mapref::entry::Entry;

        let chain_id = config.chain_id;
        let subscriber = EthSubscriber::new(self.last_block_numbers.clone(), config)
            .await
            .with_context(|| format!("Failed to create EVM subscriber for chain id: {chain_id}"))?;

        match self.subscribers.entry(chain_id) {
            Entry::Vacant(entry) => {
                entry.insert(subscriber);
            }
            Entry::Occupied(entry) => {
                tracing::warn!(chain_id, "replacing existing ETH subscriber");
                entry.replace_entry(subscriber);
            }
        };

        Ok(())
    }
}

pub struct EthSubscriber {
    chain_id: u32,
    chain_id_str: String,
    config: EthConfig,
    api: EthApi,
    pool: Arc<Semaphore>,
    topics: parking_lot::RwLock<TopicsMap>,
    last_processed_block: Arc<AtomicU64>,
    last_block_numbers: Arc<LastBlockNumbersMap>,
    pending_confirmations: tokio::sync::Mutex<FxHashMap<EventId, PendingConfirmation>>,
    pending_confirmation_count: AtomicUsize,
    new_events_notify: Notify,
}

impl EthSubscriber {
    async fn new(
        last_block_numbers: Arc<LastBlockNumbersMap>,
        config: EthConfig,
    ) -> Result<Arc<Self>> {
        let chain_id = config.chain_id;
        let transport = web3::transports::Http::new(config.endpoint.as_str())?;
        let api = web3::api::Eth::new(transport);
        let pool = Arc::new(Semaphore::new(config.pool_size));

        let subscriber = Arc::new(Self {
            chain_id,
            chain_id_str: chain_id.to_string(),
            config,
            api,
            pool,
            topics: Default::default(),
            last_processed_block: Arc::new(AtomicU64::default()),
            last_block_numbers,
            pending_confirmations: Default::default(),
            pending_confirmation_count: Default::default(),
            new_events_notify: Notify::new(),
        });

        let last_processed_block = subscriber.get_current_block_number().await?;
        subscriber
            .last_processed_block
            .store(last_processed_block, Ordering::Release);
        subscriber
            .last_block_numbers
            .insert(chain_id, last_processed_block);

        Ok(subscriber)
    }

    pub fn chain_id_str(&self) -> &str {
        &self.chain_id_str
    }

    pub fn metrics(&self) -> EthSubscriberMetrics {
        EthSubscriberMetrics {
            last_processed_block: self.last_processed_block.load(Ordering::Acquire),
            pending_confirmation_count: self.pending_confirmation_count.load(Ordering::Acquire),
        }
    }

    pub async fn verify_relay_staker_address(
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
                tracing::info!(
                    verification_state = %settings.state_path.display(),
                    "failed to reset address verification state: {e:?}"
                );
            }
        };

        // Restore previous state
        match AddressVerificationState::try_load(&settings.state_path)? {
            // Ignore state for different address
            Some(state) if state.address != relay_address.0 => {
                tracing::warn!(
                    old_addr = hex::encode(state.address),
                    new_addr = hex::encode(relay_address.0),
                    "address verification state created for the different relay address, it will be ignored",
                );
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
                                tracing::info!(
                                    tx = transaction_id,
                                    block = hex::encode(block.as_bytes()),
                                    "ETH transaction found",
                                );
                                clear_state();
                                return Ok(());
                            }
                            // If it wasn't included, poll
                            None => {
                                tracing::info!(
                                    tx = transaction_id,
                                    "ETH transaction is still pending",
                                );
                                tokio::time::sleep(Duration::from_secs(10)).await;
                            }
                        },
                        // Ignore state for non-existing transaction
                        None => {
                            tracing::warn!(
                                "address verification state contains non-existing transaction, \
                                 it will be ignored"
                            );
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
                NetworkType::EVM(self.chain_id),
                "Failed getting balance",
            )
            .await?;

            if balance < min_balance {
                tracing::info!(
                    current_balance = %balance,
                    target_balance = %min_balance,
                    "insufficient balance",
                );
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
            .map_err(|err| web3::error::Error::Decoder(format!("{err:?}")))
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
    }

    pub fn subscribe(
        &self,
        address: ethabi::Address,
        topic_hash: [u8; 32],
        configuration_account: UInt256,
    ) {
        self.topics
            .write()
            .add_entry(address, topic_hash, configuration_account);
    }

    pub fn unsubscribe<I>(&self, subscriptions: I)
    where
        I: IntoIterator<Item = (ethabi::Address, [u8; 32], UInt256)>,
    {
        self.topics.write().remove_entries(subscriptions)
    }

    pub fn get_last_processed_block(&self) -> u64 {
        self.last_processed_block.load(Ordering::Acquire)
    }

    pub async fn verify(
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
                    status: None,
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

    fn start(self: &Arc<Self>) {
        let subscriber = Arc::downgrade(self);

        tokio::spawn(async move {
            loop {
                let subscriber = match subscriber.upgrade() {
                    Some(subscriber) => subscriber,
                    None => return,
                };

                if let Err(e) = subscriber.update().await {
                    tracing::error!("error occurred during EVM subscriber update: {e:?}");
                }
            }
        });
    }

    async fn update(&self) -> Result<()> {
        if self.pending_confirmations.lock().await.is_empty() {
            // Wait until new events appeared or idle poll interval passed.
            // NOTE: Idle polling is needed there to prevent large intervals from occurring (e.g. BSC)
            tokio::select! {
                _ = self.new_events_notify.notified() => {},
                _ = tokio::time::sleep(Duration::from_secs(self.config.poll_interval_sec)) => {},
            }
        }

        let chain_id = self.chain_id;
        tracing::info!(
            chain_id,
            pending_confirmations = self.pending_confirmation_count.load(Ordering::Acquire),
            "updating EVM-{chain_id} subscriber",
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
            NetworkType::EVM(self.chain_id),
            "get actual ethereum height",
        )
        .await
        {
            Ok(height) => height,
            Err(e) if is_incomplete_message(&e) => return Ok(()),
            Err(e) => {
                return Err(e)
                    .with_context(|| format!("Failed to get actual EVM-{chain_id} height"))
            }
        };

        tracing::info!(
            chain_id = self.chain_id,
            current_block,
            "got new EVM-{chain_id} block height",
        );

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
                tracing::warn!(
                    chain_id,
                    current_block,
                    last_processed_block,
                    max_block_range,
                    "truncating blocks query for EVM-{chain_id}",
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
            NetworkType::EVM(self.chain_id),
            "process block",
        )
        .await
        {
            Ok(Ok(events)) => events,
            Ok(Err(e)) => {
                return Err(e).with_context(|| {
                    format!(
                        "failed processing EVM-{chain_id} blocks in range \
                        from {last_processed_block} to {current_block}",
                    )
                })
            }
            Err(_) => {
                tracing::warn!(
                    chain_id,
                    current_block,
                    last_processed_block,
                    "EVM-{chain_id} blocks processing timeout",
                );
                return Ok(());
            }
        };

        // Update pending confirmations
        let mut pending_confirmations = self.pending_confirmations.lock().await;
        for event in events {
            if let Some(confirmation) = pending_confirmations.get_mut(&event.event_id()) {
                match event {
                    ParsedEthEvent::Removed(event) => {
                        tracing::info!(
                            chain_id,
                            tx = hex::encode(event.transaction_hash.0),
                            event_index = event.event_index,
                            "log removed"
                        );
                        confirmation.status = Some(VerificationStatus::NotExists {
                            reason: "Log removed".to_owned(),
                        })
                    }
                    ParsedEthEvent::Received(event) => {
                        tracing::info!(
                            chain_id,
                            tx = hex::encode(event.transaction_hash.0),
                            event_index = event.event_index,
                            "log received"
                        );
                        confirmation.status = Some(confirmation.check(event));
                    }
                }
            }
        }

        // Resolve verified confirmations
        let events_to_check = futures_util::stream::FuturesUnordered::new();
        pending_confirmations.retain(|&event_id, confirmation| {
            if confirmation.target_block > current_block {
                return true;
            }

            tracing::info!(
                chain_id,
                current_block,
                target_block = confirmation.target_block,
                block = confirmation.vote_data.event_block.to_hex_string(),
                tx = confirmation.vote_data.event_transaction.to_hex_string(),
                event_index = confirmation.vote_data.event_index,
                status = ?confirmation.status,
                "found pending confirmation to resolve"
            );

            let status = match confirmation.status.clone() {
                None | Some(VerificationStatus::Exists) => {
                    events_to_check.push(async move {
                        let result = self.find_event(&event_id).await;
                        (event_id, result)
                    });
                    return true;
                }
                Some(not_exists) => not_exists,
            };

            if let Some(tx) = confirmation.status_tx.take() {
                tx.send(status).ok();
            }

            false
        });

        let events_to_check = events_to_check
            .collect::<Vec<(EventId, Result<Option<ParsedEthEvent>>)>>()
            .await;
        tracing::info!(chain_id, current_block, ?events_to_check);

        for (event_id, result) in events_to_check {
            if let hash_map::Entry::Occupied(mut entry) = pending_confirmations.entry(event_id) {
                let status = match result {
                    Ok(Some(ParsedEthEvent::Received(event))) => entry.get_mut().check(event),
                    Ok(Some(ParsedEthEvent::Removed(_))) => VerificationStatus::NotExists {
                        reason: "Log removed".to_owned(),
                    },
                    Ok(None) => VerificationStatus::NotExists {
                        reason: "Not found".to_owned(),
                    },
                    Err(e) => {
                        tracing::error!(
                            chain_id,
                            tx = hex::encode(event_id.0 .0),
                            event_idnex = event_id.1,
                            "failed to check EVM-{chain_id} event: {e:?}",
                        );
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
    }

    async fn process_blocks(
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
                NetworkType::EVM(self.chain_id),
                "get contract logs",
            )
            .await
            .context("Failed getting eth logs")?
        };

        Ok(Either::Right(parse_transaction_logs(logs)))
    }

    async fn find_event(
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
                NetworkType::EVM(self.chain_id),
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
    }

    async fn get_balance(&self, address: ethabi::Address) -> Result<U256> {
        Ok(self.api.balance(address, None).await?)
    }

    async fn get_current_block_number(&self) -> Result<u64> {
        let result = timeout(
            Duration::from_secs(self.config.get_timeout_sec),
            self.api.block_number(),
        )
        .await
        .context("Timeout getting height")??;
        Ok(result.as_u64())
    }
}

#[derive(Debug, Copy, Clone)]
pub struct EthSubscriberMetrics {
    pub last_processed_block: u64,
    pub pending_confirmation_count: usize,
}

#[derive(Default)]
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
type LastBlockNumbersMap = FxDashMap<u32, u64>;

struct PendingConfirmation {
    vote_data: EthTonEventVoteData,
    status_tx: Option<VerificationStatusTx>,
    event_emitter: [u8; 20],
    event_abi: Arc<EthEventAbi>,
    target_block: u64,
    status: Option<VerificationStatus>,
}

impl PendingConfirmation {
    fn check(&self, event: ReceivedEthEvent) -> VerificationStatus {
        let vote_data = &self.vote_data;

        // NOTE: event_index and transaction_hash are already checked while searching
        // ETH event log, but here they are also checked just in case.
        let result = if event.address.0 != self.event_emitter {
            Err(format!(
                "Event emitter address mismatch. From event: {:x}. Expected: {}",
                event.address,
                hex::encode(self.event_emitter.as_slice())
            ))
        } else if &event.topic_hash != self.event_abi.get_eth_topic_hash() {
            Err(format!(
                "Topic hash mismatch. From event: {:x}. Expected: {:x}",
                event.topic_hash,
                self.event_abi.get_eth_topic_hash(),
            ))
        } else if event.event_index != vote_data.event_index {
            Err(format!(
                "Event index mismatch. Received: {}. Expected: {}",
                vote_data.event_index, event.event_index,
            ))
        } else if &event.transaction_hash.0 != vote_data.event_transaction.as_slice() {
            Err(format!(
                "Transaction hash mismatch. Received: {}. Expected: {:x}",
                vote_data.event_transaction.to_hex_string(),
                event.transaction_hash,
            ))
        } else if event.block_number != vote_data.event_block_number as u64 {
            Err(format!(
                "Block number mismatch. Received: {}. Expected: {}",
                vote_data.event_block_number, event.block_number,
            ))
        } else if &event.block_hash.0 != vote_data.event_block.as_slice() {
            Err(format!(
                "Block hash mismatch. Received: {}. Expected: {:x}",
                vote_data.event_block.to_hex_string(),
                event.block_hash,
            ))
        } else {
            // Event data is checked last, because it is quite expensive. `topic_hash`
            // is checked earlier to also skip this without decoding data.
            match self.event_abi.decode_and_map(&event.data) {
                Ok(data) if data.repr_hash() == vote_data.event_data.repr_hash() => Ok(()),
                Ok(data) => {
                    let boc = ton_types::serialize_toc(&data).unwrap_or_default();
                    Err(format!(
                        "Event data mismatch. Expected: {}",
                        base64::encode(boc)
                    ))
                }
                Err(e) => Err(format!("Failed to convert event data: {e:?}")),
            }
        };

        match result {
            Ok(()) => VerificationStatus::Exists,
            Err(reason) => VerificationStatus::NotExists { reason },
        }
    }
}

type VerificationStatusTx = oneshot::Sender<VerificationStatus>;

fn parse_transaction_logs(
    logs: Vec<web3::types::Log>,
) -> impl Iterator<Item = ParsedEthEvent> + DoubleEndedIterator {
    logs.into_iter()
        .map(ParsedEthEvent::try_from)
        .filter_map(|event| match event {
            Ok(event) => Some(event),
            Err(e) => {
                tracing::error!("failed to parse transaction log: {e:?}");
                None
            }
        })
}

fn is_incomplete_message(error: &anyhow::Error) -> bool {
    error
        .to_string()
        .contains("hyper::Error(IncompleteMessage)")
}

#[derive(thiserror::Error, Debug)]
enum EthSubscriberError {
    #[error("Unknown chain id")]
    UnknownChainId,
}
