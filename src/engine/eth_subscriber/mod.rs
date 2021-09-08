use std::collections::hash_map;
use std::convert::TryFrom;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use anyhow::{Context, Result};
use dashmap::DashMap;
use either::Either;
use futures::StreamExt;
use tiny_adnl::utils::*;
use tokio::sync::{oneshot, Semaphore};
use tokio::time::timeout;
use ton_types::UInt256;
use web3::api::Namespace;
use web3::types::{BlockNumber, FilterBuilder, H256, U256, U64};
use web3::{transports::Http, Transport};

use self::models::*;
use crate::config::*;
use crate::engine::ton_contracts::*;
use crate::utils::*;
use std::time::Duration;
use ton_block::{MsgAddressInt, Serializable};
use web3::contract::Options;

mod contracts;
pub mod models;

pub struct EthSubscriberRegistry {
    subscribers: DashMap<u32, Arc<EthSubscriber>>,
    last_block_numbers: Arc<LastBlockNumbersMap>,
}

impl EthSubscriberRegistry {
    pub async fn new<I>(networks: I) -> Result<Arc<Self>>
    where
        I: IntoIterator<Item = (u32, EthConfig)>,
    {
        let registry = Arc::new(Self {
            subscribers: Default::default(),
            last_block_numbers: Arc::new(LastBlockNumbersMap::default()),
        });

        for (chain_id, config) in networks {
            registry.new_subscriber(chain_id, config).await?;
        }

        Ok(registry)
    }

    pub fn start(&self) {
        for subscriber in &self.subscribers {
            subscriber.start();
        }
    }

    pub fn get_subscriber(&self, chain_id: u32) -> Option<Arc<EthSubscriber>> {
        // Not cloning will deadlock
        self.subscribers.get(&chain_id).map(|x| x.clone())
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

    async fn new_subscriber(&self, chain_id: u32, config: EthConfig) -> Result<()> {
        use dashmap::mapref::entry::Entry;

        let subscriber =
            EthSubscriber::new(self.last_block_numbers.clone(), chain_id, config).await?;

        match self.subscribers.entry(chain_id) {
            Entry::Vacant(entry) => {
                entry.insert(subscriber);
            }
            Entry::Occupied(entry) => {
                log::warn!("Replacing existing ETH subscriber with id {}", chain_id);
                entry.replace_entry(subscriber);
            }
        };

        Ok(())
    }
}

pub struct EthSubscriber {
    staking_account: Option<ethabi::Address>,
    chain_id: u32,
    config: EthConfig,
    api: EthApi,
    pool: Arc<Semaphore>,
    topics: parking_lot::RwLock<TopicsMap>,
    last_processed_block: Arc<AtomicU64>,
    last_block_numbers: Arc<LastBlockNumbersMap>,
    pending_confirmations: tokio::sync::Mutex<FxHashMap<EventId, PendingConfirmation>>,
}

impl EthSubscriber {
    async fn new(
        last_block_numbers: Arc<LastBlockNumbersMap>,
        chain_id: u32,
        config: EthConfig,
    ) -> Result<Arc<Self>> {
        let transport = web3::transports::Http::new(config.endpoint.as_str())?;
        let api = web3::api::Eth::new(transport);
        let pool = Arc::new(Semaphore::new(config.pool_size));

        let subscriber = Arc::new(Self {
            chain_id,
            config,
            api,
            pool,
            topics: Default::default(),
            last_processed_block: Arc::new(AtomicU64::default()),
            last_block_numbers,
            pending_confirmations: Default::default(),
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

    async fn account_is_ready(&self, ton_address: MsgAddressInt) -> Result<()> {
        const GWEI: u64 = 1000000000;
        let account = self
            .staking_account
            .as_ref()
            .context("Account is not set")?;
        loop {
            let balance = retry(
                || self.clone().get_balance(account.clone()),
                crate::utils::generate_default_timeout_config(Duration::from_secs(60)),
                "Failed getting balance",
            )
            .await?;
            // 0.05 ETH
            if balance >= U256::from(50000000 * GWEI) {
                break;
            }
        }
        let contract = contracts::staking_contract(self.api.clone(), account.clone())?;
        let workchain_id = ethabi::Token::Int(U256::from(ton_address.workchain_id()));
        let address_body = ethabi::Token::Uint(U256::from_big_endian(
            &ton_address.address().write_to_bytes()?,
        ));
        contract.call(
            "verify_relay_staker_address",
            [workchain_id, address_body],
            account.clone(),
            Options {
                gas: Some((U256::from(200 * GWEI))),
                gas_price: None,
                value: None,
                nonce: None,
                condition: None,
                transaction_type: None,
                access_list: None,
            },
        );
        Ok(())
    }

    async fn get_balance(&self, address: ethabi::Address) -> Result<U256> {
        Ok(self.api.balance(address, None).await?)
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
        vote_data: EthEventVoteData,
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
                    vote_data,
                    status_tx: Some(tx),
                    event_abi,
                    target_block,
                    status: PendingConfirmationStatus::InProcess,
                },
            );

            rx
        };

        let status = rx.await?;
        Ok(status)
    }

    pub async fn send_message(
        &self,
        from: ethabi::Address,
        to: ethabi::Address,
        data: Vec<u8>,
    ) -> Result<()> {
        let request = web3::types::TransactionRequest {
            from,
            to: Some(to),
            data: Some(web3::types::Bytes(data.clone())),
            ..Default::default()
        };

        let hash: H256 = retry(
            || self.api.send_transaction(request.clone()),
            generate_default_timeout_config(self.config.maximum_failed_responses_time),
            "send transaction",
        )
        .await
        .context("Failed sending ETH transaction")?;

        log::warn!("Sent ETH transaction: {:?}", hash);

        Ok(())
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
                    log::error!("Error occurred during EVM node subscriber update: {:?}", e);
                }
            }
        });
    }

    async fn update(&self) -> Result<()> {
        let api_request_strategy = generate_fixed_timeout_config(
            self.config.poll_interval,
            self.config.maximum_failed_responses_time,
        );

        let current_block = match retry(
            || self.get_current_block_number(),
            api_request_strategy,
            "get actual ethereum height",
        )
        .await
        {
            Ok(height) => height,
            Err(e) if is_incomplete_message(&e) => return Ok(()),
            Err(e) => return Err(e).context("Failed to get  actual ethereum height"),
        };

        let last_processed_block = self.last_processed_block.load(Ordering::Acquire);
        if last_processed_block == current_block {
            tokio::time::sleep(self.config.poll_interval).await;
            return Ok(());
        }

        let events = match retry(
            || {
                timeout(
                    self.config.get_timeout,
                    self.process_block(last_processed_block, current_block),
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
                        "Failed processing eth block in the time range from {} to {}",
                        last_processed_block, current_block
                    )
                })
            }
            Err(_) => {
                log::warn!("Timed out processing eth blocks.");
                return Ok(());
            }
        };

        let mut pending_confirmations = self.pending_confirmations.lock().await;
        for event in events {
            if let Some(confirmation) = pending_confirmations.get_mut(&event.event_id()) {
                match event {
                    ParsedEthEvent::Removed(_) => {
                        confirmation.status = PendingConfirmationStatus::Invalid
                    }
                    ParsedEthEvent::Received(event) => {
                        confirmation.status = confirmation.check(event).into();
                    }
                }
            }
        }

        let mut events_to_check = futures::stream::FuturesOrdered::new();
        pending_confirmations.retain(|&event_id, confirmation| {
            if confirmation.target_block > current_block {
                return true;
            }

            let status = match confirmation.status {
                PendingConfirmationStatus::InProcess => {
                    events_to_check.push(async move {
                        let result = self.find_event(&event_id).await;
                        (event_id, result)
                    });
                    return true;
                }
                PendingConfirmationStatus::Valid => VerificationStatus::Exists,
                PendingConfirmationStatus::Invalid => VerificationStatus::NotExists,
            };

            if let Some(tx) = confirmation.status_tx.take() {
                tx.send(status).ok();
            }

            false
        });

        let events_to_check = events_to_check
            .collect::<Vec<(EventId, Result<Option<ParsedEthEvent>>)>>()
            .await;

        for (event_id, result) in events_to_check {
            if let hash_map::Entry::Occupied(mut entry) = pending_confirmations.entry(event_id) {
                let status = match result {
                    Ok(Some(ParsedEthEvent::Received(event))) => entry.get_mut().check(event),
                    Ok(_) => VerificationStatus::NotExists,
                    Err(e) => {
                        log::error!("Failed to check ETH event: {:?}", e);
                        continue;
                    }
                };

                if let Some(tx) = entry.get_mut().status_tx.take() {
                    tx.send(status).ok();
                }

                entry.remove();
            }
        }

        drop(pending_confirmations);

        // Update last processed block while
        if let Some(mut entry) = self.last_block_numbers.get_mut(&self.chain_id) {
            *entry.value_mut() = current_block;
            self.last_processed_block
                .store(current_block, Ordering::Release);
        }

        Ok(())
    }

    async fn process_block(
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
                generate_default_timeout_config(self.config.maximum_failed_responses_time),
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
                        self.config.get_timeout,
                        self.api.transaction_receipt(*transaction_hash),
                    )
                },
                generate_default_timeout_config(self.config.maximum_failed_responses_time),
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

    async fn get_current_block_number(&self) -> Result<u64> {
        let result = timeout(self.config.get_timeout, self.api.block_number())
            .await
            .context("Timeout getting height")??;
        Ok(result.as_u64())
    }
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
    vote_data: EthEventVoteData,
    status_tx: Option<VerificationStatusTx>,
    event_abi: Arc<EthEventAbi>,
    target_block: u64,
    status: PendingConfirmationStatus,
}

impl PendingConfirmation {
    fn check(&self, event: ReceivedEthEvent) -> VerificationStatus {
        let vote_data = &self.vote_data;

        match self.event_abi.decode_and_map(&event.data) {
            Ok(data)
                if data.repr_hash() == vote_data.event_data.repr_hash()
                    && event.block_number == vote_data.event_block_number as u64
                    && &event.block_hash.0 == vote_data.event_block.as_slice() =>
            {
                VerificationStatus::Exists
            }
            _ => VerificationStatus::NotExists,
        }
    }
}

enum PendingConfirmationStatus {
    InProcess,
    Valid,
    Invalid,
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
                log::error!("Failed to parse transaction log: {:?}", e);
                None
            }
        })
}

#[derive(Debug, Copy, Clone, Hash, Eq, PartialEq)]
pub enum VerificationStatus {
    Exists,
    NotExists,
}

impl From<VerificationStatus> for PendingConfirmationStatus {
    fn from(status: VerificationStatus) -> Self {
        match status {
            VerificationStatus::Exists => Self::Valid,
            VerificationStatus::NotExists => Self::Invalid,
        }
    }
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
