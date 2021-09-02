use std::convert::TryFrom;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;

use anyhow::{Context, Result};
use dashmap::DashMap;
use parking_lot::RwLock;
use tiny_adnl::utils::*;
use tokio::sync::{mpsc, Semaphore};
use tokio::time::timeout;
use ton_types::UInt256;
use web3::api::Namespace;
use web3::types::{BlockNumber, FilterBuilder, H256, U64};
use web3::{transports::Http, Transport};

use self::models::ReceivedEthEvent;
use crate::config::*;
use crate::engine::state::*;
use crate::engine::ton_contracts::*;
use crate::filter_log;
use crate::utils::*;

pub mod models;

pub struct EthSubscriberRegistry {
    state: Arc<State>,
    subscribers: DashMap<u32, Arc<EthSubscriber>>,
    last_block_numbers: Arc<LastBlockNumbersMap>,
}

impl EthSubscriberRegistry {
    pub async fn new<I>(state: Arc<State>, networks: I) -> Result<Arc<Self>>
    where
        I: IntoIterator<Item = (u32, EthConfig)>,
    {
        let last_block_numbers = Arc::new(
            state
                .get_connection()
                .await
                .and_then(|conn| EthState(conn).get_last_block_numbers())
                .map(|map| {
                    let mut result =
                        FxDashMap::with_capacity_and_hasher(map.len(), Default::default());
                    result.extend(map);
                    result
                })?,
        );

        let registry = Arc::new(Self {
            state,
            subscribers: Default::default(),
            last_block_numbers,
        });

        for (chain_id, config) in networks {
            registry.new_subscriber(chain_id, config).await?;
        }

        Ok(registry)
    }

    pub async fn new_subscriber(&self, chain_id: u32, config: EthConfig) -> Result<()> {
        use dashmap::mapref::entry::Entry;

        let subscriber = EthSubscriber::new(
            self.state.clone(),
            self.last_block_numbers.clone(),
            chain_id,
            config,
        )
        .await?;

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
}

pub struct EthSubscriber {
    chain_id: u32,
    config: EthConfig,
    api: EthApi,
    pool: Arc<Semaphore>,
    topics: RwLock<TopicsMap>,
    current_height: Arc<AtomicU64>,
    state: Arc<State>,
    last_block_numbers: Arc<LastBlockNumbersMap>,
}

impl EthSubscriber {
    async fn new(
        state: Arc<State>,
        last_block_numbers: Arc<LastBlockNumbersMap>,
        chain_id: u32,
        config: EthConfig,
    ) -> Result<Arc<Self>> {
        let transport = web3::transports::Http::new(config.endpoint.as_str())?;
        let api = web3::api::Eth::new(transport);
        let pool = Arc::new(Semaphore::new(config.pool_size));

        let current_height = {
            let mut conn = state.get_connection().await?;
            conn.eth_state().get_last_block_number(chain_id)?
        };

        Ok(Arc::new(Self {
            chain_id,
            config,
            api,
            pool,
            topics: Default::default(),
            current_height: Arc::new(current_height.into()),
            state,
            last_block_numbers,
        }))
    }

    pub fn subscribe(
        &self,
        configuration_account: UInt256,
        address: ethabi::Address,
        topic_hash: [u8; 32],
        blocks_to_confirm: u16,
    ) {
        self.topics.write().add_entry(
            configuration_account,
            address,
            topic_hash,
            blocks_to_confirm,
        );
    }

    pub fn unsubscribe(
        &self,
        configuration_account: UInt256,
        address: ethabi::Address,
        topic_hash: [u8; 32],
    ) {
        self.topics
            .write()
            .remove_entry(configuration_account, address, topic_hash)
    }

    pub fn get_last_processed_block(&self) -> Result<u64> {
        self.last_block_numbers
            .get(&self.chain_id)
            .map(|item| *item)
            .ok_or_else(|| EthSubscriberError::UnknownChainId.into())
    }

    pub fn send_message(
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
            || self.api.send_transaction(request.clone()).await,
            generate_default_timeout_config(self.config.maximum_failed_responses_time),
            "send transaction",
        )
        .await
        .context("Failed sending ETH transaction")?;

        log::warn!("Sent ETH transaction: {:?}", hash);

        Ok(())
    }

    pub async fn check_transaction(&self, vote_data: EthEventVoteData) -> Result<ReceivedEthEvent> {
        let _permission = self.pool.acquire().await;
        let receipt = retry(
            || timeout(self.config.get_timeout, self.api.transaction_receipt(hash)),
            generate_default_timeout_config(self.config.maximum_failed_responses_time),
            "get transaction receipt",
        )
        .await
        .context("Timed out getting receipt")?
        .context("Failed getting logs")?
        .with_context(|| format!("No logs found for {}", hex::encode(&hash.0)))?;

        match receipt.status {
            Some(a) => {
                if a.as_u64() == 0 {
                    anyhow::bail!("Tx has failed status")
                }
            }
            None => anyhow::bail!("No status field in eth node answer"),
        };

        receipt
            .logs
            .into_iter()
            .map(ReceivedEthEvent::try_from)
            .filter_map(|x| filter_log!(x, "Failed mapping log to event in receipt logs"))
            .find(|x| x.transaction_hash == hash && x.event_index == event_index)
            .context("No events for tx. Assuming confirmation is fake")
    }

    pub fn run(self: &Arc<Self>) -> EventsBatchesRx {
        // TODO: replace `DB ERROR` with proper handlers

        let (events_tx, events_rx) = mpsc::unbounded_channel();

        let api_request_strategy = generate_fixed_timeout_config(
            self.config.poll_interval,
            self.config.maximum_failed_responses_time,
        );

        let subscriber = Arc::downgrade(self);

        tokio::spawn(async move {
            loop {
                let subscriber = match subscriber.upgrade() {
                    Some(subscriber) => subscriber,
                    None => return,
                };

                let current_height = match retry(
                    || subscriber.get_current_height(),
                    api_request_strategy,
                    "get actual ethereum height",
                )
                .await
                {
                    Ok(height) => height,
                    Err(e) if is_incomplete_message(&e) => continue,
                    Err(e) => {
                        log::error!("Failed getting actual ethereum height: {}", e);
                        continue;
                    }
                };

                let last_height = subscriber
                    .use_eth_state(|state| state.get_last_block_number(subscriber.chain_id))
                    .await
                    .expect("DB ERROR");

                if last_height == current_height {
                    tokio::time::sleep(subscriber.config.poll_interval).await;
                    continue;
                }

                let events = match retry(
                    || {
                        timeout(
                            subscriber.config.get_timeout,
                            subscriber.process_block(last_height, current_height),
                        )
                    },
                    api_request_strategy,
                    "process block",
                )
                .await
                {
                    Ok(Ok(events)) => events.into_iter().map(|event| {
                        let target_event_block = event.block_number + 12;

                        StoredEthEvent {
                            event,
                            target_event_block: 0,
                            status: StoredEthEventStatus::InProgress,
                        }
                    }),
                    Ok(Err(e)) => {
                        log::error!(
                            "Failed processing eth block: {:?} in time range from {} to {}",
                            e,
                            last_height,
                            current_height
                        );
                        continue;
                    }
                    Err(_) => {
                        log::warn!("Timed out processing eth blocks.");
                        continue;
                    }
                };

                if let Err(e) = subscriber.save_events(current_height, &events).await {
                    log::error!("Failed to save new ETH events: {:?}", e);
                    continue;
                }
                subscriber
                    .last_block_numbers
                    .insert(subscriber.chain_id, current_height);

                let batch = EventsBatch {
                    from: last_height,
                    to: current_height,
                    events,
                };

                if events_tx.send(batch).is_err() {
                    return;
                }
            }
        });

        events_rx
    }

    async fn process_block(&self, from: u64, to: u64) -> Result<Vec<ReceivedEthEvent>> {
        let filter = match self.topics.read().make_filter(from, to) {
            Some(filter) => filter,
            None => return Ok(Vec::new()),
        };

        let _permit = self.pool.acquire().await;
        let logs: Vec<web3::types::Log> = retry(
            || {
                let transport = self.api.transport();
                let request = transport.execute("eth_getLogs", vec![filter.clone()]);
                web3::helpers::CallFuture::new(request)
            },
            generate_default_timeout_config(self.config.maximum_failed_responses_time),
            "get contract logs",
        )
        .await
        .context("Failed getting eth logs")?;

        let logs = logs
            .into_iter()
            .map(ReceivedEthEvent::try_from)
            .filter_map(|x| filter_log!(x, "Failed mapping log to event"))
            .collect();
        Ok(logs)
    }

    async fn save_events(&self, last_block_number: u64, events: &[ReceivedEthEvent]) -> Result<()> {
        self.use_eth_state(|mut state| {
            if events.is_empty() {
                state.set_last_block_number(self.chain_id, last_block_number)
            } else {
                state.save_events(self.chain_id, events, last_block_number)
            }
        })
        .await
    }

    async fn get_current_height(&self) -> Result<u64> {
        let result = timeout(self.config.get_timeout, self.api.block_number())
            .await
            .context("Timeout getting height")??;
        Ok(result.as_u64())
    }

    async fn use_eth_state<F, T>(&self, f: F) -> Result<T>
    where
        F: FnOnce(EthState<PooledConnection<'_>>) -> Result<T>,
    {
        self.state.get_connection().await.map(EthState).and_then(f)
    }
}

#[derive(Debug, Clone)]
pub struct EventsBatch {
    pub from: u64,
    pub to: u64,
    pub events: Vec<ReceivedEthEvent>,
}

pub type EventsBatchesRx = mpsc::UnboundedReceiver<EventsBatch>;

#[derive(Default)]
struct TopicsMap {
    entries: FxHashMap<TopicsMapEntry, u16>,
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
        configuration_account: UInt256,
        address: ethabi::Address,
        topic_hash: [u8; 32],
        blocks_to_confirm: u16,
    ) {
        if self.entries.insert(
            TopicsMapEntry {
                configuration_account,
                address,
                topic_hash,
            },
            blocks_to_confirm,
        ) {
            self.update();
        }
    }

    fn remove_entry(
        &mut self,
        configuration_account: UInt256,
        address: ethabi::Address,
        topic_hash: [u8; 32],
    ) {
        if self.entries.remove(&TopicsMapEntry {
            configuration_account,
            address,
            topic_hash,
        }) {
            self.update()
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
    configuration_account: UInt256,
    address: ethabi::Address,
    topic_hash: [u8; 32],
}

type EthApi = web3::api::Eth<Http>;
type LastBlockNumbersMap = FxDashMap<u32, u64>;

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
