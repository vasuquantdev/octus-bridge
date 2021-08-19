use std::convert::TryFrom;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;

use anyhow::{Context, Result};
use dashmap::{DashMap, DashSet};
use futures::{SinkExt, Stream, StreamExt};
use nekoton_utils::TrustMe;
use tap::Pipe;
use tokio::sync::Semaphore;
use tokio::time::timeout;
use uuid::Uuid;
use web3::transports::Http;
use web3::types::U64;
use web3::types::{BlockNumber, FilterBuilder, H256};
use web3::Web3;

use self::eth_config::EthConfig;
use self::models::StoredEthEvent;
use self::utils::*;
use crate::engine::state::*;
use crate::filter_log;
use crate::utils::*;

mod eth_config;
pub mod models;
mod utils;

pub struct EthSubscriberRegistry {
    state: Arc<State>,
    subscribed: DashMap<u8, Arc<EthSubscriber>>,
}

impl EthSubscriberRegistry {
    pub fn new(state: Arc<State>) -> Result<Arc<Self>> {
        // TODO: add config and create subscriber for each chain id

        Ok(Arc::new(Self {
            state,
            subscribed: Default::default(),
        }))
    }

    pub async fn new_subscriber(&self, chain_id: u8, config: EthConfig) -> Result<()> {
        use dashmap::mapref::entry::Entry;

        let subscriber = EthSubscriber::new(self.state.clone(), chain_id, config).await?;

        match self.subscribed.entry(chain_id) {
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

    pub fn get_subscriber(&self, chain_id: u8) -> Option<Arc<EthSubscriber>> {
        // Not cloning will deadlock
        self.subscribed.get(&chain_id).map(|x| x.clone())
    }
}

#[derive(Clone)]
pub struct EthSubscriber {
    chain_id: u8,
    config: EthConfig,
    web3: Web3<Http>,
    pool: Arc<Semaphore>,
    topics: DashSet<[u8; 32]>,
    addresses: DashSet<ethabi::Address>,
    current_height: Arc<AtomicU64>,
    state: Arc<State>,
}

impl EthSubscriber {
    async fn new(state: Arc<State>, chain_id: u8, config: EthConfig) -> Result<Arc<Self>> {
        let transport = web3::transports::Http::new(config.endpoint.as_str())?;
        let web3 = web3::Web3::new(transport);
        let pool = Arc::new(Semaphore::new(config.pool_size));

        let current_height = {
            let conn = state.get_connection().await?;
            conn.eth_state().get_last_block_id(chain_id)?
        };

        Ok(Arc::new(Self {
            chain_id,
            config,
            web3,
            pool,
            topics: Default::default(),
            addresses: Default::default(),
            current_height: Arc::new(current_height.into()),
            state,
        }))
    }

    async fn use_eth_state<F, T>(&self, f: F) -> Result<T>
    where
        F: FnOnce(EthState<PooledConnection<'_>>) -> Result<T>,
    {
        self.state.get_connection().await.map(EthState).and_then(f)
    }

    pub fn run(self: &Arc<Self>) -> impl Stream<Item = Uuid> {
        // TODO: replace `DB error` with proper handlers

        let (mut events_tx, events_rx) = futures::channel::mpsc::unbounded();
        let this = Arc::downgrade(self);
        tokio::spawn(async move {
            loop {
                let this = match this.upgrade() {
                    Some(a) => a,
                    None => {
                        log::error!("Eth subscriber is dropped.");
                        return;
                    }
                };
                let ethereum_actual_height = match retry(
                    || this.get_current_height(),
                    generate_fixed_config(
                        this.config.poll_interval,
                        this.config.maximum_failed_responses_time,
                    ),
                    "get actual ethereum height",
                )
                .await
                {
                    Ok(a) => a,
                    Err(e) => {
                        if e.to_string().contains("hyper::Error(IncompleteMessage)") {
                            continue;
                        }
                        log::error!("Failed getting actual ethereum height: {}", e);
                        continue;
                    }
                };

                let last_id = this
                    .use_eth_state(|state| state.get_last_block_id(this.chain_id))
                    .await
                    .expect("DB ERROR");

                if last_id == ethereum_actual_height {
                    tokio::time::sleep(this.config.poll_interval).await;
                    continue;
                }
                let logs = match retry(
                    || {
                        timeout(
                            this.config.get_timeout,
                            this.process_block(last_id, ethereum_actual_height),
                        )
                    },
                    generate_fixed_config(
                        this.config.poll_interval,
                        this.config.maximum_failed_responses_time,
                    ),
                    "process block",
                )
                .await
                {
                    Ok(Ok(a)) => a,
                    Ok(Err(e)) => {
                        log::error!(
                            "FATAL ERROR. Failed processing eth block: {:#?}. From: {}, To: {}",
                            e,
                            last_id,
                            ethereum_actual_height
                        );
                        continue;
                    }
                    Err(_) => {
                        log::warn!("Timed out processing eth blocks.");
                        continue;
                    }
                };

                for uuid in this.save_logs(logs).await.expect("DB ERROR") {
                    if let Err(e) = events_tx.send(uuid).await {
                        log::error!("CRITICAL ERROR: failed sending event notify: {}", e);
                        return;
                    }
                }

                this.use_eth_state(|state| {
                    state.block_processed(ethereum_actual_height, this.chain_id)
                })
                .await
                .expect("DB error");
            }
        });
        events_rx
    }

    async fn process_block(&self, from: u64, to: u64) -> Result<Vec<StoredEthEvent>> {
        let (addresses, topics): (Vec<_>, Vec<_>) = {
            (
                self.addresses.iter().map(|x| *x.key()).collect(),
                self.topics
                    .iter()
                    .map(|x| *x.key())
                    .map(H256::from)
                    .collect(),
            )
        };

        if addresses.is_empty() && topics.is_empty() {
            return Err(anyhow::anyhow!("Addresses and topics are empty. Cowardly refusing to process all ethereum transactions"));
        }

        let filter = FilterBuilder::default()
            .address(addresses)
            .topics(Some(topics), None, None, None)
            .from_block(BlockNumber::Number(U64::from(from)))
            .to_block(BlockNumber::Number(U64::from(to)))
            .build();

        let _permit = self.pool.acquire().await;
        let logs = retry(
            || self.web3.eth().logs(filter.clone()),
            generate_default_timeout_config(self.config.maximum_failed_responses_time),
            "get contract logs",
        )
        .await
        .context("Failed getting eth logs")?;
        let logs = logs
            .into_iter()
            .map(StoredEthEvent::try_from)
            .filter_map(|x| filter_log!(x, "Failed mapping log to event"))
            .collect();
        Ok(logs)
    }

    pub async fn check_transaction(&self, hash: H256, event_index: u32) -> Result<StoredEthEvent> {
        let _permission = self.pool.acquire().await;
        let receipt = retry(
            || {
                timeout(
                    self.config.get_timeout,
                    self.web3.eth().transaction_receipt(hash),
                )
            },
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
            .map(StoredEthEvent::try_from)
            .filter_map(|x| filter_log!(x, "Failed mapping log to event in receipt logs"))
            .find(|x| x.tx_hash == hash && x.event_index == event_index)
            .context("No events for tx. Assuming confirmation is fake")
    }

    async fn save_logs(&self, logs: Vec<StoredEthEvent>) -> Result<Vec<Uuid>> {
        // TODO: use transaction

        let connection = self.state.get_connection().await.map(EthState)?;

        let len = logs.len();
        logs.into_iter()
            .try_fold(Vec::with_capacity(len), |mut uuids, event| {
                uuids.push(connection.new_event(event)?);
                Ok(uuids)
            })
    }

    pub async fn subscribe_topic(&self, abi: &str) -> Result<()> {
        let topic_hash = utils::get_topic_hash(abi)?;
        self.topics.insert(topic_hash);
        Ok(())
    }

    pub async fn subscribe_address(&self, address: ethabi::Address) {
        self.addresses.insert(address);
    }

    pub async fn unsubscribe_address(&self, address: &ethabi::Address) {
        self.addresses.remove(address);
    }

    pub async fn unsubscribe_by_encoded_topic<K: AsRef<[u8]>>(&self, key: K) {
        self.topics.remove(key.as_ref());
    }

    pub async fn unsubscribe_by_abi(&self, abi: &str) -> Result<()> {
        let topic_hash = utils::get_topic_hash(abi)?;
        self.topics.remove(&topic_hash);
        Ok(())
    }

    async fn get_current_height(&self) -> Result<u64> {
        let res = *timeout(self.config.get_timeout, self.web3.eth().block_number())
            .await
            .context("Timeout getting height")??
            .0
            .get(0)
            .trust_me();
        Ok(res)
    }
}
