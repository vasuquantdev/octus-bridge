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
use self::utils::{generate_default_timeout_config, generate_fixed_config};
use crate::filter_log;
use crate::state::db::Pool;
use crate::state::eth_state::EthState;
use crate::state::models::StoredEthEvent;
use crate::utils::retry;

mod eth_config;
pub mod models;
mod utils;

pub struct EthSubscriberRegistry {
    subscribed: DashMap<u8, Arc<EthSubscriber>>,
}

impl EthSubscriberRegistry {
    pub async fn new_subscriber(
        &self,
        config: EthConfig,
        chain_id: u8,
        db: Arc<Pool>,
    ) -> Result<Arc<EthSubscriber>> {
        match self.subscribed.get(&chain_id) {
            Some(a) => a.clone(),
            None => Arc::new(EthSubscriber::new(config, chain_id, db).await?),
        }
        .pipe(Ok)
    }

    pub fn get_subscriber(&self, chain_id: u8) -> Option<Arc<EthSubscriber>> {
        // Not cloning will deadlock
        self.subscribed.get(&chain_id).map(|x| x.clone())
    }
}

#[derive(Clone)]
pub struct EthSubscriber {
    config: EthConfig,
    chain_id: u8,
    web3: Web3<Http>,
    pool: Arc<Semaphore>,
    topics: DashSet<[u8; 32]>,
    addresses: DashSet<ethabi::Address>,
    current_height: Arc<AtomicU64>,
    state: EthState,
}

impl EthSubscriber {
    async fn new(config: EthConfig, id: u8, db: Arc<Pool>) -> Result<Self> {
        let transport = web3::transports::Http::new(config.endpoint.as_str())?;
        let web3 = web3::Web3::new(transport);
        let pool = Arc::new(Semaphore::new(config.pool_size));
        let state = EthState::new(db);
        let current_height = state.get_last_block_id(id).await?;
        Ok(Self {
            config,
            chain_id: id,
            web3,
            pool,
            topics: Default::default(),
            addresses: Default::default(),
            current_height: Arc::new(current_height.into()),
            state,
        })
    }

    pub fn run(self: &Arc<Self>) -> impl Stream<Item = Uuid> {
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
                    .state
                    .get_last_block_id(this.chain_id)
                    .await
                    .expect("DB error");
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
                this.state
                    .block_processed(ethereum_actual_height, this.chain_id)
                    .await
                    .expect("DB ERROR");
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
            anyhow::anyhow!("Addresses and topics are empty. Cowardly refusing to process all ethereum transactions");
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
        let uuids: Vec<_> = futures::stream::iter(logs.into_iter())
            .then(|x| self.state.new_event(x))
            .collect()
            .await;
        uuids.into_iter().collect()
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
