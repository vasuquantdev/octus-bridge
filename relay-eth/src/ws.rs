use std::collections::{HashMap, HashSet};
use std::convert::TryInto;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Error};
use futures::stream::Stream;
use serde::{Deserialize, Serialize};
use sha3::{Digest, Keccak256};
use sled::{Db, Tree};
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};
use tokio::sync::RwLock;
use tokio::sync::Semaphore;
use url::Url;
use web3::transports::http::Http;
pub use web3::types::SyncState;
pub use web3::types::{Address, BlockNumber, H256};
use web3::types::{FilterBuilder, Log, H160};
use web3::{Transport, Web3};

const ETH_POLL_INTERVAL: Duration = Duration::from_secs(5);
const ETH_TIMEOUT: Duration = Duration::from_secs(2);

const ETH_TREE_NAME: &str = "ethereum_data";
pub const ETH_LAST_MET_HEIGHT: &str = "last_met_height";

pub struct EthListener {
    web3: Web3<Http>,
    db: Tree,
    topics: Arc<RwLock<(HashSet<Address>, HashSet<H256>)>>,
    current_block: Arc<AtomicU64>,
    connections_pool: Arc<Semaphore>,
    relay_keys_function_to_topic_map: HashMap<String, H256>,
}

async fn get_actual_eth_height<T: Transport>(
    w3: &Web3<T>,
    connection_pool: &Arc<Semaphore>,
) -> Option<u64> {
    use tokio::time::timeout;
    log::debug!("Getting height");
    let _permission = connection_pool.acquire().await;
    match timeout(ETH_TIMEOUT, w3.eth().block_number()).await {
        Ok(a) => match a {
            Ok(a) => {
                let height = a.as_u64();
                log::trace!("Got height: {}", height);
                Some(height)
            }
            Err(e) => {
                if let web3::error::Error::Transport(e) = &e {
                    if e == "hyper::Error(IncompleteMessage)" {
                        log::debug!("Failed getting height: {}", e);
                        return None;
                    }
                }
                log::error!("Failed getting block number: {:?}", e);
                None
            }
        },
        Err(e) => {
            log::error!("Timed out on getting actual eth block number: {:?}", e);
            None
        }
    }
}

/// Returns topic hash and abi for ETH and TON
pub fn parse_eth_abi(abi: &str) -> Result<HashMap<String, H256>, Error> {
    #[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Abi {
        pub inputs: Vec<Input>,
        pub name: String,
    }

    #[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Input {
        #[serde(rename = "type")]
        pub type_field: String,
    }

    let abis: Vec<Abi> = serde_json::from_str(abi)?;
    let mut topics = HashMap::with_capacity(abis.len());
    for abi in abis {
        let fn_name = abi.name;

        let input_types: String = abi
            .inputs
            .iter()
            .map(|x| x.type_field.clone())
            .collect::<Vec<String>>()
            .join(",");

        let signature = format!("{}({})", fn_name, input_types);
        topics.insert(
            fn_name,
            H256::from_slice(&*Keccak256::digest(signature.as_bytes())),
        );
    }

    Ok(topics)
}

#[derive(Debug)]
pub enum SyncedHeight {
    Synced(u64),
    NotSynced(u64),
}

impl SyncedHeight {
    pub fn as_u64(&self) -> u64 {
        match *self {
            SyncedHeight::Synced(a) => a,
            SyncedHeight::NotSynced(a) => a,
        }
    }
}

impl EthListener {
    pub async fn new(url: Url, db: Db, connections_number: usize) -> Result<Self, Error> {
        let connection = Http::new(url.as_str()).expect("Failed connecting to ethereum node");
        log::info!("Connected to: {}", &url);
        let tree = db.open_tree(ETH_TREE_NAME)?;
        let api = Web3::new(connection);
        let current_block = Self::get_block_number_on_start(&tree, &api).await?;
        let relay_keys_abi = parse_eth_abi(include_str!(
            "../abi/contracts_DistributedOwnable_sol_DistributedOwnable.json"
        ))?;
        let listener = Self {
            web3: api,
            db: tree,
            topics: Arc::new(Default::default()),
            connections_pool: Arc::new(Semaphore::new(connections_number)),
            current_block: Arc::new(AtomicU64::new(current_block)),
            relay_keys_function_to_topic_map: relay_keys_abi,
        };
        listener.get_actual_keys().await?; //todo use it
        Ok(listener)
    }

    pub fn change_eth_height(&self, height: u64) -> Result<(), Error> {
        self.current_block.store(height, Ordering::SeqCst);
        update_height(&self.db, height)?;
        Ok(())
    }

    pub async fn start(
        self: &Arc<Self>,
    ) -> Result<impl Stream<Item = Result<Event, Error>>, Error> {
        log::debug!("Started iterating over ethereum blocks.");
        let from_height = self.current_block.clone();

        let events_rx = spawn_blocks_scanner(
            self.db.clone(),
            self.web3.clone(),
            self.topics.clone(),
            from_height,
            self.connections_pool.clone(),
        );

        Ok(events_rx)
    }

    pub async fn get_block_number_on_start(db: &Tree, web3: &Web3<Http>) -> Result<u64, Error> {
        Ok(match db.get(ETH_LAST_MET_HEIGHT)? {
            Some(a) => u64::from_le_bytes(a.as_ref().try_into()?),
            None => web3.eth().block_number().await?.as_u64(),
        })
    }

    pub async fn check_transaction(&self, hash: H256) -> Result<(Address, Vec<u8>), Error> {
        let mut attempts = 100;
        loop {
            // Trying to get data. Retrying in case of error
            let _permission = self.connections_pool.acquire().await;
            match self.web3.eth().transaction_receipt(hash).await {
                Ok(a) => {
                    return match a {
                        //if no tx with this hash
                        None => Err(anyhow!("No transactions found by hash. Assuming it's fake")),
                        Some(a) => {
                            // if tx status is failed, then no such tx exists
                            match a.status {
                                Some(a) => {
                                    if a.as_u64() == 0 {
                                        return Err(anyhow!("Tx has failed status"));
                                    }
                                }
                                None => return Err(anyhow!("No status field in eth node answer")),
                            };

                            let logs = a.logs;
                            //parsing logs into events
                            let events: Result<Vec<_>, _> =
                                logs.into_iter().map(EthListener::log_to_event).collect();

                            let events = match events {
                                Ok(a) => a,
                                Err(e) => {
                                    log::error!(
                                        "No events for tx. Assuming confirmation is fake.: {}",
                                        e
                                    );
                                    return Err(anyhow!(
                                        "No events for tx. Assuming confirmation is fake.: {}",
                                        e
                                    ));
                                }
                            };

                            // if any event matches
                            let event: Option<_> = events
                                .into_iter()
                                .find(|x| x.tx_hash == hash /* && x.data == data */);
                            match event {
                                Some(a) => Ok((a.address, a.data)),
                                None => Err(anyhow!(
                                    "No events for tx. Assuming confirmation is fake.: {}"
                                )),
                            }
                        }
                    };
                }
                Err(e) => {
                    if attempts == 0 {
                        return Err(Error::from(e));
                    }
                    attempts -= 0;
                    log::error!(
                        "Failed fetching info from eth node. Attempts left: {}",
                        attempts
                    );
                    //todo move to config?
                    tokio::time::delay_for(Duration::from_secs(5)).await;
                }
            }
        }
    }

    async fn get_actual_keys(&self) -> Result<HashMap<Address, Vec<H160>>, Error> {
        let addresses = (*self.topics.read().await).0.clone();
        async fn get_keys(
            topic: H256,
            address: Address,
            web3: &Web3<Http>,
        ) -> Result<HashSet<H160>, Error> {
            let filter = FilterBuilder::default()
                .address(vec![address])
                .topics(Some(vec![topic]), None, None, None)
                .from_block(BlockNumber::Earliest)
                .to_block(BlockNumber::Latest)
                .build();

            Ok(web3
                .eth()
                .logs(filter)
                .await?
                .into_iter()
                .map(EthListener::log_to_event)
                .filter_map(|x| match x {
                    Ok(a) if a.data.len() == 20 => Some(H160::from_slice(&*a.data)),
                    Err(e) => {
                        log::error!("Failed parsing log as event: {}", e);
                        None
                    }
                    Ok(a) => {
                        log::error!("Bad address len: {}", a.data.len());
                        None
                    }
                })
                .collect())
        }
        let ok_topic = self.relay_keys_function_to_topic_map["OwnershipGranted"];
        let bad_topic = self.relay_keys_function_to_topic_map["OwnershipRemoved"];
        let mut keys_map = HashMap::with_capacity(addresses.len());
        for address in addresses {
            let ok_fut = get_keys(ok_topic, address, &self.web3);
            let bad_fut = get_keys(bad_topic, address, &self.web3);
            let (ok, bad): (HashSet<_>, HashSet<_>) = futures::try_join!(ok_fut, bad_fut)?;
            keys_map.insert(address, ok.difference(&bad).cloned().collect());
        }
        Ok(keys_map)
    }

    fn log_to_event(log: Log) -> Result<Event, Error> {
        let data = log.data.0;
        let hash = match log.transaction_hash {
            Some(a) => a,
            None => {
                log::error!("No tx hash!");
                return Err(Error::msg("No tx hash in log"));
            }
        };
        let block_number = match log.block_number {
            Some(a) => a.as_u64(),
            None => {
                let err = "No block number in log!".to_string();
                log::error!("{}", &err);
                return Err(Error::msg(err));
            }
        };
        let event_index = match log.transaction_log_index {
            Some(a) => a.as_u64(),
            None => {
                let err = format!(
                    "No transaction_log_index in log. Tx hash: {}. Block: {}",
                    hash, block_number
                );
                log::warn!("{}", &err);
                0
            }
        };
        let block_hash = match log.block_hash {
            Some(a) => a,
            None => {
                let err = format!("No hash in log. Tx hash: {}. Block: {}", hash, block_number);
                log::error!("{}", err);
                return Err(Error::msg(err));
            }
        };

        log::debug!("Sent logs from block {} with hash {}", block_number, hash);
        Ok(Event {
            address: log.address,
            data,
            tx_hash: hash,
            topics: log.topics,
            event_index,
            block_number,
            block_hash,
        })
    }

    ///subscribe on address and topic
    pub async fn add_topic(&self, address: Address, topic: H256) {
        log::info!(
            "Subscribing for address: {:?} with topic: {:?}",
            address,
            topic
        );

        let mut topics = self.topics.write().await;
        topics.0.insert(address);
        topics.1.insert(topic);
    }

    ///unsubscribe from address
    pub async fn unsubscribe_from_address(&self, address: &Address) {
        let mut topics = self.topics.write().await;
        topics.0.remove(address);
    }

    ///unsubscribe from 1 topic
    pub async fn unsubscribe_from_topic(&self, topic: &H256) {
        let mut topics = self.topics.write().await;
        topics.1.remove(topic);
    }

    ///unsubscribe from list of topics
    pub async fn unsubscribe_from_topics(&self, topics_list: &[H256]) {
        let mut topics = self.topics.write().await;
        topics_list.iter().for_each(|t| {
            topics.1.remove(t);
        });
    }

    pub async fn get_synced_height(&self) -> Result<SyncedHeight, Error> {
        let mut counter = 100;
        loop {
            match self.web3.eth().syncing().await {
                Ok(sync_state) => match sync_state {
                    SyncState::Syncing(a) => {
                        let current_synced_block = a.current_block.as_u64();
                        let network_height = a.highest_block.as_u64();
                        if network_height - current_synced_block > 200 {
                            log::warn!(
                                "Ethereum node is far behind network head: {} blocks to sync",
                                network_height - current_synced_block
                            );
                        }
                        return Ok(SyncedHeight::NotSynced(current_synced_block));
                    }
                    SyncState::NotSyncing => loop {
                        match get_actual_eth_height(&self.web3, &self.connections_pool).await {
                            Some(a) => return Ok(SyncedHeight::Synced(a)),
                            None => {
                                tokio::time::delay_for(Duration::from_secs(5)).await;
                                continue;
                            }
                        }
                    },
                },
                Err(e) => {
                    log::error!("Failed fetching eth sync status: {}", e);
                    if counter == 0 {
                        return Err(Error::new(e).context("Failed getting eth syncing status"));
                    }
                    counter -= 1;
                    tokio::time::delay_for(Duration::from_secs(5)).await;
                }
            }
        }
    }
}

fn spawn_blocks_scanner(
    db: Tree,
    w3: Web3<Http>,
    topics: Arc<RwLock<(HashSet<Address>, HashSet<H256>)>>,
    from_height: Arc<AtomicU64>,
    connections_pool: Arc<Semaphore>,
) -> impl Stream<Item = Result<Event, Error>> {
    let (events_tx, events_rx) = unbounded_channel();

    tokio::spawn(async move {
        //
        //
        let w3 = w3.clone();
        let connection_pool = connections_pool.clone();
        let scanned_height = from_height;
        loop {
            // trying to get actual height
            let ethereum_actual_height = loop {
                match get_actual_eth_height(&w3, &connection_pool).await {
                    Some(a) => break a,
                    None => {
                        tokio::time::delay_for(ETH_POLL_INTERVAL).await;
                        log::debug!("Failed getting start height. Polling");
                        continue;
                    }
                };
            };

            let mut loaded_height = scanned_height.load(Ordering::SeqCst);
            // polling if we are near head and sleeping
            if loaded_height > ethereum_actual_height
                || (ethereum_actual_height - loaded_height) <= 2
            {
                let block_number = BlockNumber::from(loaded_height);
                process_block(
                    &w3,
                    topics.as_ref(),
                    block_number,
                    block_number,
                    &events_tx,
                    &connection_pool,
                )
                .await;
                tokio::time::delay_for(ETH_POLL_INTERVAL).await;
            }
            // batch processing all blocks from `loaded_height` to `ethereum_actual_height`
            else {
                log::debug!(
                    "Batch processing blocks from {} to {}",
                    loaded_height,
                    ethereum_actual_height
                );
                let block_number = BlockNumber::from(loaded_height);
                process_block(
                    &w3,
                    topics.as_ref(),
                    block_number,
                    BlockNumber::from(ethereum_actual_height),
                    &events_tx,
                    &connection_pool,
                )
                .await;
                loaded_height = ethereum_actual_height - 1;
                scanned_height.store(loaded_height, Ordering::SeqCst);
            }

            if let Err(e) = update_eth_state(&db, loaded_height, ETH_LAST_MET_HEIGHT) {
                let err = format!("Critical error: failed saving eth state: {}", e);
                log::error!("{}", &err);
            };

            scanned_height.fetch_add(1, Ordering::SeqCst);
        }
    });

    events_rx
}

async fn process_block(
    w3: &Web3<Http>,
    topics: &RwLock<(HashSet<Address>, HashSet<H256>)>,
    from: BlockNumber,
    to: BlockNumber,
    events_tx: &UnboundedSender<Result<Event, Error>>,
    connection_pool: &Arc<Semaphore>,
) {
    // TODO: optimize
    let (addresses, topics): (Vec<_>, Vec<_>) = {
        let state = topics.read().await;
        (
            state.0.iter().cloned().collect(),
            state.1.iter().cloned().collect(),
        )
    };

    let filter = FilterBuilder::default()
        .address(addresses)
        .topics(Some(topics), None, None, None)
        .from_block(from) //fixme
        .to_block(to)
        .build();

    let mut attempts_number = 86400 / 5;
    let sleep_time = Duration::from_secs(5);
    loop {
        let _permit = connection_pool.acquire().await;
        match w3.eth().logs(filter.clone()).await {
            Ok(a) => {
                if !a.is_empty() {
                    log::info!("There are some logs in block: {:?}", from);
                }
                for log in a {
                    let event = EthListener::log_to_event(log);
                    match event {
                        Ok(a) => {
                            if let Err(e) = events_tx.send(Ok(a)) {
                                log::error!("FATAL ERROR. Failed sending event: {:?}", e);
                            }
                            continue;
                        }
                        Err(e) => {
                            log::error!("Failed parsing log to event: {:?}", e);
                            if let Err(e) = events_tx.send(Err(e)) {
                                log::error!("Failed sending event: {:?}", e);
                            }
                            continue;
                        }
                    }
                }
                return;
            }
            Err(e) => {
                attempts_number -= 1;
                if attempts_number == 0 {
                    return;
                }
                log::error!("Critical error in eth subscriber: {}", e);
                log::error!(
                    "Retrying to get block: {:?}. Attempts left: {}",
                    from,
                    attempts_number
                );
                tokio::time::delay_for(sleep_time).await;
            }
        };
    }
}

///topics: `Keccak256("Method_Signature")`
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Serialize, Deserialize, Ord)]
pub struct Event {
    pub address: Address,
    pub data: Vec<u8>,
    pub tx_hash: H256,
    pub topics: Vec<H256>,
    pub event_index: u64,
    pub block_number: u64,
    pub block_hash: H256,
}

fn update_eth_state(db: &Tree, height: u64, key: &str) -> Result<(), Error> {
    db.insert(key, &height.to_le_bytes())?;
    Ok(())
}

pub fn update_height(db: &Tree, height: u64) -> Result<(), Error> {
    update_eth_state(&db, height, ETH_LAST_MET_HEIGHT)?;
    Ok(())
}
