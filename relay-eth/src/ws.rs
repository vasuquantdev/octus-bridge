use std::collections::HashSet;
use std::convert::TryInto;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Error};
use futures::stream::{Stream, StreamExt};
use serde::{Deserialize, Serialize};
use sled::{Db, Tree};
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};
use tokio::sync::RwLock;
use url::Url;
use web3::transports::http::Http;
pub use web3::types::{Address, BlockNumber, H256};
use web3::types::{FilterBuilder, Log};
use web3::{Transport, Web3};

const ETH_POLL_INTERVAL: Duration = Duration::from_secs(5);
const ETH_TIMEOUT: Duration = Duration::from_secs(2);

const ETH_TREE_NAME: &str = "ethereum_data";
pub const ETH_LAST_MET_HEIGHT: &str = "last_met_height";

pub struct EthListener {
    web3: Web3<Http>,
    db: Tree,
    topics: Arc<RwLock<(HashSet<Address>, HashSet<H256>)>>,
}

async fn get_actual_eth_height<T: Transport>(w3: &Web3<T>) -> Option<u64> {
    use tokio::time::timeout;
    log::debug!("Getting height");
    match timeout(ETH_TIMEOUT, w3.eth().block_number()).await {
        Ok(a) => match a {
            Ok(a) => Some(a.as_u64()),
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

impl EthListener {
    pub async fn new(url: Url, db: Db) -> Result<Self, Error> {
        let connection = Http::new(url.as_str()).expect("Failed connecting to ethereum node");
        log::info!("Connected to: {}", &url);
        let api = Web3::new(connection);

        Ok(Self {
            web3: api,
            db: db.open_tree(ETH_TREE_NAME)?,
            topics: Arc::new(Default::default()),
        })
    }

    pub async fn start<S>(
        self: &Arc<Self>,
        subscriptions: S,
    ) -> Result<
        (
            impl Stream<Item = u64>,
            impl Stream<Item = Result<Event, Error>>,
        ),
        Error,
    >
    where
        S: Stream<Item = (Address, H256)> + Send + Unpin + 'static,
    {
        let from_height = self.get_block_number_on_start().await?;

        tokio::spawn({
            let mut subscriptions = subscriptions;
            let ws = Arc::downgrade(&self);
            async move {
                while let Some((address, topic)) = subscriptions.next().await {
                    let ws = match ws.upgrade() {
                        Some(ws) => ws,
                        None => return,
                    };

                    log::info!(
                        "Subscribing for address: {:?} with topic: {:?}",
                        address,
                        topic
                    );

                    ws.add_topic(address, topic).await
                }
            }
        });

        let (blocks_rx, events_rx) = spawn_blocks_scanner(
            self.db.clone(),
            self.web3.clone(),
            self.topics.clone(),
            from_height,
        );

        Ok((blocks_rx, events_rx))
    }

    pub async fn get_block_number_on_start(&self) -> Result<u64, Error> {
        Ok(match self.db.get(ETH_LAST_MET_HEIGHT)? {
            Some(a) => u64::from_le_bytes(a.as_ref().try_into()?),
            None => self.web3.eth().block_number().await?.as_u64(),
        })
    }

    // TODO
    pub async fn check_transaction(&self, hash: H256) -> Result<(Address, Vec<u8>), Error> {
        let mut attempts = 100;
        loop {
            /// Trying to get data in case of transport error
            match self.web3.eth().transaction_receipt(hash).await {
                Ok(a) => match a {
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
                },
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
                // TODO: return Err(Error::msg(err));
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

    pub async fn add_topic(&self, address: Address, topic: H256) {
        let mut topics = self.topics.write().await;
        topics.0.insert(address);
        topics.1.insert(topic);
    }
}

fn spawn_blocks_scanner(
    db: Tree,
    w3: Web3<Http>,
    topics: Arc<RwLock<(HashSet<Address>, HashSet<H256>)>>,
    from_height: u64,
) -> (
    impl Stream<Item = u64>,
    impl Stream<Item = Result<Event, Error>>,
) {
    let (blocks_tx, blocks_rx) = unbounded_channel();
    let (events_tx, events_rx) = unbounded_channel();

    tokio::spawn(async move {
        //
        //
        let w3 = w3.clone();
        let mut ethereum_actual_height = loop {
            match get_actual_eth_height(&w3).await {
                Some(a) => break a,
                None => {
                    tokio::time::delay_for(ETH_POLL_INTERVAL).await;
                    log::error!("Failed getting start height. Polling");
                    continue;
                }
            };
        };
        let mut scanned_height = from_height;
        loop {
            //sleeping if we are on head
            if ethereum_actual_height <= scanned_height {
                tokio::time::delay_for(ETH_POLL_INTERVAL).await;
                ethereum_actual_height = match get_actual_eth_height(&w3).await {
                    Some(a) => a,
                    None => continue,
                };
                if ethereum_actual_height <= scanned_height {
                    continue;
                }
            // polling if we are near head
            } else if (ethereum_actual_height - scanned_height) < 2 {
                ethereum_actual_height = match get_actual_eth_height(&w3).await {
                    Some(a) => a,
                    None => continue,
                };
            }
            process_block(&w3, topics.as_ref(), scanned_height.into(), &events_tx).await;

            if let Err(e) = update_eth_state(&db, scanned_height, ETH_LAST_MET_HEIGHT) {
                let err = format!("Critical error: failed saving eth state: {}", e);
                log::error!("{}", &err);
            };

            if let Err(e) = blocks_tx.send(scanned_height) {
                log::error!("Failed sending event via channel: {:?}", e);
                // Panic?
            }
            scanned_height += 1;
        }
    });

    (blocks_rx, events_rx)
}

async fn process_block(
    w3: &Web3<Http>,
    topics: &RwLock<(HashSet<Address>, HashSet<H256>)>,
    block_number: BlockNumber,
    events_tx: &UnboundedSender<Result<Event, Error>>,
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
        .from_block(block_number) //fixme
        .to_block(block_number)
        .build();
    let mut attempts_number = 100;
    let sleep_time = Duration::from_secs(5);
    loop {
        match w3.eth().logs(filter.clone()).await {
            Ok(a) => {
                if !a.is_empty() {
                    log::info!("There are some logs in block: {:?}", block_number);
                }
                for log in a {
                    let event = EthListener::log_to_event(log);
                    match event {
                        Ok(a) => {
                            if let Err(e) = events_tx.send(Ok(a)) {
                                log::error!("Failed sending event: {:?}", e);
                            }
                            break;
                        }
                        Err(e) => {
                            log::error!("Failed parsing log to event: {:?}", e);
                            if let Err(e) = events_tx.send(Err(e)) {
                                log::error!("Failed sending event: {:?}", e);
                            }
                            break;
                        }
                    }
                }
            }
            Err(e) => {
                attempts_number -= 1;
                if attempts_number == 0 {
                    break;
                }
                log::error!("Critical error in eth subscriber: {}", e);
                log::error!(
                    "Retrying to get block: {}. Attempts left: {}",
                    attempts_number
                );
                tokio::time::delay_for(sleep_time).await;
                //FIXME will block be skipped in case of interrupted connection?
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

pub fn update_height(db: &Db, height: u64) -> Result<(), Error> {
    let tree = db.open_tree(ETH_TREE_NAME)?;
    update_eth_state(&tree, height, ETH_LAST_MET_HEIGHT)?;
    Ok(())
}
