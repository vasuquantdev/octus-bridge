use std::convert::TryInto;
use std::sync::Arc;

use anyhow::Error;
use futures::stream::{Stream, StreamExt};
use log::{error, info};
use serde::{Deserialize, Serialize};
use sled::{Db, Tree};
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::sync::Mutex;
use url::Url;
use web3::transports::ws::WebSocket;
pub use web3::types::{Address, BlockNumber, H256};
use web3::types::{BlockHeader, FilterBuilder, Log};
use web3::Web3;

const ETH_TREE_NAME: &str = "ethereum_data";
pub const ETH_LAST_MET_HEIGHT: &str = "last_met_height";

#[derive(Clone)]
pub struct EthListener {
    stream: Web3<WebSocket>,
    db: Tree,
    blocks_tx: UnboundedSender<u64>,
    blocks_rx: Arc<Mutex<Option<UnboundedReceiver<u64>>>>,
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
    pub block_hash: Vec<u8>,
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

impl EthListener {
    pub async fn get_block_number(&self) -> Result<u64, Error> {
        Ok(match self.db.get(ETH_LAST_MET_HEIGHT)? {
            Some(a) => u64::from_le_bytes(a.as_ref().try_into()?),
            None => self.stream.eth().block_number().await?.as_u64(),
        })
    }

    pub async fn get_blocks_stream(&self) -> Option<UnboundedReceiver<u64>> {
        let mut guard = self.blocks_rx.lock().await;
        guard.take()
    }

    fn log_to_event(log: Log, db: &Tree) -> Result<Event, Error> {
        let data = log.data.0;
        let hash = match log.transaction_hash {
            Some(a) => a,
            None => {
                error!("No tx hash!");
                return Err(Error::msg("No tx hash in log"));
            }
        };
        let block_number = match log.block_number {
            Some(a) => {
                if let Err(e) = update_eth_state(db, a.as_u64(), ETH_LAST_MET_HEIGHT) {
                    let err = format!("Critical error: failed saving eth state: {}", e);
                    error!("{}", &err);
                    return Err(Error::msg(err));
                }
                a.as_u64()
            }
            None => {
                let err = "No block number in log!".to_string();
                error!("{}", &err);
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
                error!("{}", &err);
                // return Err(Error::msg(err));
                0
            }
        };
        let block_hash = match log.block_hash {
            Some(a) => Vec::from(a.0),
            None => {
                let err = format!("No hash in log. Tx hash: {}. Block: {}", hash, block_number);
                error!("{}", err);
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

    pub async fn new(url: Url, db: Db) -> Result<Self, Error> {
        let connection = WebSocket::new(url.as_str())
            .await
            .expect("Failed connecting to ethereum node");
        info!("Connected to: {}", &url);
        let api = Web3::new(connection);
        let (tx, rx) = unbounded_channel();
        Ok(Self {
            stream: api,
            db: db.open_tree(ETH_TREE_NAME)?,
            blocks_tx: tx,
            blocks_rx: Arc::new(Mutex::new(Option::Some(rx))),
        })
    }

    pub async fn subscribe(
        &self,
        addresses: Vec<Address>,
        topics: Vec<H256>,
    ) -> Result<impl Stream<Item=Result<Event, Error>>, Error> {
        let height = self.get_block_number().await?;
        log::info!(
            "Subscribing on addresses: {:?}, topics: {:?}",
            addresses,
            topics
        );
        // spawn(monitor_block_number(
        //     self.db.clone(),
        //     self.stream.clone(),
        //     self.blocks_tx.clone(),
        // ));
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

        let addresses = addresses.clone();
        let topics = topics.clone();
        Self::scan_blocks(
            addresses,
            topics,
            height,
            self.stream.clone(),
            tx,
            rx,
            self.db.clone(),
            self.blocks_tx.clone(),
        )
            .await
    }

    async fn scan_blocks(
        addresses: Vec<Address>,
        topics: Vec<H256>,
        mut from: u64,
        w3: Web3<WebSocket>,
        tx: UnboundedSender<Result<Event, Error>>,
        rx: UnboundedReceiver<Result<Event, Error>>,
        db: Tree,
        ticker: UnboundedSender<u64>,
    ) -> Result<impl Stream<Item=Result<Event, Error>>, Error> {
        tokio::spawn(async move {
            let mut current_height = match w3.eth().block_number().await {
                Ok(a) => a.as_u64(),
                Err(e) => {
                    log::error!("Failed getting block number: {}", e);
                    return;
                }
            };

            while from != current_height {
                get_event_from_block(
                    addresses.clone(),
                    topics.clone(),
                    &tx,
                    &db,
                    from.into(),
                    &w3,
                )
                    .await;
                if let Err(e) = ticker.send(from) {
                    log::error!("Failed sending event via channel: {}", e);
                }
                current_height = match w3.eth().block_number().await {
                    Ok(a) => a.as_u64(),
                    Err(e) => {
                        log::error!("Failed getting block number: {}", e);
                        continue;
                    }
                };
                from += 1;
            }
            log::info!("Synchronized with ehtereum");
            log::info!("Now subscribing instead of polling");
            let mut blocks_stream = match w3.eth_subscribe().subscribe_new_heads().await {
                Ok(a) => a,
                Err(e) => {
                    error!("Failed subscribing on eth logs: {}", e);
                    return;
                }
            };

            while let Some(block) = blocks_stream.next().await {
                let head: BlockHeader = match block {
                    Ok(a) => a,
                    Err(e) => {
                        log::error!("Got bad data from stream: {}", e);
                        continue;
                    }
                };
                let block_number = match head.number {
                    Some(a) => a.as_u64(),
                    None => {
                        log::error!("Block is pending");
                        continue;
                    }
                };
                if let Err(e) = ticker.send(block_number) {
                    log::error!("Failed sending event via channel: {}", e);
                }
                get_event_from_block(
                    addresses.clone(),
                    topics.clone(),
                    &tx,
                    &db,
                    block_number.into(),
                    &w3,
                )
                    .await;
            }
        });
        Ok(rx)
    }
}

async fn get_event_from_block(
    addresses: Vec<Address>,
    topics: Vec<H256>,
    tx: &UnboundedSender<Result<Event, Error>>,
    db: &Tree,
    block_number: BlockNumber,
    w3: &Web3<WebSocket>,
) {
    let filter = FilterBuilder::default()
        .address(addresses)
        .topics(Some(topics), None, None, None)
        .from_block(block_number) //fixme
        .to_block(block_number)
        .build();
    match w3.eth().logs(filter).await {
        Ok(a) => {
            if !a.is_empty() {
                log::info!("There some logs in block: {:?}", block_number);
            }
            for log in a {
                let event = EthListener::log_to_event(log, &db);
                match event {
                    Ok(a) => {
                        if let Err(e) = tx.send(Ok(a)) {
                            log::error!("Failed sending event: {}", e);
                        }
                    }
                    Err(e) => {
                        log::error!("Failed parsing log to event: {}", e);
                        if let Err(e) = tx.send(Err(e)) {
                            log::error!("Failed sending event: {}", e);
                        }
                        continue;
                    }
                }
            }
        }
        Err(e) => {
            log::error!("Failed fetching logs: {}", e);
        }
    };
}
// while let Some(a) = stream.next().await {
// match a {
// Err(e) => {
// if let Err(e) = tx.send(Err(e.into())) {
// error!("Error while transmitting value via channel: {}", e);
// }
// }
// Ok(a) => {
// let event = EthListener::log_to_event(a, &db);
// log::trace!("Received event: {:#?}", &event);
// if let Err(e) = tx.send(event) {
// error!("Error while transmitting value via channel: {}", e);
// }
// }
// }
// }
