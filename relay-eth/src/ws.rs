use std::convert::TryInto;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Error;
use futures::stream::{Stream, StreamExt};
use log::{error, info, warn};
use serde::{Deserialize, Serialize};
use sled::{Db, Tree};
use tokio::spawn;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::sync::{Mutex, RwLock};
use url::Url;
use web3::transports::ws::WebSocket;
pub use web3::types::{Address, BlockNumber, H256};
use web3::types::{BlockHeader, FilterBuilder, Log, U256, U64};
use web3::Web3;

pub const ETH_HEIGHT_KEY: &str = "height";
const ETH_TREE_NAME: &str = "ethereum_data";
const ETH_LAST_MET_HEIGHT: &str = "last_met_height";

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
}

pub fn update_eth_state(db: &Tree, height: u64, key: &str) -> Result<(), Error> {
    db.insert(key, &height.to_le_bytes())?;
    Ok(())
}

async fn monitor_block_number(db: Tree, listener: Web3<WebSocket>, sender: UnboundedSender<u64>) {
    let mut sub = match listener.eth_subscribe().subscribe_new_heads().await {
        Ok(a) => a,
        Err(e) => {
            error!("Failed subscribing on eth logs: {}", e);
            return;
        }
    };

    while let Some(a) = sub.next().await {
        let head: BlockHeader = match a {
            Ok(a) => a,
            Err(e) => {
                log::error!("Got bad data from stream: {}", e);
                continue;
            }
        };

        match head.number {
            Some(a) => {
                log::info!("Received new head with number: {}", a);
                if let Err(e) = sender.send(a.as_u64()) {
                    log::error!("Failed sending current eth height: {}", e)
                }
                if let Err(e) = update_eth_state(&db, a.as_u64(), ETH_HEIGHT_KEY) {
                    log::error!("Failed saving state to db: {}", e);
                }
            }
            None => {
                log::error!("No block number in head");
            }
        }
    }
}

impl EthListener {
    pub async fn get_block_number(&self) -> Result<u64, Error> {
        Ok(match self.db.get(ETH_LAST_MET_HEIGHT)? {
            Some(a) => u64::from_le_bytes(a.as_ref().try_into()?),
            None => {
                let block = self.stream.eth().block_number().await?.as_u64();
                block
            }
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
    ) -> Result<impl Stream<Item = Result<Event, Error>>, Error> {
        let height = self.get_block_number().await?;
        let filter = FilterBuilder::default()
            .address(addresses)
            .topics(Some(topics), None, None, None)
            .from_block(BlockNumber::Number(height.into())) //fixme
            .build();

        let filter = self.stream.eth_filter().create_logs_filter(filter).await?;
        let mut stream = filter.stream(Duration::from_secs(1));
        spawn(monitor_block_number(self.db.clone(), self.stream.clone(), self.blocks_tx.clone()));
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let db = self.db.clone();
        spawn(async move {
            while let Some(a) = stream.next().await {
                match a {
                    Err(e) => {
                        if let Err(e) = tx.send(Err(e.into())) {
                            error!("Error while transmitting value via channel: {}", e);
                        }
                    }
                    Ok(a) => {
                        let event = EthListener::log_to_event(a, &db);
                        log::trace!("Received event: {:#?}", &event);
                        if let Err(e) = tx.send(event) {
                            error!("Error while transmitting value via channel: {}", e);
                        }
                    }
                }
            }
        });
        Ok(rx)
    }
}
