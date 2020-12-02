use std::convert::TryInto;
use std::time::Duration;

use anyhow::Error;
use futures::stream::{Stream, StreamExt};
use log::{error, info, warn};
use num256::Uint256;
use serde::{Deserialize, Serialize};
use sled::Db;
use tokio::spawn;
use url::Url;
use web3::transports::ws::WebSocket;
pub use web3::types::{Address, BlockNumber, H256};
use web3::types::{FilterBuilder, Log, U64};
use web3::Web3;

pub const ETH_PERSISTENT_KEY_NAME: &str = "ethereum_height";

#[derive(Clone)]
pub struct EthListener {
    stream: Web3<WebSocket>,
    db: Db,
}

///topics: `Keccak256("Method_Signature")`
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Serialize, Deserialize, Ord)]
pub struct Event {
    pub address: Address,
    pub data: Vec<u8>,
    pub tx_hash: H256,
    pub topics: Vec<H256>,
}

pub fn update_eth_state(db: &Db, height: u64) -> Result<(), Error> {
    db.insert(ETH_PERSISTENT_KEY_NAME, &height.to_le_bytes())?;
    Ok(())
}

impl EthListener {
    fn log_to_event(log: Log, db: &Db) -> Result<Event, Error> {
        let data = log.data.0;
        let hash = match log.transaction_hash {
            Some(a) => a,
            None => {
                error!("No tx hash!");
                return Err(Error::msg("No tx hash in log"));
            }
        };
        match log.block_number {
            Some(a) => {
                if let Err(e) = update_eth_state(db, a.as_u64()) {
                    error!("Failed saving eth state: {}", e)
                }
            }
            None => {
                error!("No block number in log!");
            }
        };

        Ok(Event {
            address: log.address,
            data,
            tx_hash: hash,
            topics: log.topics,
        })
    }

    pub async fn new(url: Url, db: Db) -> Self {
        let connection = WebSocket::new(url.as_str())
            .await
            .expect("Failed connecting to ethereum node");
        info!("Connected to: {}", &url);
        Self {
            stream: Web3::new(connection),
            db,
        }
    }

    pub async fn subscribe(
        &self,
        addresses: Vec<Address>,
        topics: Vec<H256>,
    ) -> Result<impl Stream<Item = Result<Event, Error>>, Error> {
        let height = match self.db.get(ETH_PERSISTENT_KEY_NAME)? {
            None => {
                warn!("No data in persistent storage. Setting last block as height");
                BlockNumber::Latest
            }
            Some(a) => {
                let num = u64::from_le_bytes(a.as_ref().try_into()?);
                BlockNumber::Number(U64([num]))
            }
        };
        let filter = FilterBuilder::default()
            .address(addresses)
            .topics(Some(topics), None, None, None)
            .from_block(height)
            .build();

        let filter = self.stream.eth_filter().create_logs_filter(filter).await?;
        let mut stream = filter.stream(Duration::from_secs(1));
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
