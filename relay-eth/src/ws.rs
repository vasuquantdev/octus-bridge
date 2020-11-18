use anyhow::Error;
use futures::stream::{Stream, StreamExt};
use log::{info,error};
use num256::Uint256;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio::spawn;
use url::Url;
use web3::transports::ws::WebSocket;
pub use web3::types::{Address, H256, BlockNumber};
use web3::types::{FilterBuilder, Log};
use web3::Web3;

#[derive(Clone)]
pub struct EthListener {
    stream: Web3<WebSocket>,
}

///topics: `Keccak256("Method_Signature")`
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Serialize, Deserialize, Ord)]
pub struct Event {
    pub address: Address,
    pub amount: Uint256,
    pub tx_hash: H256,
    pub topics: Vec<H256>,
}

impl EthListener {
    fn log_to_event(log: Log) -> Result<Event, Error> {
        let num = Uint256::from_bytes_be(&log.data.0);
        let hash = match log.transaction_hash {
            Some(a) => a,
            None => {
                error!("No tx hash!");
                return Err(Error::msg("No tx hash in log"));
            }
        };
        Ok(Event {
            address: log.address,
            amount: num,
            tx_hash: hash,
            topics: log.topics,
        })
    }
    pub async fn new(url: Url) -> Self {
        let connection = WebSocket::new(url.as_str())
            .await
            .expect("Failed connecting to etherium node");
        info!("Connected to: {}", &url);
        Self {
            stream: Web3::new(connection),
        }
    }

    pub async fn subscribe(
        &self,
        addresses: Vec<Address>,
        topics: Vec<H256>,
        height:BlockNumber
    ) -> Result<impl Stream<Item = Result<Event, Error>>, Error> {
        let filter = FilterBuilder::default()
            .address(addresses)
            .topics(Some(topics), None, None, None)
            .from_block(height)
            .build();

        let filter = self.stream.eth_filter().create_logs_filter(filter).await;
        let mut stream = filter?.stream(Duration::from_secs(1));
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        spawn(async move {
            while let Some(a) = stream.next().await {
                match a {
                    Err(e) => {
                        if let Err(e) = tx.send(Err(e.into())) {
                            error!("Error while transmitting value via channel: {}", e);
                        }
                    }
                    Ok(a) => {
                        let event = EthListener::log_to_event(a);
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
