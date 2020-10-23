use anyhow::Error;
use log::info;
use sha3::{Digest, Keccak256};
use tokio::time::Duration;
use url::Url;
use web3::api::{BaseFilter, FilterStream};
use web3::transports::ws::WebSocket;
use web3::types::{ FilterBuilder, Log, H256, Address};
use web3::Web3;
use std::str::FromStr;

pub struct EthConfig {
    stream: Web3<WebSocket>,
}

pub trait Method{
    fn get_function_hash(&self) -> Vec<u8>;
}


impl EthConfig {
    pub async fn new(infura_url: Url) -> Self {
        let connection = WebSocket::new(infura_url.as_str())
            .await
            .expect("Failed connecting to infura");
        info!("Connected to: {}", &infura_url);

        Self {
            stream: Web3::new(connection),
        }
    }
    pub async fn subscribe(&self, address: Address, method: &dyn Method) -> Result<FilterStream<WebSocket, Log>, Error> {
        //todo multisign
        // let mut hasher = Keccak256::new();
        // hasher.update(b"Transfer(address,address,uint256)");
        // let topic_hash: H256 = H256::from_slice(hasher.finalize().as_slice());
       let topic_hash: H256 = H256::from_slice(method.get_function_hash().as_slice());
        let filter = FilterBuilder::default()
            .address(vec![address])
            .topics(Some(vec![topic_hash]), None, None, None)
            .build();
        let filter = self.stream.eth_filter().create_logs_filter(filter).await;
        filter
            .and_then(|x| Ok(x.stream(Duration::from_secs(1))))
            .map_err(|e| e.into())
    }
}
