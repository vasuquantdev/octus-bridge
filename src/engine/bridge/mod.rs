use anyhow::Error;
use futures::StreamExt;
use log::info;
use tokio::time::Duration;

use relay_eth::ws::{Address, EthListener};
use relay_ton::contracts::*;

use crate::crypto::key_managment::EthSigner;
use crate::engine::bridge::util::abi_to_topic_hash;

mod util;

pub struct Bridge {
    eth_signer: EthSigner,
    ton_client: BridgeContract,
    eth_client: EthListener,
}

impl Bridge {
    pub fn new(eth_signer: EthSigner, eth_client: EthListener, ton_client: BridgeContract) -> Self {
        Self {
            eth_signer,
            ton_client,
            eth_client,
        }
    }

    async fn eth_side(
        eth_client: EthListener,
        config: Vec<EthereumEventsConfiguration>,
        _ton_client: BridgeContract,
    ) -> Result<(), Error> {
        let mut eth_addr = Vec::new();
        let mut eth_topic = Vec::new();
        for conf in config.into_iter() {
            eth_addr.push(Address::from_slice(conf.ethereum_address.as_slice()));
            eth_topic.push(abi_to_topic_hash(&conf.ethereum_event_abi)?); //todo Vec of hashes
        }
        let eth_topic = eth_topic.into_iter().flatten().collect();
        let mut stream = eth_client.subscribe(eth_addr, eth_topic).await?; //todo logzz &&  business logic
        while let Some(_a) = stream.next().await {
            todo!()
            // ton_client.sign_eth_to_ton_event();
        }
        Ok(())
    }

    pub async fn run(&self) -> Result<(), anyhow::Error> {
        info!("Bridge started");
        let config = self.ton_client.get_ethereum_events_configuration().await?;
        tokio::spawn(Self::eth_side(
            self.eth_client.clone(),
            config,
            self.ton_client.clone(),
        ));
        tokio::time::delay_for(Duration::from_secs(8640000)).await;
        Ok(())
    }

    fn start_voting_for_update_config() {
        todo!()
    }
    fn update_config() {
        todo!()
    }
    fn start_voting_for_remove_event_type() {
        todo!()
    }
}
