use log::info;
use tokio::time::Duration;

use relay_eth::ws::EthListener;
use relay_ton::contracts::bridge::{
    BridgeContract, BridgeContractEvent, EthereumEventsConfiguration,
};
use relay_ton::transport::tonlib_transport::config::Config;
use relay_ton::transport::TonlibTransport;

use crate::crypto::key_managment::EthSigner;

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

    pub async fn run(&self) {
        info!("Bridge started");
        tokio::time::delay_for(Duration::from_secs(8640000)).await;
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
