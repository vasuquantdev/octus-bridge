use crate::crypto::key_managment::EthSigner;
use relay_eth::ws::EthListener;
use tokio::time::Duration;
use log::info;

pub struct Bridge {
    eth_signer: EthSigner,
    ton_client: (),
    eth_client: EthListener
}

impl Bridge {
    pub fn new(eth_signer: EthSigner, eth_client: EthListener) -> Self {
        Self {
            eth_signer,
            ton_client: (),
            eth_client
        }
    }

 pub async  fn run(&self){
     info!("Bridge started");
     tokio::time::delay_for(Duration::from_secs(std::u64::MAX)).await;
 }

    fn start_voting_for_update_config(){

    }
    fn update_config(){

    }
    fn start_voting_for_remove_event_type(){}
}
