use anyhow::Error;
use ethereum_types::{H160, H256};
use futures::StreamExt;
use log::info;
use tokio::time::Duration;

use relay_eth::ws::{Address, EthListener};
use relay_ton::contracts::utils::pack_tokens;
use relay_ton::contracts::*;
use relay_ton::prelude::{HashMap, MsgAddrStd};

use crate::crypto::key_managment::EthSigner;
use crate::engine::bridge::util::{abi_to_topic_hash, map_eth_ton};

mod util;

pub struct Bridge {
    eth_signer: EthSigner,
    ton_client: BridgeContract,
    eth_client: EthListener,
}

#[derive(Debug)]
struct MappedData {
    eth_addr: Vec<ethereum_types::Address>,
    eth_topic: Vec<H256>,
    address_topic_map: HashMap<H160, Vec<(H256, Vec<ethabi::ParamType>)>>,
    topic_abi_map: HashMap<H256, Vec<ethabi::ParamType>>,
    eth_proxy_map: HashMap<H160, MsgAddrStd>,
}

impl Bridge {
    pub fn new(eth_signer: EthSigner, eth_client: EthListener, ton_client: BridgeContract) -> Self {
        Self {
            eth_signer,
            ton_client,
            eth_client,
        }
    }

    async fn eth_config_to_maps(ton_client: &BridgeContract) -> Result<MappedData, Error> {
        //todo process TransportError
        let mut eth_addr = Vec::new();
        let mut eth_topic = Vec::new();
        let mut address_topic_map = HashMap::new();
        let mut topic_abi_map = HashMap::new();
        let mut eth_proxy_map = HashMap::new();
        let config = ton_client.get_ethereum_events_configuration().await?;
        for conf in config.into_iter() {
            let address = Address::from_slice(conf.ethereum_address.as_slice());
            eth_addr.push(address);
            eth_proxy_map.insert(address, conf.event_proxy_address);
            let topics = abi_to_topic_hash(&conf.ethereum_event_abi)?;
            topics.iter().for_each(|x| eth_topic.push(x.0));
            address_topic_map.insert(address, topics.clone());
            topics.into_iter().for_each(|topic| {
                topic_abi_map.insert(topic.0, topic.1);
            });
        }
        let mapped_data = MappedData {
            eth_addr,
            eth_topic,
            address_topic_map,
            topic_abi_map,
            eth_proxy_map,
        };
        log::trace!("Full config: {:#?}", &mapped_data);

        Ok(mapped_data)
    }

    async fn eth_side(eth_client: EthListener, ton_client: BridgeContract) -> Result<(), Error> {
        let MappedData {
            eth_addr,
            eth_topic,
            address_topic_map,
            topic_abi_map,
            eth_proxy_map,
        } = Self::eth_config_to_maps(&ton_client).await?;

        log::info!("Got config for ethereum.");
        log::debug!("Topics: {:?}", &eth_topic);
        log::debug!("Ethereum address: {:?}", &eth_addr);

        let mut stream = eth_client.subscribe(eth_addr, eth_topic).await?;
        while let Some(a) = stream.next().await {
            let event: relay_eth::ws::Event = match a {
                Ok(a) => a,
                Err(e) => {
                    log::error!("Failed parsing data from ethereum stream: {}", e);
                    continue;
                }
            };
            log::info!(
                "Received event from address: {}. Tx hash: {}.",
                &event.address,
                &event.tx_hash
            );

            let decoded_data: Option<Result<Vec<ethabi::Token>, _>> = event
                .topics
                .iter()
                .map(|x| topic_abi_map.get(x))
                .filter_map(|x| x)
                .map(|x| ethabi::decode(x, &event.data))
                .next();
            //taking first element, cause topics and abi shouldn't overlap more than once
            let tokens = match decoded_data {
                None => {
                    log::error!("No data from event could be parsed");
                    continue;
                }
                Some(a) => match a {
                    Ok(a) => a,
                    Err(e) => {
                        log::error!("Failed decoding data from event: {}", e);
                        continue;
                    }
                },
            };
            let ton_data: Vec<_> = tokens.into_iter().map(map_eth_ton).collect();
            let cell = match pack_tokens(ton_data) {
                Ok(a) => a,
                Err(e) => {
                    log::error!("Failed mapping ton_data to cell: {}", e);
                    continue;
                }
            };
            let eth_event_address = match eth_proxy_map.get(&event.address) {
                Some(a) => a.clone(),
                None => {
                    log::error!("Can't map eth address into ton proxy contract address");
                    continue;
                }
            };
            if let Err(e) = ton_client
                .sign_eth_to_ton_event(cell, eth_event_address)
                .await
            {
                log::error!("Failed to ton: {}", e); //fixme
            }
            todo!()
        }
        Ok(())
    }

    pub async fn run(&self) -> Result<(), anyhow::Error> {
        info!("Bridge started");
        tokio::spawn(Self::eth_side(
            self.eth_client.clone(),
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
