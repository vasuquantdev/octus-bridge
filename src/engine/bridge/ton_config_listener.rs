use std::collections::{HashMap, HashSet};
use std::ops::Deref;
use std::str::FromStr;
use std::sync::Arc;

use anyhow::Error;
use ethereum_types::{Address, H160, H256};
use futures::TryFutureExt;
use serde::{Deserialize, Serialize};
use tokio::stream::StreamExt;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::{mpsc, Mutex, Notify, RwLock};
use tokio::time::{delay_for, Duration};
use ton_block::{MsgAddrStd, MsgAddressInt};

use relay_ton::contracts::{
    BridgeContract, BridgeContractEvent, ContractWithEvents, EthereumEventConfiguration,
    EthereumEventConfigurationContract, EthereumEventConfigurationContractEvent,
    EthereumEventContract, EthereumEventDetails,
};
use relay_ton::prelude::UInt256;
use relay_ton::prelude::{serde_std_addr, serde_uint256};
use relay_ton::transport::Transport;

use crate::engine::bridge::util::{abi_to_topic_hash, validate_ethereum_event_configuration};

#[derive(Debug, Clone)]
pub struct MappedData {
    pub eth_addr: HashSet<ethereum_types::Address>,
    pub eth_topic: HashSet<H256>,
    pub address_topic_map: HashMap<H160, (H256, Vec<ethabi::ParamType>)>,
    pub topic_abi_map: HashMap<H256, Vec<ethabi::ParamType>>,
    pub eth_proxy_map: HashMap<H160, MsgAddrStd>,
}

#[derive(Debug, Clone, Hash, Serialize, Deserialize)]
pub struct ExtendedEventInfo {
    #[serde(with = "serde_std_addr")]
    pub address: MsgAddrStd,
    #[serde(with = "serde_uint256")]
    pub relay_key: UInt256,
    pub data: EthereumEventDetails,
}

impl MappedData {
    fn new() -> Self {
        Self {
            eth_addr: HashSet::new(),
            eth_topic: HashSet::new(),
            address_topic_map: HashMap::new(),
            topic_abi_map: HashMap::new(),
            eth_proxy_map: HashMap::new(),
        }
    }
}

#[derive(Debug)]
pub struct ConfigListener {
    current_config: Arc<RwLock<MappedData>>,
    initial_data_received: Arc<Notify>,
    ton_received_events: Arc<Mutex<Option<mpsc::UnboundedReceiver<ExtendedEventInfo>>>>,
    ton_tx: mpsc::UnboundedSender<ExtendedEventInfo>,
    event_configuration: Arc<RwLock<HashMap<Vec<u8>, EthereumEventConfiguration>>>,
}

async fn make_config_contract(
    transport: &Arc<dyn Transport>,
    addr: MsgAddrStd,
) -> Arc<EthereumEventConfigurationContract> {
    Arc::new(
        EthereumEventConfigurationContract::new(transport.clone(), MsgAddressInt::AddrStd(addr))
            .await
            .unwrap(),
    )
}

async fn listener(
    transport: Arc<dyn Transport>,
    tx: mpsc::UnboundedSender<EthereumEventConfigurationContractEvent>,
    config: MsgAddrStd,
) {
    log::debug!("start listening config: {:?}", config);

    let configuration_contract = make_config_contract(&transport, config).await;
    let mut eth_events = configuration_contract.events();
    while let Some(event) = eth_events.next().await {
        log::debug!("got event configuration config event: {:?}", event);
        if tx.send(event).is_err() {
            return;
        }
    }
}

fn update_mapped_data(mapped_data: &mut MappedData, conf: EthereumEventConfiguration) {
    if let Err(e) = validate_ethereum_event_configuration(&conf) {
        log::error!("Got bad EthereumEventConfiguration: {}", e);
        return;
    }
    let address = String::from_utf8(conf.ethereum_event_address).unwrap(); //checked upper
    let address = match address.strip_prefix("0x") {
        None => address,
        Some(a) => a.to_string(),
    };

    let address = match Address::from_str(&address) {
        Ok(a) => a,
        Err(e) => {
            log::error!(
                "Failed parsing ethereum_event_address ({}) as H160: {}",
                address,
                e
            );
            return;
        }
    };
    mapped_data.eth_addr.insert(address);
    mapped_data
        .eth_proxy_map
        .insert(address, conf.event_proxy_address);
    let topic = match abi_to_topic_hash(&conf.ethereum_event_abi) {
        Ok(a) => a,
        Err(e) => {
            log::error!("Failed parsing abi: {}", e);
            return;
        }
    };

    mapped_data.eth_topic.insert(topic.0);
    mapped_data
        .address_topic_map
        .insert(address, topic.clone());
    mapped_data.topic_abi_map.insert(topic.0, topic.1);
}

async fn get_initial_configs(
    transport: Arc<dyn Transport>,
    configs: &[MsgAddrStd],
) -> Result<Vec<EthereumEventConfiguration>, Error> {
    async fn get_config(
        config: MsgAddrStd,
        transport: &Arc<dyn Transport>,
    ) -> Result<EthereumEventConfiguration, Error> {
        let contract = make_config_contract(&transport, config).await;
        Ok(contract.get_details().await?)
    }
    let futures = configs
        .into_iter()
        .map(|x| get_config(x.clone(), &transport));
    futures::future::join_all(futures)
        .await
        .into_iter()
        .collect()
}

/// Listens to config streams and maps them.
impl ConfigListener {
    pub async fn get_config(&self) -> MappedData {
        let lock = self.current_config.read().await;
        lock.clone()
    }

    pub fn new() -> Arc<Self> {
        let (tx, rx) = mpsc::unbounded_channel();
        Arc::new(Self {
            current_config: Arc::new(RwLock::new(MappedData::new())),
            initial_data_received: Arc::new(Notify::new()),
            ton_received_events: Arc::new(Mutex::new(Some(rx))),
            ton_tx: tx,
            event_configuration: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    ///Gives mapping address in ethereum :EthereumEventConfiguration
    pub async fn get_event_configuration(&self) -> HashMap<Vec<u8>, EthereumEventConfiguration> {
        loop {
            let configuration = self.event_configuration.read().await;
            if !configuration.is_empty() {
                return configuration.clone();
            }
            delay_for(Duration::from_millis(300)).await;
        }
    }

    pub async fn get_initial_config_map(&self) -> MappedData {
        self.initial_data_received.notified().await;
        self.current_config.read().await.clone()
    }

    pub async fn get_config_map(&self) -> MappedData {
        self.current_config.read().await.clone()
    }

    pub async fn get_events_stream(&self) -> Option<UnboundedReceiver<ExtendedEventInfo>> {
        let mut guard = self.ton_received_events.lock().await;
        guard.take()
    }

    async fn notify_received(self: Arc<Self>, number: Arc<Mutex<usize>>) {
        loop {
            let number = number.lock().await;
            log::error!("Got congig {}", *number);
            if *number == 0 {
                self.initial_data_received.notify();
                return;
            }
            drop(number);
            tokio::time::delay_for(tokio::time::Duration::from_millis(300)).await;
        }
    }

    pub async fn run(self: Arc<Self>, transport: Arc<dyn Transport>, bridge: BridgeContract) {
        let known_configs = bridge.get_known_config_contracts().await.unwrap();
        let (tx, mut ton_events) = mpsc::unbounded_channel();

        log::error!("Known configs: {:?}", &known_configs);
        let initial_configs: Vec<_> = get_initial_configs(transport.clone(), &known_configs)
            .await
            .unwrap()
            .into_iter()
            .filter_map(|x| match validate_ethereum_event_configuration(&x) {
                Ok(_) => Some(x),
                Err(e) => {
                    log::error!("Got bad ethereum config: {}", e);
                    None
                }
            })
            .collect();
        {
            let rwlock = self.current_config.clone();
            let guard = rwlock.write().await;
            let mut mapped_data = guard;
            initial_configs
                .iter()
                .cloned()
                .for_each(|x| update_mapped_data(&mut mapped_data, x));
            log::info!(
                "Parsed initial configs. Mapping: {:#?}",
                mapped_data.deref()
            );
            let rwlock = self.event_configuration.clone();
            let mut guard = rwlock.write().await;
            initial_configs.into_iter().for_each(|config| {
                guard.insert(config.ethereum_event_address.clone(), config);
            });
            log::info!("EthereumEventConfiguration: {:#?}", guard);
        }
        self.initial_data_received.notify();
        for config in known_configs.into_iter() {
            log::info!("start listening config: {:?}", config);
            tokio::spawn(listener(transport.clone(), tx.clone(), config));
        }

        tokio::spawn({
            let transport = transport.clone();
            let bridge = Arc::new(bridge.clone());
            async move {
                let mut bridge_events = bridge.events();
                while let Some(event) = bridge_events.next().await {
                    match event {
                        BridgeContractEvent::NewEthereumEventConfiguration { address } => {
                            tokio::spawn(listener(transport.clone(), tx.clone(), address));
                        }
                    }
                }
            }
        });

        let ethereum_event_contract =
            Arc::new(EthereumEventContract::new(transport.clone()).await.unwrap());

        let ton_tx = self.ton_tx.clone();
        while let Some(event) = ton_events.next().await {
            if let EthereumEventConfigurationContractEvent::NewEthereumEventConfirmation {
                address,
                relay_key,
            } = event
            {
                let details = ethereum_event_contract.get_details(address.clone()).await;
                let ethereum_event_configuration_contract =
                    EthereumEventConfigurationContract::new(
                        transport.clone(),
                        ton_block::MsgAddressInt::AddrStd(address.clone()),
                    )
                    .await
                    .unwrap();

                let ethereum_event_configuration_contract_details =
                    ethereum_event_configuration_contract
                        .get_details()
                        .await
                        .unwrap();

                let ethereum_event_details = match details {
                    Ok(a) => a,
                    Err(e) => {
                        log::error!("get_details failed: {}", e);
                        continue;
                    }
                };
                if let Err(e) = ton_tx.send(ExtendedEventInfo {
                    relay_key,
                    address,
                    data: ethereum_event_details,
                }) {
                    log::error!("Failed sending eth event details via channel: {}", e);
                }

                let rwlock = self.current_config.clone();
                let guard = rwlock.write().await;
                let mut mapped_data = guard;
                update_mapped_data(
                    &mut mapped_data,
                    ethereum_event_configuration_contract_details.clone(),
                );
                let lock = self.event_configuration.clone();
                let mut guard = lock.write().await;
                guard.insert(
                    ethereum_event_configuration_contract_details
                        .ethereum_event_address
                        .clone(),
                    ethereum_event_configuration_contract_details,
                );
            }
        }
    }
}
