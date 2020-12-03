use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;

use ethereum_types::{Address, H160, H256};
use tokio::stream::StreamExt;
use tokio::sync::{mpsc, Mutex, Notify, RwLock};
use ton_block::{MsgAddrStd, MsgAddressInt};

use relay_ton::contracts::ethereum_event::EthereumEventContract;
use relay_ton::contracts::{
    BridgeContract, BridgeContractEvent, ContractWithEvents, EthereumEventConfiguration,
    EthereumEventConfigurationContract, EthereumEventConfigurationContractEvent,
    EthereumEventDetails,
};
use relay_ton::transport::Transport;

use crate::engine::bridge::util::abi_to_topic_hash;

#[derive(Debug, Clone)]
pub struct MappedData {
    pub eth_addr: HashSet<ethereum_types::Address>,
    pub eth_topic: HashSet<H256>,
    pub address_topic_map: HashMap<H160, Vec<(H256, Vec<ethabi::ParamType>)>>,
    pub topic_abi_map: HashMap<H256, Vec<ethabi::ParamType>>,
    pub eth_proxy_map: HashMap<H160, MsgAddrStd>,
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

#[derive(Clone)]
pub struct ConfigListener {
    current_config: Arc<RwLock<MappedData>>,
    initial_data_received: Arc<Notify>,
    eth_even_config: Arc<RwLock<HashSet<EthereumEventDetails>>>,
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
    initial_config_number: Option<Arc<Mutex<u64>>>,
) {
    log::debug!("start listening config: {:?}", config);

    let configuration_contract = make_config_contract(&transport, config).await;
    let mut eth_events = configuration_contract.events();
    while let Some(event) = eth_events.next().await {
        log::debug!("got event configuration config event: {:?}", event);
        if let Some(a) = &initial_config_number {
            *a.lock().await -= 1;
        }
        if tx.send(event).is_err() {
            return;
        }
    }
}

fn update_mapped_data(mapped_data: &mut MappedData, conf: EthereumEventConfiguration) {
    let address = Address::from_slice(conf.ethereum_event_address.as_slice());
    mapped_data.eth_addr.insert(address);
    mapped_data
        .eth_proxy_map
        .insert(address, conf.event_proxy_address);
    let topics = match abi_to_topic_hash(&conf.ethereum_event_abi) {
        Ok(a) => a,
        Err(e) => {
            log::error!("Failed parsing abi: {}", e);
            return;
        }
    };
    topics.iter().for_each(|x| {
        mapped_data.eth_topic.insert(x.0);
    });
    mapped_data
        .address_topic_map
        .insert(address, topics.clone());
    topics.into_iter().for_each(|topic| {
        mapped_data.topic_abi_map.insert(topic.0, topic.1);
    });
}
/// Listens to config streams and maps them.
impl ConfigListener {
    pub async fn get_config(&self) -> MappedData {
        let lock = self.current_config.read().await;
        lock.clone()
    }

    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            current_config: Arc::new(RwLock::new(MappedData::new())),
            initial_data_received: Arc::new(Notify::new()),
            eth_even_config: Arc::new(RwLock::new(HashSet::new())),
        })
    }
    pub async fn get_initial_config(&self) -> MappedData {
        self.initial_data_received.notified().await;
        self.current_config.read().await.clone()
    }

    async fn notify_received(self: Arc<Self>, number: Arc<Mutex<usize>>) {
        loop {
            let number = number.lock().await;
            if *number == 0 {
                self.initial_data_received.notify();
                return;
            }
            tokio::time::delay_for(tokio::time::Duration::from_millis(300)).await;
        }
    }

    pub async fn run(self: Arc<Self>, transport: Arc<dyn Transport>, bridge: BridgeContract) {
        let known_configs = bridge.get_known_config_contracts().await.unwrap();
        let (tx, mut ton_events) = mpsc::unbounded_channel();

        let initial_config_count = Arc::new(Mutex::new(known_configs.len()));
        tokio::spawn(self.clone().notify_received(initial_config_count.clone()));

        for config in known_configs.into_iter() {
            log::debug!("start listening config: {:?}", config);
            tokio::spawn(listener(transport.clone(), tx.clone(), config, None));
        }
        tokio::spawn({
            let transport = transport.clone();
            let bridge = Arc::new(bridge.clone());
            async move {
                let mut bridge_events = bridge.events();
                while let Some(event) = bridge_events.next().await {
                    match event {
                        BridgeContractEvent::NewEthereumEventConfiguration { address } => {
                            tokio::spawn(listener(transport.clone(), tx.clone(), address, None));
                        }
                    }
                }
            }
        });

        let ethereum_event_contract =
            Arc::new(EthereumEventContract::new(transport.clone()).await.unwrap());
        while let Some(event) = ton_events.next().await {
            match event {
                EthereumEventConfigurationContractEvent::NewEthereumEventConfirmation {
                    address,
                    ..
                } => {
                    let details = ethereum_event_contract.get_details(address.clone()).await;
                    let ethereum_event_configuration_contract =
                        EthereumEventConfigurationContract::new(
                            transport.clone(),
                            ton_block::MsgAddressInt::AddrStd(address),
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
                    let event_lock = self.eth_even_config.clone();
                    let mut guard = event_lock.write().await;
                    guard.insert(ethereum_event_details);
                    let rwlock = self.current_config.clone();
                    let guard = rwlock.write().await;
                    let mut mapped_data = guard;
                    update_mapped_data(
                        &mut mapped_data,
                        ethereum_event_configuration_contract_details,
                    );
                }
            }
        }
    }
}
