use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use ethereum_types::{H160, H256};
use futures::StreamExt;
use tokio::sync::{mpsc, Mutex, Notify, RwLock, RwLockReadGuard};
use ton_block::{MsgAddrStd, MsgAddressInt};

use relay_ton::contracts::*;
use relay_ton::prelude::UInt256;
use relay_ton::transport::{Transport, TransportError};

use super::models::{EventVote, ExtendedEventInfo};
use crate::engine::bridge::util::{abi_to_topic_hash, validate_ethereum_event_configuration};

/// Listens to config streams and maps them.
#[derive(Debug)]
pub struct EventConfigurationsListener {
    configs_state: Arc<RwLock<ConfigsState>>,
    known_config_contracts: Arc<Mutex<HashSet<MsgAddressInt>>>,

    initial_data_received: Arc<Notify>,
}

impl EventConfigurationsListener {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            configs_state: Arc::new(RwLock::new(ConfigsState::new())),
            known_config_contracts: Arc::new(Mutex::new(HashSet::new())),
            initial_data_received: Arc::new(Notify::new()),
        })
    }

    pub async fn start(
        self: &Arc<Self>,
        transport: Arc<dyn Transport>,
        bridge: Arc<BridgeContract>,
    ) -> mpsc::UnboundedReceiver<(EventVote, ExtendedEventInfo)> {
        let (events_tx, events_rx) = mpsc::unbounded_channel();

        let ethereum_event_contract =
            Arc::new(EthereumEventContract::new(transport.clone()).await.unwrap());

        // Subscribe to bridge events
        tokio::spawn({
            let transport = transport.clone();
            let listener = self.clone();
            let events_tx = events_tx.clone();
            let event_contract = ethereum_event_contract.clone();
            let mut bridge_events = bridge.events();

            async move {
                while let Some(event) = bridge_events.next().await {
                    match event {
                        BridgeContractEvent::NewEthereumEventConfiguration { address } => {
                            tokio::spawn(
                                listener.clone().subscribe_to_events_configuration_contract(
                                    transport.clone(),
                                    event_contract.clone(),
                                    address,
                                    events_tx.clone(),
                                    None,
                                ),
                            );
                        }
                        BridgeContractEvent::NewBridgeConfigurationUpdate { .. } => {
                            //TODO: handle new bridge configuration
                        }
                    }
                }
            }
        });

        // Get all configs before now
        let known_configs = bridge.get_known_config_contracts().await.unwrap();
        log::info!("Known configs: {:?}", known_configs);
        let semaphore = Semaphore::new(known_configs.len());

        for address in known_configs {
            tokio::spawn(self.clone().subscribe_to_events_configuration_contract(
                transport.clone(),
                ethereum_event_contract.clone(),
                address,
                events_tx.clone(),
                Some(semaphore.clone()),
            ));
        }

        semaphore.wait().await;
        events_rx
    }

    // Creates listener for new event configuration contracts
    async fn subscribe_to_events_configuration_contract(
        self: Arc<Self>,
        transport: Arc<dyn Transport>,
        event_contract: Arc<EthereumEventContract>,
        address: MsgAddrStd,
        events_tx: mpsc::UnboundedSender<(EventVote, ExtendedEventInfo)>,
        semaphore: Option<Semaphore>,
    ) {
        let address = MsgAddressInt::AddrStd(address);

        let mut known_config_contracts = self.known_config_contracts.lock().await;
        if !known_config_contracts.insert(address.clone()) {
            try_notify(semaphore).await;
            return;
        }

        let config_contract = make_config_contract(transport, address.clone()).await;

        let details = match config_contract.get_details().await {
            Ok(details) => match validate_ethereum_event_configuration(&details) {
                Ok(_) => details,
                Err(e) => {
                    try_notify(semaphore).await;
                    known_config_contracts.remove(&address);
                    log::error!("got bad ethereum config: {:?}", e);
                    return;
                }
            },
            Err(e) => {
                try_notify(semaphore).await;
                known_config_contracts.remove(&address);
                log::error!(
                    "failed to get events configuration contract details: {:?}",
                    e
                );
                return;
            }
        };

        std::mem::drop(known_config_contracts); // drop lock

        self.configs_state
            .write()
            .await
            .insert_configuration(address, details);
        try_notify(semaphore).await;

        let mut eth_events = config_contract.events();
        while let Some(event) = eth_events.next().await {
            log::debug!("got event confirmation event: {:?}", event);

            let (address, relay_key, vote) = match event {
                EthereumEventConfigurationContractEvent::NewEthereumEventConfirmation {
                    address,
                    relay_key,
                } => (address, relay_key, EventVote::Confirm),
                EthereumEventConfigurationContractEvent::NewEthereumEventReject {
                    address,
                    relay_key,
                } => (address, relay_key, EventVote::Reject),
            };

            tokio::spawn(handle_event(
                event_contract.clone(),
                events_tx.clone(),
                address,
                relay_key,
                vote,
            ));
        }
    }

    pub async fn get_state(&self) -> RwLockReadGuard<'_, ConfigsState> {
        self.configs_state.read().await
    }
}

async fn handle_event(
    event_contract: Arc<EthereumEventContract>,
    events_tx: mpsc::UnboundedSender<(EventVote, ExtendedEventInfo)>,
    event_addr: MsgAddrStd,
    relay_key: UInt256,
    vote: EventVote,
) {
    // TODO: move into config
    let mut retries_count = 3;
    let retries_interval = tokio::time::Duration::from_secs(1); // 1 sec ~= time before next block in masterchain.
                                                                // Should be greater then account polling interval

    //
    let data = loop {
        match event_contract.get_details(event_addr.clone()).await {
            Ok(details) => break details,
            Err(ContractError::TransportError(TransportError::AccountNotFound))
                if retries_count > 0 =>
            {
                retries_count -= 1;
                log::error!(
                    "Failed to get event details for {}. Retrying ({} left)",
                    event_addr,
                    retries_count
                );
                tokio::time::delay_for(retries_interval).await;
            }
            e => {
                log::error!("get_details failed: {:?}", e);
                return;
            }
        };
    };

    if let Err(e) = events_tx.send((
        vote,
        ExtendedEventInfo {
            event_addr,
            relay_key,
            data,
        },
    )) {
        log::error!("Failed sending eth event details via channel: {:?}", e);
    }

    // TODO: update config
    // let mut states = self.configs_state.write().await;
}

#[derive(Debug, Clone)]
pub struct ConfigsState {
    pub eth_addr: HashSet<ethereum_types::Address>,
    pub address_topic_map: HashMap<H160, (H256, Vec<ethabi::ParamType>)>,
    pub topic_abi_map: HashMap<H256, Vec<ethabi::ParamType>>,
    pub eth_configs_map: HashMap<H160, (MsgAddressInt, EthereumEventConfiguration)>,
}

impl ConfigsState {
    fn new() -> Self {
        Self {
            eth_addr: HashSet::new(),
            address_topic_map: HashMap::new(),
            topic_abi_map: HashMap::new(),
            eth_configs_map: HashMap::new(),
        }
    }

    fn insert_configuration(
        &mut self,
        contract_addr: MsgAddressInt,
        configuration: EthereumEventConfiguration,
    ) {
        if let Err(e) = validate_ethereum_event_configuration(&configuration) {
            log::error!("Got bad EthereumEventConfiguration: {:?}", e);
            return;
        }

        let (topic_hash, topic_params) = match abi_to_topic_hash(&configuration.ethereum_event_abi)
        {
            Ok(a) => a,
            Err(e) => {
                log::error!("Failed parsing abi: {:?}", e);
                return;
            }
        };

        self.eth_addr.insert(configuration.ethereum_event_address);
        self.address_topic_map.insert(
            configuration.ethereum_event_address,
            (topic_hash, topic_params.clone()),
        );
        self.topic_abi_map.insert(topic_hash, topic_params);
        self.eth_configs_map.insert(
            configuration.ethereum_event_address,
            (contract_addr, configuration),
        );
    }
}

async fn make_config_contract(
    transport: Arc<dyn Transport>,
    address: MsgAddressInt,
) -> Arc<EthereumEventConfigurationContract> {
    Arc::new(
        EthereumEventConfigurationContract::new(transport, address)
            .await
            .unwrap(),
    )
}

async fn try_notify(semaphore: Option<Semaphore>) {
    if let Some(semaphore) = semaphore {
        semaphore.notify().await;
    }
}

#[derive(Clone)]
struct Semaphore {
    counter: Arc<Mutex<usize>>,
    done: Arc<Notify>,
}

impl Semaphore {
    fn new(count: usize) -> Self {
        Self {
            counter: Arc::new(Mutex::new(count)),
            done: Arc::new(Notify::new()),
        }
    }

    async fn wait(&self) {
        self.done.notified().await
    }

    async fn notify(&self) {
        let mut counter = self.counter.lock().await;
        match counter.checked_sub(1) {
            Some(new) if new != 0 => *counter = new,
            _ => self.done.notify(),
        }
    }
}
