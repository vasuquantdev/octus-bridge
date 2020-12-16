use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use ethereum_types::{Address, H256};
use futures::{ Stream, StreamExt};
use tokio::sync::{mpsc, Mutex, RwLock, RwLockReadGuard};
use ton_block::{MsgAddrStd, MsgAddressInt};

use ethabi::ParamType as EthParamType;
use relay_ton::contracts::*;
use relay_ton::prelude::UInt256;
use relay_ton::transport::{Transport, TransportError};
use ton_abi::ParamType as TonParamType;

use super::models::{EventVote, ExtendedEventInfo};
use crate::engine::bridge::util::{parse_eth_abi, validate_ethereum_event_configuration};
use num_traits::ToPrimitive;

/// Listens to config streams and maps them.
#[derive(Debug)]
pub struct EventConfigurationsListener {
    configs_state: Arc<RwLock<ConfigsState>>,
    known_config_contracts: Arc<Mutex<HashSet<MsgAddressInt>>>,
}

impl EventConfigurationsListener {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            configs_state: Arc::new(RwLock::new(ConfigsState::new())),
            known_config_contracts: Arc::new(Mutex::new(HashSet::new())),
        })
    }

    pub async fn start(
        self: &Arc<Self>,
        transport: Arc<dyn Transport>,
        bridge: Arc<BridgeContract>,
    ) -> (
        impl Stream<Item = (Address, H256)>,
        impl Stream<Item = ExtendedEventInfo>,
    ) {
        let (subscriptions_tx, subscriptions_rx) = mpsc::unbounded_channel();
        let (events_tx, events_rx) = mpsc::unbounded_channel();

        let ethereum_event_contract =
            Arc::new(EthereumEventContract::new(transport.clone()).await.unwrap());

        // Subscribe to bridge events
        tokio::spawn({
            let listener = self.clone();
            let transport = transport.clone();
            let event_contract = ethereum_event_contract.clone();

            let subscriptions_tx = subscriptions_tx.clone();
            let events_tx = events_tx.clone();

            let mut bridge_events = bridge.events();

            async move {
                while let Some(event) = bridge_events.next().await {
                    match event {
                        BridgeContractEvent::NewEthereumEventConfiguration { address } => {
                            tokio::spawn(
                                listener.clone().subscribe_to_events_configuration_contract(
                                    transport.clone(),
                                    event_contract.clone(),
                                    subscriptions_tx.clone(),
                                    events_tx.clone(),
                                    address,
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

        for address in known_configs {
            tokio::spawn(self.clone().subscribe_to_events_configuration_contract(
                transport.clone(),
                ethereum_event_contract.clone(),
                subscriptions_tx.clone(),
                events_tx.clone(),
                address,
            ));
        }

        (subscriptions_rx, events_rx)
    }

    // Creates listener for new event configuration contracts
    async fn subscribe_to_events_configuration_contract(
        self: Arc<Self>,
        transport: Arc<dyn Transport>,
        event_contract: Arc<EthereumEventContract>,
        subscriptions_tx: mpsc::UnboundedSender<(Address, H256)>,
        events_tx: mpsc::UnboundedSender<ExtendedEventInfo>,
        address: MsgAddrStd,
    ) {
        let address = MsgAddressInt::AddrStd(address);

        let mut known_config_contracts = self.known_config_contracts.lock().await;
        if !known_config_contracts.insert(address.clone()) {
            return;
        }

        // TODO: retry connection to configuration contract
        let config_contract = make_config_contract(transport, address.clone()).await;

        let details = match config_contract.get_details().await {
            Ok(details) => match validate_ethereum_event_configuration(&details) {
                Ok(_) => details,
                Err(e) => {
                    known_config_contracts.remove(&address);
                    log::error!("got bad ethereum config: {:?}", e);
                    return;
                }
            },
            Err(e) => {
                known_config_contracts.remove(&address);
                log::error!(
                    "failed to get events configuration contract details: {:?}",
                    e
                );
                return;
            }
        };

        std::mem::drop(known_config_contracts); // drop lock

        let ethereum_event_blocks_to_confirm = details
            .ethereum_event_blocks_to_confirm
            .to_u64()
            .unwrap_or_else(u64::max_value);
        let ethereum_event_address = details.ethereum_event_address;

        self.configs_state
            .write()
            .await
            .insert_configuration(address, details);

        {
            let topics = self.get_state().await;
            if let Some((topic, _, _)) = topics.address_topic_map.get(&ethereum_event_address) {
                let _ = subscriptions_tx.send((ethereum_event_address, *topic));
            }
        }

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

            // TODO: update config on new event configuration confirmations
            // let mut states = self.configs_state.write().await;

            tokio::spawn(handle_event(
                event_contract.clone(),
                ethereum_event_blocks_to_confirm,
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
    ethereum_event_blocks_to_confirm: u64,
    events_tx: mpsc::UnboundedSender<ExtendedEventInfo>,
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

    if let Err(e) = events_tx.send(ExtendedEventInfo {
        vote,
        event_addr,
        relay_key,
        ethereum_event_blocks_to_confirm,
        data,
    }) {
        log::error!("Failed sending eth event details via channel: {:?}", e);
    }
}

#[derive(Debug, Clone)]
pub struct ConfigsState {
    pub eth_addr: HashSet<Address>,
    pub address_topic_map: HashMap<Address, (H256, Vec<EthParamType>, Vec<TonParamType>)>,
    pub topic_abi_map: HashMap<H256, Vec<EthParamType>>,
    pub eth_configs_map: HashMap<Address, (MsgAddressInt, EthereumEventConfiguration)>,
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

        let (topic_hash, eth_abi, ton_abi) = match parse_eth_abi(&configuration.ethereum_event_abi)
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
            (topic_hash, eth_abi.clone(), ton_abi),
        );
        self.topic_abi_map.insert(topic_hash, eth_abi);
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
