use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use anyhow::{anyhow, Error};
use ethabi::ParamType as EthParamType;
use ethereum_types::{Address, H256};
use futures::{future, Stream, StreamExt};
use num_traits::ToPrimitive;
use tokio::sync::oneshot;
use tokio::sync::{mpsc, Mutex, RwLock, RwLockReadGuard};
use ton_abi::ParamType as TonParamType;
use ton_block::{MsgAddrStd, MsgAddressInt};

use relay_ton::contracts::*;
use relay_ton::prelude::UInt256;
use relay_ton::transport::{Transport, TransportError};

use crate::config::TonOperationRetryParams;
use crate::db_managment::*;
use crate::engine::bridge::util::{parse_eth_abi, validate_ethereum_event_configuration};

use super::models::*;

/// Listens to config streams and maps them.
pub struct EventConfigurationsListener {
    transport: Arc<dyn Transport>,
    bridge: Arc<BridgeContract>,
    event_contract: Arc<EthereumEventContract>,

    eth_queue: EthQueue,
    ton_queue: TonQueue,
    stats_db: StatsDb,

    relay_key: UInt256,
    confirmations: Mutex<HashMap<H256, oneshot::Sender<()>>>,
    rejections: Mutex<HashMap<H256, oneshot::Sender<()>>>,

    configs_state: Arc<RwLock<ConfigsState>>,
    config_contracts: Arc<RwLock<ContractsMap>>,
    known_config_addresses: Arc<Mutex<HashSet<MsgAddressInt>>>,
    timeout_params: TonOperationRetryParams,
}

type ContractsMap = HashMap<MsgAddressInt, Arc<EthereumEventConfigurationContract>>;

impl EventConfigurationsListener {
    pub async fn new(
        transport: Arc<dyn Transport>,
        bridge: Arc<BridgeContract>,
        eth_queue: EthQueue,
        ton_queue: TonQueue,
        stats_db: StatsDb,
        timeout_params: TonOperationRetryParams,
    ) -> Arc<Self> {
        let relay_key = bridge.pubkey();

        let event_contract = Arc::new(EthereumEventContract::new(transport.clone()).await.unwrap());

        Arc::new(Self {
            transport,
            bridge,
            event_contract,

            eth_queue,
            ton_queue,
            stats_db,

            relay_key,
            confirmations: Default::default(),
            rejections: Default::default(),

            configs_state: Arc::new(RwLock::new(ConfigsState::new())),
            config_contracts: Arc::new(RwLock::new(HashMap::new())),
            known_config_addresses: Arc::new(Mutex::new(HashSet::new())),
            timeout_params,
        })
    }

    /// Spawn configuration contracts listeners
    pub async fn start(self: &Arc<Self>) -> impl Stream<Item = (Address, H256)> {
        let (subscriptions_tx, subscriptions_rx) = mpsc::unbounded_channel();

        // Subscribe to bridge events
        tokio::spawn({
            let listener = self.clone();
            let subscriptions_tx = subscriptions_tx.clone();

            let mut bridge_events = self.bridge.events();
            async move {
                while let Some(event) = bridge_events.next().await {
                    match event {
                        BridgeContractEvent::NewEthereumEventConfiguration { address } => {
                            tokio::spawn(
                                listener.clone().subscribe_to_events_configuration_contract(
                                    subscriptions_tx.clone(),
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
        let mut known_configs = self.bridge.get_known_config_contracts();
        while let Some(address) = known_configs.next().await {
            tokio::spawn(
                self.clone()
                    .subscribe_to_events_configuration_contract(subscriptions_tx.clone(), address),
            );
        }

        // Return subscriptions stream
        subscriptions_rx
    }

    /// Adds transaction to queue, starts reliable sending
    pub fn enqueue_vote(self: &Arc<Self>, data: EthTonTransaction) -> Result<(), Error> {
        let hash = data.get_event_transaction();
        self.ton_queue.insert_pending(&hash, &data)?;

        tokio::spawn(self.clone().ensure_sent(hash, data));

        Ok(())
    }

    /// Current configs state
    pub async fn get_state(&self) -> RwLockReadGuard<'_, ConfigsState> {
        dbg!(self.configs_state.read().await)
    }

    /// Find configuration contract by its address
    pub async fn get_configuration_contract(
        &self,
        address: &MsgAddressInt,
    ) -> Option<Arc<EthereumEventConfigurationContract>> {
        self.config_contracts.read().await.get(address).cloned()
    }

    async fn ensure_sent(self: Arc<Self>, hash: H256, data: EthTonTransaction) {
        let (tx, rx) = oneshot::channel();

        let vote = match &data {
            EthTonTransaction::Confirm(_) => {
                self.confirmations.lock().await.insert(hash, tx);
                EventVote::Confirm
            }
            EthTonTransaction::Reject(_) => {
                self.rejections.lock().await.insert(hash, tx);
                EventVote::Reject
            }
        };

        let mut rx = Some(rx);
        let mut retries_count = self.timeout_params.broadcast_in_ton_times;
        let mut retries_interval = self.timeout_params.broadcast_in_ton_interval_secs;

        let result = loop {
            let delay = tokio::time::delay_for(retries_interval);
            retries_interval = std::time::Duration::from_secs_f64(
                retries_interval.as_secs_f64()
                    * self.timeout_params.broadcast_in_ton_interval_multiplier,
            );
            if let Err(e) = data.send(&self.bridge).await {
                log::error!(
                    "Failed to vote for event: {:?}. Retrying ({} left)",
                    e,
                    retries_count
                );

                retries_count -= 1;
                if retries_count < 0 {
                    break Err(e.into());
                }

                delay.await;
            } else if let Some(rx_fut) = rx.take() {
                match future::select(rx_fut, delay).await {
                    future::Either::Left((Ok(()), _)) => {
                        log::info!("Got response for voting for {:?} {}", vote, hash);
                        break Ok(());
                    }
                    future::Either::Left((Err(e), _)) => {
                        break Err(e.into());
                    }
                    future::Either::Right((_, new_rx)) => {
                        log::error!(
                            "Failed to get voting event response: timeout reached. Retrying ({} left)",
                            retries_count
                        );

                        retries_count -= 1;
                        if retries_count < 0 {
                            break Err(anyhow!("Failed to vote for event, no retries left"));
                        }

                        rx = Some(new_rx);
                    }
                }
            } else {
                unreachable!()
            }
        };

        self.cancel(&hash, vote).await;

        match result {
            Ok(_) => {
                log::info!(
                    "Stopped waiting for transaction: {}",
                    hex::encode(hash.as_bytes())
                );
            }
            Err(e) => {
                log::error!(
                    "Stopped waiting for transaction: {}. Reason: {:?}",
                    hex::encode(hash.as_bytes()),
                    e
                );
                if let Err(e) = self.ton_queue.mark_failed(&hash) {
                    log::error!("failed to mark transaction: {:?}", e);
                }
            }
        }
    }

    /// Remove transaction from TON queue and notify spawned `ensure_sent`
    async fn notify_found(&self, event: &ExtendedEventInfo) {
        let mut table = match event.vote {
            EventVote::Confirm => self.confirmations.lock().await,
            EventVote::Reject => self.rejections.lock().await,
        };

        if let Some(tx) = table.remove(&event.data.ethereum_event_transaction) {
            if tx.send(()).is_err() {
                log::error!("Failed sending event notification");
            }
        }
    }

    /// Just remove the transaction from TON queue
    async fn cancel(&self, hash: &H256, vote: EventVote) {
        match vote {
            EventVote::Confirm => self.confirmations.lock().await.remove(hash),
            EventVote::Reject => self.rejections.lock().await.remove(hash),
        };
    }

    // Creates listener for new event configuration contract
    async fn subscribe_to_events_configuration_contract(
        self: Arc<Self>,
        subscriptions_tx: mpsc::UnboundedSender<(Address, H256)>,
        address: MsgAddrStd,
    ) {
        let address = MsgAddressInt::AddrStd(address);

        // Ensure identity
        let new_configuration = self
            .known_config_addresses
            .lock()
            .await
            .insert(address.clone());
        if !new_configuration {
            return;
        }

        let config_contract = make_config_contract(
            self.transport.clone(),
            address.clone(),
            self.bridge.address().clone(),
        )
        .await;

        // retry connection to configuration contract
        let mut retries_count = self.timeout_params.configuration_contract_try_poll_times;
        // 1 sec ~= time before next block in masterchain.
        // Should be greater then account polling interval
        let retries_interval = self.timeout_params.get_event_details_poll_interval_secs;

        let details = loop {
            match config_contract.get_details().await {
                Ok(details) => match validate_ethereum_event_configuration(&details) {
                    Ok(_) => break details,
                    Err(e) => {
                        self.known_config_addresses.lock().await.remove(&address);
                        log::error!("got bad ethereum config: {:?}", e);
                        return;
                    }
                },
                Err(ContractError::TransportError(TransportError::AccountNotFound))
                    if retries_count > 0 =>
                {
                    retries_count -= 1;
                    log::error!(
                            "failed to get events configuration contract details for {}. Retrying ({} left)",
                            address,
                            retries_count
                        );
                    tokio::time::delay_for(retries_interval).await;
                }
                Err(e) => {
                    self.known_config_addresses.lock().await.remove(&address);
                    log::error!(
                        "failed to get events configuration contract details: {:?}",
                        e
                    );
                    return;
                }
            }
        };

        // Register contract object
        self.config_contracts
            .write()
            .await
            .insert(address.clone(), config_contract.clone());

        // Prefetch required properties
        let ethereum_event_blocks_to_confirm = details
            .ethereum_event_blocks_to_confirm
            .to_u64()
            .unwrap_or_else(u64::max_value);
        let ethereum_event_address = details.ethereum_event_address;

        //
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

        // Spawn listener of new events
        tokio::spawn({
            let listener = self.clone();

            let mut eth_events = config_contract.events();
            async move {
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
                        _ => continue, // TODO: handle new votes for configuration
                    };

                    // TODO: update config on new event configuration confirmations
                    // let mut states = self.configs_state.write().await;

                    tokio::spawn(listener.clone().handle_event(
                        ethereum_event_blocks_to_confirm,
                        address,
                        relay_key,
                        vote,
                    ));
                }
            }
        });

        // Process all past events
        let mut known_events = config_contract.get_known_events();
        while let Some(event) = known_events.next().await {
            let (address, relay_key, vote) = match event {
                EthereumEventConfigurationContractEvent::NewEthereumEventConfirmation {
                    address,
                    relay_key,
                } => (address, relay_key, EventVote::Confirm),
                EthereumEventConfigurationContractEvent::NewEthereumEventReject {
                    address,
                    relay_key,
                } => (address, relay_key, EventVote::Reject),
                _ => continue, // ignore votes for configuration
            };

            tokio::spawn(self.clone().handle_event(
                ethereum_event_blocks_to_confirm,
                address,
                relay_key,
                vote,
            ));
        }
    }

    async fn handle_event(
        self: Arc<Self>,
        ethereum_event_blocks_to_confirm: u64,
        event_addr: MsgAddrStd,
        relay_key: UInt256,
        vote: EventVote,
    ) {
        let data = match self.get_event_details(event_addr.clone()).await {
            Ok(data) => data,
            Err(e) => {
                log::error!("get_details failed: {:?}", e);
                return;
            }
        };

        let event = ExtendedEventInfo {
            vote,
            event_addr,
            relay_key,
            ethereum_event_blocks_to_confirm,
            data,
        };

        let new_event = !self
            .stats_db
            .has_confirmed_event(&event.event_addr)
            .expect("Fatal db error");
        let should_check = event.vote == EventVote::Confirm
            && new_event
            && !event.data.proxy_callback_executed
            && !event.data.event_rejected;

        log::info!(
            "Received {}, new event: {}, should check: {}",
            event,
            new_event,
            should_check
        );

        self.stats_db
            .update_relay_stats(&event)
            .expect("Fatal db error");

        if event.relay_key == self.relay_key {
            // Stop retrying after our event response was found
            if let Err(e) = self
                .ton_queue
                .mark_complete(&event.data.ethereum_event_transaction)
            {
                log::error!("Failed to mark transaction completed. {:?}", e);
            }

            self.notify_found(&event).await;
        } else if should_check {
            let target_block_number = event.target_block_number();

            if let Err(e) = self
                .eth_queue
                .insert(target_block_number, &event.into())
                .await
            {
                log::error!("Failed to insert event confirmation. {:?}", e);
            }
        }
    }

    async fn get_event_details(
        &self,
        event_addr: MsgAddrStd,
    ) -> Result<EthereumEventDetails, ContractError> {
        // TODO: move into config
        let mut retries_count = 100;

        // Should be greater then account polling interval
        let retries_interval = tokio::time::Duration::from_secs(5); // 1 sec ~= time before next block in masterchain.

        loop {
            match self.event_contract.get_details(event_addr.clone()).await {
                Ok(details) => break Ok(details),
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
                Err(e) => break Err(e),
            };
        }
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
    bridge_address: MsgAddressInt,
) -> Arc<EthereumEventConfigurationContract> {
    Arc::new(
        EthereumEventConfigurationContract::new(transport, address, bridge_address)
            .await
            .unwrap(),
    )
}

impl EthTonTransaction {
    async fn send(&self, bridge: &BridgeContract) -> ContractResult<()> {
        match self.clone() {
            Self::Confirm(a) => {
                bridge
                    .confirm_ethereum_event(
                        a.event_transaction,
                        a.event_index.into(),
                        a.event_data,
                        a.event_block_number.into(),
                        a.event_block,
                        a.ethereum_event_configuration_address,
                    )
                    .await
            }
            Self::Reject(a) => {
                bridge
                    .reject_ethereum_event(
                        a.event_transaction,
                        a.event_index.into(),
                        a.event_data,
                        a.event_block_number.into(),
                        a.event_block,
                        a.ethereum_event_configuration_address,
                    )
                    .await
            }
        }
    }
}
