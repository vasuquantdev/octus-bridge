use std::collections::hash_map::Entry;

use ethabi::ParamType as EthParamType;
use tokio::sync::{mpsc, Mutex, RwLock, RwLockReadGuard};
use tokio::sync::{Notify, oneshot};
use ton_abi::ParamType as TonParamType;

use relay_models::models::EventVote;
use relay_ton::contracts::*;
use relay_ton::prelude::UInt256;
use relay_ton::transport::{Transport, TransportError};

use crate::config::TonOperationRetryParams;
use crate::db_management::*;
use crate::engine::bridge::util::{parse_eth_abi, validate_ethereum_event_configuration};
use crate::prelude::*;

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
    confirmations: Mutex<HashMap<MsgAddrStd, oneshot::Sender<()>>>,
    rejections: Mutex<HashMap<MsgAddrStd, oneshot::Sender<()>>>,

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
    pub async fn start(self: &Arc<Self>) -> impl Stream<Item=(Address, H256)> {
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
        let mut known_contracts = Vec::new();
        let mut stream = self.bridge.get_known_config_contracts();
        while let Some(address) = stream.next().await {
            known_contracts.push(address);
        }

        // Wait for all existing configuration contracts subscriptions to start ETH part properly
        let semaphore = Semaphore::new(known_contracts.len());
        for address in known_contracts.into_iter() {
            tokio::spawn(self.clone().subscribe_to_events_configuration_contract(
                subscriptions_tx.clone(),
                address,
                Some(semaphore.clone()),
            ));
        }

        // Wait until all initial subscriptions done
        semaphore.wait().await;

        // Restart sending for all enqueued confirmations
        for (event_address, data) in self.ton_queue.get_all_pending() {
            tokio::spawn(self.clone().ensure_sent(event_address, data));
        }

        // Return subscriptions stream
        subscriptions_rx
    }

    /// Restart voting for failed transactions
    pub fn retry_failed(self: &Arc<Self>) {
        for (event_address, data) in self.ton_queue.get_all_failed() {
            tokio::spawn(self.clone().ensure_sent(event_address, data));
        }
    }

    /// Adds transaction to queue, starts reliable sending
    pub async fn enqueue_vote(self: &Arc<Self>, data: EthTonTransaction) -> Result<(), Error> {
        let event_address = self.get_event_contract_address(&data).await?;

        tokio::spawn(self.clone().ensure_sent(event_address, data));

        Ok(())
    }

    /// Check statistics and current queue wether transaction exists
    fn has_already_voted(&self, event_address: &MsgAddrStd) -> bool {
        self.ton_queue
            .has_event(event_address)
            .expect("Fatal db error")
            || self
            .stats_db
            .has_already_voted(event_address, &self.relay_key)
            .expect("Fatal db error")
    }

    /// Current configs state
    pub async fn get_state(&self) -> RwLockReadGuard<'_, ConfigsState> {
        self.configs_state.read().await
    }

    /// Find configuration contract by its address
    pub async fn get_configuration_contract(
        &self,
        address: &MsgAddressInt,
    ) -> Option<Arc<EthereumEventConfigurationContract>> {
        self.config_contracts.read().await.get(address).cloned()
    }

    /// Compute event address based on its data
    async fn get_event_contract_address(
        &self,
        data: &EthTonTransaction,
    ) -> Result<MsgAddrStd, Error> {
        let data = data.inner().clone();

        let config_contract = match self
            .get_configuration_contract(&data.ethereum_event_configuration_address)
            .await
        {
            Some(contract) => contract,
            None => {
                return Err(anyhow!(
                    "Unknown event configuration contract: {}",
                    data.ethereum_event_configuration_address
                ));
            }
        };

        let event_addr = config_contract
            .compute_event_address(
                data.event_transaction,
                data.event_index.into(),
                data.event_data,
                data.event_block_number.into(),
                data.event_block,
            )
            .await?;

        Ok(event_addr)
    }

    /// Sends message to TON with small amount of retries on failures.
    /// Can be stopped using `cancel` or `notify_found`
    async fn ensure_sent(self: Arc<Self>, event_address: MsgAddrStd, data: EthTonTransaction) {
        // Skip voting for events which are already in stats db and TON queue
        if self.has_already_voted(&event_address) {
            return;
        }

        // Insert specified data in TON queue, replacing failed transaction if it exists
        self.ton_queue
            .insert_pending(&event_address, &data)
            .await
            .unwrap();

        // Start listening for cancellation
        let (rx, vote) = {
            // Get suitable channels map
            let (mut runtime_queue, vote) = match &data {
                EthTonTransaction::Confirm(_) => {
                    (self.confirmations.lock().await, EventVote::Confirm)
                }
                EthTonTransaction::Reject(_) => (self.rejections.lock().await, EventVote::Reject),
            };

            // Just in case of duplication, check if we are already waiting cancellation for this
            // event data set
            match runtime_queue.entry(event_address.clone()) {
                Entry::Occupied(_) => {
                    return;
                }
                Entry::Vacant(entry) => {
                    let (tx, rx) = oneshot::channel();
                    entry.insert(tx);
                    (rx, vote)
                }
            }
        };

        let mut rx = Some(rx);
        let mut retries_count = self.timeout_params.broadcast_in_ton_times;
        let mut retries_interval = self.timeout_params.broadcast_in_ton_interval_secs;

        // Send message with several retries on failure
        let result = loop {
            // Prepare delay future
            let delay = tokio::time::delay_for(retries_interval);
            retries_interval = std::time::Duration::from_secs_f64(
                retries_interval.as_secs_f64()
                    * self.timeout_params.broadcast_in_ton_interval_multiplier,
            );

            // Try send message
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

                // Wait for prepared delay on failure
                delay.await;
            } else if let Some(rx_fut) = rx.take() {
                // Handle future results
                match future::select(rx_fut, delay).await {
                    // Got cancellation notification
                    future::Either::Left((Ok(()), _)) => {
                        log::info!(
                            "Got response for voting for {:?} {}",
                            vote,
                            data.inner().event_transaction,
                        );
                        break Ok(());
                    }
                    // Stopped waiting for notification
                    future::Either::Left((Err(e), _)) => {
                        break Err(e.into());
                    }
                    // Timeout reached
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

        let hash = hex::encode(data.inner().event_transaction.as_bytes());
        match result {
            // Do nothing on success
            Ok(_) => {
                log::info!("Stopped waiting for transaction: {}", hash);
            }
            // When ran out of retries count, stop waiting for transaction and mark it as failed
            Err(e) => {
                log::error!("Stopped waiting for transaction: {}. Reason: {:?}", hash, e);
                if let Err(e) = self.ton_queue.mark_failed(&event_address) {
                    log::error!(
                        "failed to mark transaction with hash {} as failed: {:?}",
                        hash,
                        e
                    );
                }
            }
        }

        // Remove cancellation channel
        self.cancel(&event_address, vote).await;
    }

    /// Remove transaction from TON queue and notify spawned `ensure_sent`
    async fn notify_found(&self, event: &ExtendedEventInfo) {
        let mut table = match event.vote {
            EventVote::Confirm => self.confirmations.lock().await,
            EventVote::Reject => self.rejections.lock().await,
        };

        if let Some(tx) = table.remove(&event.event_addr) {
            if tx.send(()).is_err() {
                log::error!("Failed sending event notification");
            }
        }
    }

    /// Just remove the transaction from TON queue
    async fn cancel(&self, event_address: &MsgAddrStd, vote: EventVote) {
        match vote {
            EventVote::Confirm => self.confirmations.lock().await.remove(event_address),
            EventVote::Reject => self.rejections.lock().await.remove(event_address),
        };
    }

    /// Creates listener for new event configuration contract
    async fn subscribe_to_events_configuration_contract(
        self: Arc<Self>,
        subscriptions_tx: mpsc::UnboundedSender<(Address, H256)>,
        address: MsgAddrStd,
        semaphore: Option<Semaphore>,
    ) {
        let address = MsgAddressInt::AddrStd(address);

        // Ensure identity
        let new_configuration = self
            .known_config_addresses
            .lock()
            .await
            .insert(address.clone());
        if !new_configuration {
            try_notify(semaphore).await;
            return;
        }

        // Create config contract
        let config_contract = make_config_contract(
            self.transport.clone(),
            address.clone(),
            self.bridge.address().clone(),
        )
            .await;

        // Get it's data
        let details = match self
            .get_event_configuration_details(config_contract.as_ref(), semaphore.clone())
            .await
        {
            Ok(details) => details,
            Err(e) => {
                log::error!("{:?}", e);
                return;
            }
        };

        // Register contract object
        self.config_contracts
            .write()
            .await
            .insert(address.clone(), config_contract.clone());
        try_notify(semaphore).await;

        // Prefetch required properties
        let ethereum_event_blocks_to_confirm = details
            .ethereum_event_blocks_to_confirm
            .to_u64()
            .unwrap_or_else(u64::max_value);
        let mut is_active = details.active;
        let mut subscription_data = Some((subscriptions_tx, details.ethereum_event_address));

        // Store configuration contract details
        self.configs_state
            .write()
            .await
            .insert_configuration(address.clone(), details);

        if is_active {
            // Subscribe to topic in ETH if configuration contract is active
            if let Some((subscriptions_tx, ethereum_event_address)) = subscription_data.take() {
                self.notify_subscription(subscriptions_tx, ethereum_event_address)
                    .await;
            }
        }

        // Spawn listener of new events
        tokio::spawn({
            let listener = self.clone();
            let config_contract = config_contract.clone();

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
                        _ if !is_active => {
                            if let Ok(details) = listener
                                .get_event_configuration_details(config_contract.as_ref(), None)
                                .await
                            {
                                is_active = details.active;

                                listener.configs_state.write().await.eth_configs_map.insert(
                                    details.ethereum_event_address,
                                    (address.clone(), details),
                                );

                                if is_active {
                                    if let Some((subscriptions_tx, ethereum_event_address)) =
                                    subscription_data.take()
                                    {
                                        listener
                                            .notify_subscription(
                                                subscriptions_tx,
                                                ethereum_event_address,
                                            )
                                            .await;
                                    }
                                }
                            }

                            continue;
                        }
                        _ => continue,
                    };

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

    async fn notify_subscription(
        &self,
        subscriptions_tx: mpsc::UnboundedSender<(Address, H256)>,
        ethereum_event_address: Address,
    ) {
        let topics = self.get_state().await;
        if let Some((topic, _, _)) = topics.address_topic_map.get(&ethereum_event_address) {
            let _ = subscriptions_tx.send((ethereum_event_address, *topic));
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

        let should_check = event.vote == EventVote::Confirm
            && !event.data.proxy_callback_executed
            && !event.data.event_rejected
            && event.relay_key != self.relay_key // event from other relay
            && !self.has_already_voted(&event.event_addr);

        log::info!("Received {}, should check: {}", event, should_check);

        self.stats_db
            .update_relay_stats(&event)
            .await
            .expect("Fatal db error");

        if event.relay_key == self.relay_key {
            // Stop retrying after our event response was found
            if let Err(e) = self.ton_queue.mark_complete(&event.event_addr) {
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

    async fn get_event_configuration_details(
        &self,
        config_contract: &EthereumEventConfigurationContract,
        semaphore: Option<Semaphore>,
    ) -> Result<EthereumEventConfiguration, Error> {
        // retry connection to configuration contract
        let mut retries_count = self.timeout_params.configuration_contract_try_poll_times;
        // 1 sec ~= time before next block in masterchain.
        // Should be greater then account polling interval
        let retries_interval = self.timeout_params.get_event_details_poll_interval_secs;

        let notify_if_init = || async {
            if semaphore.is_some() {
                try_notify(semaphore).await;
                self.known_config_addresses
                    .lock()
                    .await
                    .remove(config_contract.address());
            }
        };

        loop {
            match config_contract.get_details().await {
                Ok(details) => match validate_ethereum_event_configuration(&details) {
                    Ok(_) => break Ok(details),
                    Err(e) => {
                        notify_if_init().await;
                        break Err(anyhow!("got bad ethereum config: {:?}", e));
                    }
                },
                Err(ContractError::TransportError(TransportError::AccountNotFound))
                if retries_count > 0 =>
                    {
                        retries_count -= 1;
                        log::warn!(
                            "failed to get events configuration contract details for {}. Retrying ({} left)",
                            config_contract.address(),
                            retries_count
                        );
                        tokio::time::delay_for(retries_interval).await;
                    }
                Err(e) => {
                    notify_if_init().await;
                    break Err(anyhow!(
                        "failed to get events configuration contract details: {:?}",
                        e
                    ));
                }
            }
        }
    }

    async fn get_event_details(
        &self,
        event_addr: MsgAddrStd,
    ) -> Result<EthereumEventDetails, ContractError> {
        let mut retries_count = self.timeout_params.get_event_details_retry_times;
        let retries_interval = self.timeout_params.get_event_details_poll_interval_secs; // 1 sec ~= time before next block in masterchain.

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
