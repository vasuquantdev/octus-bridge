use std::collections::hash_map::Entry;

use async_trait::async_trait;
use ethabi::ParamType as EthParamType;
use futures::future::Either;
use num_bigint::BigUint;
use tokio::stream::StreamExt;
use tokio::sync::{mpsc, Mutex, RwLock, RwLockReadGuard};
use tokio::sync::{oneshot, Notify};
use ton_abi::ParamType as TonParamType;

use relay_models::models::EventVote;
use relay_ton::contracts::*;
use relay_ton::prelude::UInt256;
use relay_ton::transport::{Transport, TransportError};

use crate::config::TonSettings;
use crate::crypto::key_managment::EthSigner;
use crate::db_management::*;
use crate::engine::bridge::util;
use crate::prelude::*;

use super::models::*;

/// Listens to config streams and maps them.
pub struct TonListener {
    transport: Arc<dyn Transport>,

    bridge_contract: Arc<BridgeContract>,
    eth_event_contract: Arc<EthEventContract>,
    ton_event_contract: Arc<TonEventContract>,

    eth_signer: EthSigner,

    eth_queue: EthQueue,
    ton_queue: TonQueue,
    stats_db: StatsDb,

    relay_key: UInt256,
    confirmations: Mutex<HashMap<MsgAddrStd, oneshot::Sender<()>>>,
    rejections: Mutex<HashMap<MsgAddrStd, oneshot::Sender<()>>>,

    configs_state: Arc<RwLock<ConfigsState>>,
    eth_config_contracts: Arc<RwLock<EthConfigContractsMap>>,
    ton_config_contracts: Arc<RwLock<TonConfigContractsMap>>,
    known_config_addresses: Arc<Mutex<HashSet<MsgAddressInt>>>,
    timeouts: TonSettings,
}

type ConfigContractsMap<T> = HashMap<BigUint, Arc<T>>;
type EthConfigContractsMap = ConfigContractsMap<EthEventConfigurationContract>;
type TonConfigContractsMap = ConfigContractsMap<TonEventConfigurationContract>;

impl TonListener {
    pub async fn new(
        transport: Arc<dyn Transport>,
        bridge_contract: Arc<BridgeContract>,
        eth_signer: EthSigner,
        eth_queue: EthQueue,
        ton_queue: TonQueue,
        stats_db: StatsDb,
        timeouts: TonSettings,
    ) -> Arc<Self> {
        let relay_key = bridge_contract.pubkey();

        let eth_event_contract = make_eth_event_contract(transport.clone()).await;
        let ton_event_contract = make_ton_event_contract(transport.clone()).await;

        Arc::new(Self {
            transport,

            bridge_contract,
            eth_event_contract,
            ton_event_contract,

            eth_signer,

            eth_queue,
            ton_queue,
            stats_db,

            relay_key,
            confirmations: Default::default(),
            rejections: Default::default(),

            configs_state: Arc::new(Default::default()),
            eth_config_contracts: Arc::new(Default::default()),
            ton_config_contracts: Arc::new(Default::default()),
            known_config_addresses: Arc::new(Default::default()),
            timeouts,
        })
    }

    /// Spawn configuration contracts listeners
    pub async fn start<T>(
        self: &Arc<Self>,
        mut bridge_contract_events: T,
    ) -> impl Stream<Item = (Address, H256)>
    where
        T: Stream<Item = BridgeContractEvent> + Send + Unpin + 'static,
    {
        let (subscriptions_tx, subscriptions_rx) = mpsc::unbounded_channel();

        // Subscribe to bridge events
        tokio::spawn({
            let listener = self.clone();
            let subscriptions_tx = subscriptions_tx.clone();

            async move {
                while let Some(event) = bridge_contract_events.next().await {
                    match event {
                        BridgeContractEvent::EventConfigurationCreationEnd {
                            id,
                            address,
                            active: true,
                            event_type: EventType::ETH,
                        } => {
                            tokio::spawn(listener.clone().subscribe_to_eth_events_configuration(
                                subscriptions_tx.clone(),
                                id,
                                address,
                                None,
                            ));
                        }
                        _ => {
                            // TODO: handle other events
                        }
                    }
                }
            }
        });

        // Get all configs before now
        let known_contracts = self
            .bridge_contract
            .get_active_event_configurations()
            .await
            .expect("Failed to get known event configurations"); // TODO: is it really a fatal error?

        // Wait for all existing configuration contracts subscriptions to start ETH part properly
        let semaphore = Semaphore::new(known_contracts.len());
        for active_configuration in known_contracts.into_iter() {
            if active_configuration.event_type == EventType::ETH {
                tokio::spawn(self.clone().subscribe_to_eth_events_configuration(
                    subscriptions_tx.clone(),
                    active_configuration.id,
                    active_configuration.address,
                    Some(semaphore.clone()),
                ));
            }
            // TODO: subscribe to TON configuration contract
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
    pub async fn enqueue_vote(self: &Arc<Self>, data: EventTransaction) -> Result<(), Error> {
        let event_address = self.get_event_contract_address(&data).await?;

        tokio::spawn(self.clone().ensure_sent(event_address, data));

        Ok(())
    }

    /// Check current queue whether transaction exists
    fn is_in_queue(&self, event_address: &MsgAddrStd) -> bool {
        self.ton_queue
            .has_event(event_address)
            .expect("Fatal db error")
    }

    /// Check statistics whether transaction exists
    fn has_already_voted(&self, event_address: &MsgAddrStd) -> bool {
        self.stats_db
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
        configuration_id: &BigUint,
    ) -> Option<Arc<EthEventConfigurationContract>> {
        self.eth_config_contracts
            .read()
            .await
            .get(configuration_id)
            .cloned()
    }

    /// Compute event address based on its data
    async fn get_event_contract_address(
        &self,
        data: &EventTransaction,
    ) -> Result<MsgAddrStd, Error> {
        let (configuration_id, event_init_data) = data.inner().clone().into();

        let config_contract = match self.get_configuration_contract(&configuration_id).await {
            Some(contract) => contract,
            None => {
                return Err(anyhow!(
                    "Unknown event configuration contract: {}",
                    configuration_id
                ));
            }
        };

        let event_addr = config_contract
            .compute_event_address(event_init_data)
            .await?;

        Ok(event_addr)
    }

    /// Sends a message to TON with a small amount of retries on failures.
    /// Can be stopped using `cancel` or `notify_found`
    async fn ensure_sent(self: Arc<Self>, event_address: MsgAddrStd, data: EventTransaction) {
        // Skip voting for events which are already in stats db and TON queue
        if self.has_already_voted(&event_address) {
            // Make sure that TON queue doesn't contain this event
            self.ton_queue
                .mark_complete(&event_address)
                .expect("Fatal db error");
            return;
        }

        // Insert specified data in TON queue, replacing failed transaction if it exists
        self.ton_queue
            .insert_pending(&event_address, &data)
            .await
            .expect("Fatal db error");

        // Start listening for cancellation
        let (rx, vote) = {
            // Get suitable channels map
            let (mut runtime_queue, vote) = match &data {
                EventTransaction::Confirm(_) => {
                    (self.confirmations.lock().await, EventVote::Confirm)
                }
                EventTransaction::Reject(_) => (self.rejections.lock().await, EventVote::Reject),
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
        let mut retries_count = self.timeouts.message_retry_count;
        let mut retries_interval = self.timeouts.message_retry_interval;

        // Send a message with several retries on failure
        let result = loop {
            // Prepare delay future
            let delay = tokio::time::delay_for(retries_interval);
            retries_interval = std::time::Duration::from_secs_f64(
                retries_interval.as_secs_f64() * self.timeouts.message_retry_interval_multiplier,
            );

            // Try to send message
            if let Err(e) = data.send(&self.bridge_contract).await {
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
                            "Failed to get voting event response: timeout reached. Retrying ({} left for {})",
                            retries_count,
                            data.inner().event_transaction
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

            // Check if event arrived nevertheless unsuccessful sending
            if self
                .stats_db
                .has_already_voted(&event_address, &self.relay_key)
                .expect("Fatal db error")
            {
                break Ok(());
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
    async fn notify_found(&self, event_address: &MsgAddrStd, vote: EventVote) {
        let mut table = match bote {
            EventVote::Confirm => self.confirmations.lock().await,
            EventVote::Reject => self.rejections.lock().await,
        };

        if let Some(tx) = table.remove(event_address) {
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

    /// Inserts address into known addresses and returns `true` if it wasn't known before
    async fn ensure_configuration_identity(&self, address: &MsgAddressInt) -> bool {
        self.known_config_addresses
            .lock()
            .await
            .insert(address.clone())
    }

    /// Created listener for TON events
    async fn subscribe_to_ton_events_configuration(
        self: Arc<Self>,
        configuration_id: BigUint,
        address: MsgAddressInt,
        semaphore: Option<Semaphore>,
    ) {
        if !self.ensure_configuration_identity(&address).await {
            try_notify(semaphore).await;
            return;
        }

        // Create TON config contract
        let (config_contract, mut _config_contract_events) = make_ton_event_configuration_contract(
            self.transport.clone(),
            address.clone(),
            self.bridge_contract.address().clone(),
        )
        .await
        .unwrap(); // todo retry subscription

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

        // Register contract
        self.ton_config_contracts
            .write()
            .await
            .insert(configuration_id, config_contract.clone());

        // Start listening swapback events
        let mut swapback_events_contract = match make_ton_swapback_contract(
            self.transport.clone(),
            details.event_address.clone(),
            details.common.event_abi.clone(),
        )
        .await
        {
            Ok(contract) => {
                try_notify(semaphore).await;
                contract
            }
            Err(e) => {
                try_notify(semaphore).await;
                log::error!("{:?}", e);
                return;
            }
        };

        // Start listening swapback events
        tokio::spawn({
            let listener = self.clone();
            async move {
                while let Some(tokens) = swapback_events_contract.next().await {
                    log::debug!("Got swap back event: {:?}", tokens);

                    let event_data = util::ton_tokens_to_ethereum_bytes(tokens);
                    let signature = listener.eth_signer.sign(&event_data);

                    log::debug!(
                        "Calculated swap back event signature: {}",
                        hex::encode(&signature)
                    );

                    // TODO: spawn confirmation
                }
            }
        });

        todo!()
    }

    /// Creates a listener for ETH event votes in TON
    async fn subscribe_to_eth_events_configuration(
        self: Arc<Self>,
        subscriptions_tx: mpsc::UnboundedSender<(Address, H256)>,
        configuration_id: BigUint,
        address: MsgAddressInt,
        semaphore: Option<Semaphore>,
    ) {
        if !self.ensure_configuration_identity(&address).await {
            try_notify(semaphore).await;
            return;
        }

        // Create ETH config contract
        let (config_contract, mut config_contract_events) = make_eth_event_configuration_contract(
            self.transport.clone(),
            address.clone(),
            self.bridge_contract.address().clone(),
        )
        .await
        .unwrap(); //todo retry subscription

        // Get its data
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
        self.eth_config_contracts
            .write()
            .await
            .insert(configuration_id.clone(), config_contract.clone());
        try_notify(semaphore).await;

        // Prefetch required properties
        let ethereum_event_blocks_to_confirm = details
            .event_blocks_to_confirm
            .to_u64()
            .unwrap_or_else(u64::max_value);
        let ethereum_event_address = details.event_address;

        // Store configuration contract details
        self.configs_state
            .write()
            .await
            .set_configuration(configuration_id.clone(), details);

        // Start listening ETH events
        self.notify_subscription(subscriptions_tx, ethereum_event_address)
            .await;

        let handle_event =
            move |listener: Arc<Self>, configuration_id, event, semaphore: Option<Semaphore>| {
                log::debug!("got event confirmation event: {:?}", event);

                let (address, relay_key, vote) = match event {
                    EthEventConfigurationContractEvent::EventConfirmation {
                        address,
                        relay_key,
                    } => (address, relay_key, EventVote::Confirm),
                    EthEventConfigurationContractEvent::EventReject { address, relay_key } => {
                        (address, relay_key, EventVote::Reject)
                    }
                };

                tokio::spawn({
                    async move {
                        listener
                            .handle_eth_event(
                                configuration_id,
                                ethereum_event_blocks_to_confirm,
                                address,
                                relay_key,
                                vote,
                            )
                            .await;
                        if let Some(semaphore) = semaphore {
                            semaphore.notify().await;
                        }
                    }
                });
            };

        // Spawn listener of new events
        tokio::spawn({
            let listener = self.clone();
            let configuration_id = configuration_id.clone();

            async move {
                while let Some(event) = config_contract_events.next().await {
                    handle_event(listener.clone(), configuration_id.clone(), event, None);
                }
            }
        });

        // Process all past events
        let latest_known_lt = config_contract.since_lt();

        let latest_scanned_lt = self
            .stats_db
            .get_latest_scanned_lt(config_contract.address())
            .expect("Fatal db error");

        let events_semaphore = Semaphore::new_empty();

        let mut known_events = config_contract.get_known_events(latest_scanned_lt, latest_known_lt);
        let mut known_event_count = 0;
        while let Some(event) = known_events.next().await {
            handle_event(
                self.clone(),
                configuration_id.clone(),
                event,
                Some(events_semaphore.clone()),
            );
            known_event_count += 1;
        }

        // Only update latest scanned lt when all scanned events are in ton queue
        events_semaphore.wait_count(known_event_count).await;

        self.stats_db
            .update_latest_scanned_lt(config_contract.address(), latest_known_lt)
            .expect("Fatal db error");
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
        configuration_id: BigUint,
        ethereum_event_blocks_to_confirm: u64,
        event_addr: MsgAddrStd,
        relay_key: UInt256,
        vote: EventVote,
    ) {
        let data = match self
            .get_event_details(self.eth_event_contract.as_ref(), &event_addr)
            .await
        {
            Ok(data) => data,
            Err(e) => {
                log::error!("Failed to get event details: {:?}", e);
                return;
            }
        };

        let should_check = vote == EventVote::Confirm
            && data.status == EventStatus::InProcess
            && relay_key != self.relay_key // event from other relay
            && !self.is_in_queue(&event_addr)
            && !self.has_already_voted(&event_addr);

        let event = EthEventReceivedVote {
            configuration_id: &configuration_id,
            vote,
            event_addr: &event_addr,
            relay_key: &relay_key,
            ethereum_event_blocks_to_confirm,
            data: &data,
        };

        log::info!("Received {}, should check: {}", event, should_check);

        self.stats_db
            .eth_event_votes()
            .insert_vote(event)
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

    async fn get_event_configuration_details<T>(
        &self,
        config_contract: &T,
        semaphore: Option<Semaphore>,
    ) -> Result<T::Details, Error>
    where
        T: ConfigurationContract,
    {
        // retry connection to configuration contract
        let mut retry_count = self.timeouts.event_configuration_details_retry_count;
        // 1 sec ~= time before next block in masterchain.
        // Should be greater then account polling interval
        let retry_interval = self.timeouts.event_configuration_details_retry_interval;

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
                Ok(details) => match config_contract.validate(&details) {
                    Ok(_) => break Ok(details),
                    Err(e) => {
                        notify_if_init().await;
                        break Err(e);
                    }
                },
                Err(ContractError::TransportError(TransportError::AccountNotFound))
                    if retry_count > 0 =>
                {
                    retry_count -= 1;
                    log::warn!(
                        "failed to get configuration contract details for {}. Retrying ({} left)",
                        config_contract.address(),
                        retry_count
                    );
                    tokio::time::delay_for(retry_interval).await;
                }
                Err(e) => {
                    notify_if_init().await;
                    break Err(anyhow!(
                        "failed to get configuration contract details: {:?}",
                        e
                    ));
                }
            }
        }
    }

    async fn get_event_details<T>(
        &self,
        event_contract: &T,
        address: &MsgAddrStd,
    ) -> Result<T::Details, ContractError>
    where
        T: EventContract,
    {
        let mut retry_count = self.timeouts.event_details_retry_count;
        let retry_interval = self.timeouts.event_details_retry_interval;

        loop {
            match event_contract.get_details(address).await {
                Ok(details) => break Ok(details),
                Err(ContractError::TransportError(TransportError::AccountNotFound))
                    if retry_count > 0 =>
                {
                    retry_count -= 1;
                    log::error!(
                        "Failed to get event details for {}. Retrying ({} left)",
                        address,
                        retry_count
                    );
                    tokio::time::delay_for(retry_interval).await;
                }
                Err(e) => break Err(e),
            };
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct ConfigsState {
    pub eth_addr: HashSet<Address>,
    pub address_topic_map: HashMap<Address, (H256, Vec<EthParamType>, Vec<TonParamType>)>,
    pub topic_abi_map: HashMap<H256, Vec<EthParamType>>,
    pub eth_configs_map: HashMap<Address, (BigUint, EthEventConfiguration)>,
}

impl ConfigsState {
    fn set_configuration(
        &mut self,
        configuration_id: BigUint,
        configuration: EthEventConfiguration,
    ) {
        if let Err(e) = util::validate_ethereum_event_configuration(&configuration) {
            log::error!("Got bad EthereumEventConfiguration: {:?}", e);
            return;
        }

        let (topic_hash, eth_abi, ton_abi) =
            match util::parse_eth_abi(&configuration.common.event_abi) {
                Ok(a) => a,
                Err(e) => {
                    log::error!("Failed parsing abi: {:?}", e);
                    return;
                }
            };

        self.eth_addr.insert(configuration.event_address);
        self.address_topic_map.insert(
            configuration.event_address,
            (topic_hash, eth_abi.clone(), ton_abi),
        );
        self.topic_abi_map.insert(topic_hash, eth_abi);
        self.eth_configs_map.insert(
            configuration.event_address,
            (configuration_id, configuration),
        );
    }
}

#[async_trait]
trait Sendable {
    async fn send(self, bridge: &BridgeContract) -> ContractResult<()>;
}

#[async_trait]
impl Sendable for EthEventTransaction {
    async fn send(self, bridge: &BridgeContract) -> ContractResult<()> {
        let (confirmation, vote) = match self {
            EventTransaction::Confirm(data) => (data, Voting::Confirm),
            EventTransaction::Reject(data) => (data, Voting::Reject),
        };
        let (id, data): (BigUint, EthEventInitData) = confirmation.into();
        match vote {
            Voting::Confirm => bridge.confirm_ethereum_event(id, data).await,
            Voting::Reject => bridge.reject_ethereum_event(id, data).await,
        }
    }
}

#[async_trait]
impl Sendable for TonEventTransaction {
    async fn send(self, bridge: &BridgeContract) -> ContractResult<()> {
        match self {
            EventTransaction::Confirm(SignedEventVotingData { data, signature }) => {
                let (id, data) = data.into();
                bridge.confirm_ton_event(id, data, signature).await
            }
            EventTransaction::Reject(confirmation) => {
                let (id, data) = confirmation.into();
                bridge.reject_ton_event(id, data).await
            }
        }
    }
}

#[async_trait]
trait EventContract {
    type Details;

    async fn get_details(&self, address: &MsgAddrStd) -> ContractResult<Self::Details>;
}

#[async_trait]
impl EventContract for EthEventContract {
    type Details = EthEventDetails;

    async fn get_details(&self, address: &MsgAddrStd) -> ContractResult<Self::Details> {
        self.get_details(address.clone()).await
    }
}

#[async_trait]
impl EventContract for TonEventContract {
    type Details = TonEventDetails;

    async fn get_details(&self, address: &MsgAddrStd) -> ContractResult<Self::Details> {
        self.get_details(address.clone()).await
    }
}

#[async_trait]
trait ConfigurationContract {
    type Details;

    fn address(&self) -> &MsgAddressInt;
    fn validate(&self, details: &Self::Details) -> Result<(), Error>;
    async fn get_details(&self) -> ContractResult<Self::Details>;
}

#[async_trait]
impl ConfigurationContract for EthEventConfigurationContract {
    type Details = EthEventConfiguration;

    fn address(&self) -> &MsgAddressInt {
        self.address()
    }

    fn validate(&self, details: &Self::Details) -> Result<(), Error> {
        util::validate_ethereum_event_configuration(details)
    }

    async fn get_details(&self) -> ContractResult<Self::Details> {
        self.get_details().await
    }
}

#[async_trait]
impl ConfigurationContract for TonEventConfigurationContract {
    type Details = TonEventConfiguration;

    fn address(&self) -> &MsgAddressInt {
        self.address()
    }

    fn validate(&self, config: &Self::Details) -> Result<(), Error> {
        let _ = serde_json::from_str::<SwapBackEventAbi>(&config.common.event_abi)
            .map_err(|e| Error::new(e).context("Bad SwapBack event ABI"))?;
        Ok(())
    }

    async fn get_details(&self) -> ContractResult<Self::Details> {
        self.get_details().await
    }
}

async fn try_notify(semaphore: Option<Semaphore>) {
    if let Some(semaphore) = semaphore {
        semaphore.notify().await;
    }
}

#[derive(Clone)]
struct Semaphore {
    guard: Arc<RwLock<SemaphoreGuard>>,
    counter: Arc<Mutex<usize>>,
    done: Arc<Notify>,
}

impl Semaphore {
    fn new(count: usize) -> Self {
        Self {
            guard: Arc::new(RwLock::new(SemaphoreGuard {
                target: count,
                allow_notify: false,
            })),
            counter: Arc::new(Mutex::new(count)),
            done: Arc::new(Notify::new()),
        }
    }

    fn new_empty() -> Self {
        Self::new(0)
    }

    async fn wait(self) {
        let target = {
            let mut guard = self.guard.write().await;
            guard.allow_notify = true;
            guard.target
        };
        if *self.counter.lock().await >= target {
            return;
        }
        self.done.notified().await
    }

    async fn wait_count(self, count: usize) {
        *self.guard.write().await = SemaphoreGuard {
            target: count,
            allow_notify: true,
        };
        if *self.counter.lock().await >= count {
            return;
        }
        self.done.notified().await
    }

    async fn notify(&self) {
        let mut counter = self.counter.lock().await;
        *counter += 1;

        let guard = self.guard.read().await;
        if *counter >= guard.target && guard.allow_notify {
            self.done.notify();
        }
    }
}

struct SemaphoreGuard {
    target: usize,
    allow_notify: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    use tokio::time;
    use tokio::time::Duration;

    fn spawn_tasks(semaphore: Semaphore) -> usize {
        let mut target = 0;

        for i in 0..10 {
            target += 1;
            tokio::spawn({
                let semaphore = semaphore.clone();
                async move {
                    println!("Task {} started", i);
                    time::delay_for(Duration::from_secs(i)).await;
                    println!("Task {} complete", i);
                    semaphore.notify().await;
                }
            });
        }

        target
    }

    #[tokio::test]
    async fn semaphore_with_unknown_target_incomplete() {
        let semaphore = Semaphore::new_empty();
        let target_count = spawn_tasks(semaphore.clone());

        time::delay_for(Duration::from_secs(7)).await;
        println!("Waiting...");
        semaphore.wait_count(target_count).await;
        println!("Done");
    }

    #[tokio::test]
    async fn semaphore_with_unknown_target_complete() {
        let semaphore = Semaphore::new_empty();
        let target_count = spawn_tasks(semaphore.clone());

        time::delay_for(Duration::from_secs(11)).await;
        println!("Waiting...");
        semaphore.wait_count(target_count).await;
        println!("Done");
    }
}
