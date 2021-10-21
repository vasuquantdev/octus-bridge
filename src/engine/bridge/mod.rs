use std::collections::hash_map;
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use eth_ton_abi_converter::*;
use nekoton_abi::*;
use tiny_adnl::utils::*;
use tokio::sync::mpsc;
use tokio::sync::RwLock;
use ton_block::{Deserializable, HashmapAugType};
use ton_types::UInt256;

use crate::engine::eth_subscriber::*;
use crate::engine::keystore::*;
use crate::engine::ton_contracts::*;
use crate::engine::ton_subscriber::*;
use crate::engine::EngineContext;
use crate::utils::*;

/// Events part of relays logic
pub struct Bridge {
    /// Shared engine context
    context: Arc<EngineContext>,

    /// Bridge contract address
    bridge_account: UInt256,
    /// Bridge events listener
    bridge_observer: Arc<AccountObserver<BridgeEvent>>,
    /// Known contracts
    state: RwLock<BridgeState>,

    // Observers for pending ETH events
    eth_events_state: Arc<EventsState<EthEvent>>,

    // Observers for pending TON events
    ton_events_state: Arc<EventsState<TonEvent>>,

    connectors_tx: AccountEventsTx<ConnectorEvent>,
    eth_event_configurations_tx: AccountEventsTx<EthEventConfigurationEvent>,
    ton_event_configurations_tx: AccountEventsTx<TonEventConfigurationEvent>,
}

impl Bridge {
    pub async fn new(context: Arc<EngineContext>, bridge_account: UInt256) -> Result<Arc<Self>> {
        // Create bridge
        let (bridge_events_tx, bridge_events_rx) = mpsc::unbounded_channel();
        let (connectors_tx, connectors_rx) = mpsc::unbounded_channel();
        let (eth_event_configurations_tx, eth_event_configurations_rx) = mpsc::unbounded_channel();
        let (ton_event_configurations_tx, ton_event_configurations_rx) = mpsc::unbounded_channel();
        let (eth_events_tx, eth_events_rx) = mpsc::unbounded_channel();
        let (ton_events_tx, ton_events_rx) = mpsc::unbounded_channel();

        let bridge_observer = AccountObserver::new(&bridge_events_tx);

        let bridge = Arc::new(Bridge {
            context,
            bridge_account,
            bridge_observer: bridge_observer.clone(),
            state: Default::default(),
            eth_events_state: EventsState::new(eth_events_tx),
            ton_events_state: EventsState::new(ton_events_tx),
            connectors_tx,
            eth_event_configurations_tx,
            ton_event_configurations_tx,
        });

        // Prepare listeners
        start_listening_events(
            &bridge,
            "BridgeContract",
            bridge_events_rx,
            Self::process_bridge_event,
        );

        start_listening_events(
            &bridge,
            "ConnectorContract",
            connectors_rx,
            Self::process_connector_event,
        );

        start_listening_events(
            &bridge,
            "EthEventConfigurationContract",
            eth_event_configurations_rx,
            Self::process_eth_event_configuration_event,
        );

        start_listening_events(
            &bridge,
            "TonEventConfigurationContract",
            ton_event_configurations_rx,
            Self::process_ton_event_configuration_event,
        );

        start_listening_events(
            &bridge,
            "EthEventContract",
            eth_events_rx,
            Self::process_eth_event,
        );
        start_listening_events(
            &bridge,
            "TonEventContract",
            ton_events_rx,
            Self::process_ton_event,
        );

        // Subscribe bridge account to transactions
        bridge
            .context
            .ton_subscriber
            .add_transactions_subscription([bridge.bridge_account], &bridge.bridge_observer);

        // Initialize
        bridge.get_all_configurations().await?;
        bridge.get_all_events().await?;

        bridge.start_ton_event_configurations_gc();

        Ok(bridge)
    }

    pub fn metrics(&self) -> BridgeMetrics {
        BridgeMetrics {
            pending_eth_event_count: self.eth_events_state.count.load(Ordering::Acquire),
            pending_ton_event_count: self.ton_events_state.count.load(Ordering::Acquire),
        }
    }

    async fn process_bridge_event(
        self: Arc<Self>,
        (_, event): (UInt256, BridgeEvent),
    ) -> Result<()> {
        match event {
            BridgeEvent::ConnectorDeployed(event) => {
                // Create connector entry if it wasn't already created
                match self.state.write().await.connectors.entry(event.connector) {
                    hash_map::Entry::Vacant(entry) => {
                        // Create observer
                        let observer = AccountObserver::new(&self.connectors_tx);

                        let entry = entry.insert(observer);

                        // Subscribe observer to transactions
                        self.context
                            .ton_subscriber
                            .add_transactions_subscription([event.connector], entry);
                    }
                    hash_map::Entry::Occupied(_) => {
                        log::error!(
                            "Got connector deployment event but it already exists: {:x}",
                            event.connector
                        );
                        return Ok(());
                    }
                };

                // Check connector contract if it was added in this iteration
                tokio::spawn(async move {
                    if let Err(e) = self.check_connector_contract(event.connector).await {
                        log::error!("Failed to check connector contract: {:?}", e);
                    }
                });
            }
        }

        Ok(())
    }

    async fn process_connector_event(
        self: Arc<Self>,
        (connector, event): (UInt256, ConnectorEvent),
    ) -> Result<()> {
        match event {
            ConnectorEvent::Enable => self.check_connector_contract(connector).await,
        }
    }

    async fn process_eth_event_configuration_event(
        self: Arc<Self>,
        (account, event): (UInt256, EthEventConfigurationEvent),
    ) -> Result<()> {
        match event {
            // Create observer on each deployment event
            EthEventConfigurationEvent::EventDeployed { address } => {
                if self.add_pending_event(address, &self.eth_events_state) {
                    let this = self.clone();
                    self.spawn_background_task("preprocess ETH event", async move {
                        this.preprocess_event(address, &this.eth_events_state).await
                    });
                }
            }
            // Update configuration state
            EthEventConfigurationEvent::SetEndBlockNumber { end_block_number } => {
                let mut state = self.state.write().await;
                let configuration = state
                    .eth_event_configurations
                    .get_mut(&account)
                    .ok_or(BridgeError::UnknownConfiguration)?;
                configuration.details.network_configuration.end_block_number = end_block_number;
            }
        }
        Ok(())
    }

    async fn process_ton_event_configuration_event(
        self: Arc<Self>,
        (account, event): (UInt256, TonEventConfigurationEvent),
    ) -> Result<()> {
        match event {
            // Create observer on each deployment event
            TonEventConfigurationEvent::EventDeployed { address, .. } => {
                if self.add_pending_event(address, &self.ton_events_state) {
                    let this = self.clone();
                    self.spawn_background_task("preprocess TON event", async move {
                        this.preprocess_event(address, &this.ton_events_state).await
                    });
                } else {
                    // NOTE: Each TON event must be unique on the contracts level,
                    // so receiving message with duplicated address is
                    // a signal the something went wrong
                    log::warn!("Got deployment message for pending event: {:x}", account);
                }
            }
            // Update configuration state
            TonEventConfigurationEvent::SetEndTimestamp { end_timestamp } => {
                let mut state = self.state.write().await;
                let configuration = state
                    .ton_event_configurations
                    .get_mut(&account)
                    .ok_or(BridgeError::UnknownConfiguration)?;
                configuration.details.network_configuration.end_timestamp = end_timestamp;
            }
        }
        Ok(())
    }

    async fn process_eth_event(
        self: Arc<Self>,
        (account, event): (UInt256, EthEvent),
    ) -> Result<()> {
        use dashmap::mapref::entry::Entry;

        let our_public_key = self.context.keystore.ton.public_key();

        // Use flag to update counter outside events map lock to reduce its duration
        let mut event_removed = false;

        // Handle only known ETH events
        if let Entry::Occupied(entry) = self.eth_events_state.pending.entry(account) {
            match event {
                // Handle event initialization
                EthEvent::ReceiveRoundRelays { keys, status } => {
                    // Check if event contains our key
                    if status == EventStatus::Pending && keys.contains(our_public_key) {
                        // Start voting
                        self.spawn_background_task(
                            "update ETH event",
                            self.clone().update_eth_event(account),
                        );
                    } else {
                        entry.remove();
                        event_removed = true;
                    }
                }
                // Handle our confirmation or rejection
                EthEvent::Confirm { public_key, status }
                | EthEvent::Reject { public_key, status }
                    if status != EventStatus::Pending || public_key == our_public_key =>
                {
                    // Remove pending event
                    entry.remove();
                    event_removed = true;
                }
                _ => { /* Ignore other events */ }
            }
        }

        // Update metrics
        if event_removed {
            self.eth_events_state.count.fetch_sub(1, Ordering::Release);
        }

        Ok(())
    }

    async fn process_ton_event(
        self: Arc<Self>,
        (account, event): (UInt256, TonEvent),
    ) -> Result<()> {
        use dashmap::mapref::entry::Entry;

        let our_public_key = self.context.keystore.ton.public_key();

        // Use flag to update counter outside events map lock to reduce its duration
        let mut event_removed = false;

        // Handle only known TON events
        if let Entry::Occupied(entry) = self.ton_events_state.pending.entry(account) {
            match event {
                // Handle event initialization
                TonEvent::ReceiveRoundRelays { keys, status } => {
                    // Check if event contains our key
                    if status == EventStatus::Pending && keys.contains(our_public_key) {
                        // Start voting
                        self.spawn_background_task(
                            "update TON event",
                            self.clone().update_ton_event(account),
                        );
                    } else {
                        entry.remove();
                        event_removed = true;
                    }
                }
                // Handle our confirmation or rejection
                TonEvent::Confirm { public_key, status }
                | TonEvent::Reject { public_key, status }
                    if status != EventStatus::Pending || public_key == our_public_key =>
                {
                    entry.remove();
                    event_removed = true;
                }
                _ => { /* Ignore other events */ }
            }
        }

        // Update metrics
        if event_removed {
            self.ton_events_state.count.fetch_sub(1, Ordering::Release);
        }

        Ok(())
    }

    /// Check deployed event contract in parallel with transactions processing
    async fn preprocess_event<T: EventExt>(
        self: &Arc<Bridge>,
        account: UInt256,
        state: &EventsState<T>,
    ) -> Result<()> {
        // Wait contract state
        let ton_subscriber = &self.context.ton_subscriber;
        let contract = ton_subscriber.wait_contract_state(account).await?;
        let base_event_contract = EventBaseContract(&contract);

        // Check further steps based on event statuses
        match base_event_contract.process(self.context.keystore.ton.public_key())? {
            // Event was not activated yet, so it will be processed in
            // event transactions subscription
            EventAction::Nop => Ok(()),
            // Event was already processed, so just remove it
            // NOTE: it is ok to remove it even if it didn't exist
            EventAction::Remove => {
                state.remove(&account);
                Ok(())
            }
            // Start processing event.
            // NOTE: it is ok to update_ton_event twice because in fact it will
            // do anything only once
            EventAction::Vote => T::update_event(self.clone(), account).await,
        }
    }

    async fn update_eth_event(self: Arc<Self>, account: UInt256) -> Result<()> {
        if !self.eth_events_state.start_processing(&account) {
            return Ok(());
        }

        let keystore = &self.context.keystore;
        let ton_subscriber = &self.context.ton_subscriber;
        let eth_subscribers = &self.context.eth_subscribers;

        // Wait contract state
        let contract = ton_subscriber.wait_contract_state(account).await?;

        match EventBaseContract(&contract).process(keystore.ton.public_key())? {
            EventAction::Nop => return Ok(()),
            EventAction::Remove => {
                self.eth_events_state.remove(&account);
                return Ok(());
            }
            EventAction::Vote => { /* continue voting */ }
        }

        let event_init_data = EthEventContract(&contract).event_init_data()?;

        // Get event configuration data
        let data = {
            let state = self.state.read().await;
            state
                .eth_event_configurations
                .get(&event_init_data.configuration)
                .map(|configuration| {
                    (
                        configuration.details.network_configuration.chain_id,
                        configuration.event_abi.clone(),
                        configuration
                            .details
                            .network_configuration
                            .event_blocks_to_confirm,
                    )
                })
        };

        // NOTE: be sure to drop `eth_event_configurations` lock before that
        let (eth_subscriber, event_abi, blocks_to_confirm) = match data {
            // Configuration found
            Some((chain_id, abi, blocks_to_confirm)) => {
                // Get required subscriber
                match eth_subscribers.get_subscriber(chain_id) {
                    Some(subscriber) => (subscriber, abi, blocks_to_confirm),
                    None => {
                        log::error!(
                            "ETH subscriber with chain id  {} was not found for event {:x}",
                            chain_id,
                            account
                        );
                        self.eth_events_state.remove(&account);
                        return Ok(());
                    }
                }
            }
            // Configuration not found
            None => {
                log::error!(
                    "ETH event configuration {:x} not found for event {:x}",
                    event_init_data.configuration,
                    account
                );
                self.eth_events_state.remove(&account);
                return Ok(());
            }
        };

        // Verify ETH event and create message to event contract
        let message = match eth_subscriber
            .verify(event_init_data.vote_data, event_abi, blocks_to_confirm)
            .await
        {
            // Confirm event if transaction was found
            Ok(VerificationStatus::Exists) => {
                UnsignedMessage::new(eth_event_contract::confirm(), account)
            }
            // Reject event if transaction not found
            Ok(VerificationStatus::NotExists) => {
                UnsignedMessage::new(eth_event_contract::reject(), account)
            }
            // Skip event otherwise
            Err(e) => {
                log::error!("Failed to verify ETH event {:x}: {:?}", account, e);
                self.eth_events_state.remove(&account);
                return Ok(());
            }
        };

        // Clone events observer and deliver message to the contract
        let eth_event_observer = match self.eth_events_state.pending.get(&account) {
            Some(entry) => entry.observer.clone(),
            None => return Ok(()),
        };
        let eth_events_state = Arc::downgrade(&self.eth_events_state);

        self.context
            .deliver_message(
                eth_event_observer,
                message,
                // Stop voting for the contract if it was removed
                move || match eth_events_state.upgrade() {
                    Some(state) => state.pending.contains_key(&account),
                    None => false,
                },
            )
            .await?;
        Ok(())
    }

    async fn update_ton_event(self: Arc<Self>, account: UInt256) -> Result<()> {
        if !self.ton_events_state.start_processing(&account) {
            return Ok(());
        }

        let keystore = &self.context.keystore;
        let ton_subscriber = &self.context.ton_subscriber;

        // Wait contract state
        let contract = ton_subscriber.wait_contract_state(account).await?;
        let base_event_contract = EventBaseContract(&contract);

        // Check further steps based on event statuses
        match base_event_contract.process(keystore.ton.public_key())? {
            EventAction::Nop => return Ok(()),
            EventAction::Remove => {
                self.ton_events_state.remove(&account);
                return Ok(());
            }
            EventAction::Vote => { /* continue voting */ }
        }
        let round_number = base_event_contract.round_number()?;

        // Get event details
        let event_init_data = TonEventContract(&contract).event_init_data()?;

        // Find suitable configuration
        // NOTE: be sure to drop `self.state` lock before removing pending ton event.
        // It may deadlock otherwise!
        let data = {
            let state = self.state.read().await;
            state
                .ton_event_configurations
                .get(&event_init_data.configuration)
                .map(|configuration| {
                    (
                        configuration.details.network_configuration.proxy,
                        ton_abi::TokenValue::decode_params(
                            &configuration.event_abi,
                            event_init_data.vote_data.event_data.clone().into(),
                            2,
                        ),
                    )
                })
        };

        let decoded_data = match data {
            // Decode event data with event abi from configuration
            Some((proxy, data)) => data.and_then(|data| {
                Ok(make_mapped_ton_event(
                    event_init_data.vote_data.event_transaction_lt,
                    event_init_data.vote_data.event_timestamp,
                    map_ton_tokens_to_eth_bytes(data)?,
                    event_init_data.configuration,
                    account,
                    proxy,
                    round_number,
                ))
            }),
            // Do nothing when configuration was not found
            None => {
                log::error!(
                    "TON event configuration {:x} not found for event {:x}",
                    event_init_data.configuration,
                    account
                );
                self.ton_events_state.remove(&account);
                return Ok(());
            }
        };

        let message = match decoded_data {
            // Confirm with signature
            Ok(data) => {
                log::info!("Signing event data: {}", hex::encode(&data));
                UnsignedMessage::new(ton_event_contract::confirm(), account)
                    .arg(keystore.eth.sign(&data).to_vec())
            }

            // Reject if event data is invalid
            Err(e) => {
                log::warn!(
                    "Failed to compute vote data signature for {:x}: {:?}",
                    account,
                    e
                );
                UnsignedMessage::new(ton_event_contract::reject(), account)
            }
        };

        // Clone events observer and deliver message to the contract
        let ton_event_observer = match self.ton_events_state.pending.get(&account) {
            Some(entry) => entry.observer.clone(),
            None => return Ok(()),
        };
        let ton_events_state = Arc::downgrade(&self.ton_events_state);

        self.context
            .deliver_message(
                ton_event_observer,
                message,
                // Stop voting for the contract if it was removed
                move || match ton_events_state.upgrade() {
                    Some(state) => state.pending.contains_key(&account),
                    None => false,
                },
            )
            .await?;
        Ok(())
    }

    async fn check_connector_contract(&self, connector_account: UInt256) -> Result<()> {
        let ton_subscriber = &self.context.ton_subscriber;

        // Get event configuration address
        let event_configuration = {
            // Wait until connector contract state is found
            let contract = ton_subscriber
                .wait_contract_state(connector_account)
                .await?;

            // Extract details
            let connector_details = ConnectorContract(&contract).get_details()?;
            log::info!("Got connector details: {:?}", connector_details);

            // Do nothing if it is disabled
            if !connector_details.enabled {
                return Ok(());
            }
            connector_details.event_configuration
        };

        // Wait until event configuration state is found
        let contract = ton_subscriber
            .wait_contract_state(event_configuration)
            .await?;
        log::info!("Got configuration contract");

        // Extract and process info from contract
        let mut state = self.state.write().await;
        self.process_event_configuration(
            &mut *state,
            &connector_account,
            &event_configuration,
            &contract,
        )?;

        Ok(())
    }

    async fn get_all_configurations(&self) -> Result<()> {
        // Lock state before other logic to make sure that all events
        // will be queued in their handlers
        let mut state = self.state.write().await;

        let shard_accounts = self.context.get_all_shard_accounts().await?;

        let ton_subscriber = &self.context.ton_subscriber;

        let contract = shard_accounts
            .find_account(&self.bridge_account)?
            .ok_or(BridgeError::BridgeAccountNotFound)?;
        let bridge = BridgeContract(&contract);

        let connector_count = bridge
            .connector_counter()
            .context("Failed to get connector count")?;

        // Iterate for all connectors
        for id in 0..connector_count {
            // Compute next connector address
            let connector_account = bridge
                .derive_connector_address(id)
                .context("Failed to derive connector address")?;

            // Extract details from contract
            let details = match shard_accounts.find_account(&connector_account)? {
                Some(contract) => match ConnectorContract(&contract).get_details() {
                    Ok(details) => details,
                    Err(e) => {
                        log::error!(
                            "Failed to get connector details {:x}: {:?}",
                            connector_account,
                            e
                        );
                        continue;
                    }
                },
                None => {
                    log::error!("Connector not found: {:x}", connector_account);
                    continue;
                }
            };
            log::info!("Found configuration connector {}: {:?}", id, details);

            let enabled = details.enabled;
            let configuration_account = details.event_configuration;

            let observer = AccountObserver::new(&self.connectors_tx);

            // Add new connector
            state.connectors.insert(connector_account, observer.clone());

            // Subscribe connector for transaction
            ton_subscriber.add_transactions_subscription([connector_account], &observer);

            // Skip event configuration if it is disabled
            if !enabled {
                continue;
            }

            // Find event configuration contract
            let configuration_contract =
                match shard_accounts.find_account(&configuration_account)? {
                    Some(contract) => contract,
                    None => {
                        // It is a strange situation when connector contains an address of the contract
                        // which doesn't exist, so log it here to investigate it later
                        log::warn!(
                            "Connected configuration was not found: {:x}",
                            details.event_configuration
                        );
                        continue;
                    }
                };

            // Add event configuration
            if let Err(e) = self.process_event_configuration(
                &mut state,
                &connector_account,
                &configuration_account,
                &configuration_contract,
            ) {
                log::error!(
                    "Failed to process event configuration {:x}: {:?}",
                    details.event_configuration,
                    e
                );
            }
        }

        // Done
        Ok(())
    }

    /// Searches for the account contract and extracts the configuration information into context
    fn process_event_configuration(
        &self,
        state: &mut BridgeState,
        connector_account: &UInt256,
        configuration_account: &UInt256,
        configuration_contract: &ExistingContract,
    ) -> Result<()> {
        // Get event type using base contract abi
        let event_type = EventConfigurationBaseContract(configuration_contract)
            .get_type()
            .context("Failed to get event configuration type")?;
        log::info!("Found configuration of type: {}", event_type);

        if !state.connectors.contains_key(connector_account) {
            return Err(BridgeError::UnknownConnector.into());
        }

        match event_type {
            // Extract and populate ETH event configuration details
            EventType::Eth => self
                .add_eth_event_configuration(state, configuration_account, configuration_contract)
                .context("Failed to add ETH event configuration")?,
            // Extract and populate TON event configuration details
            EventType::Ton => self
                .add_ton_event_configuration(state, configuration_account, configuration_contract)
                .context("Failed to add TON event configuration")?,
        };

        // Done
        Ok(())
    }

    fn add_eth_event_configuration(
        &self,
        state: &mut BridgeState,
        account: &UInt256,
        contract: &ExistingContract,
    ) -> Result<()> {
        // Get configuration details
        let details = EthEventConfigurationContract(contract)
            .get_details()
            .context("Failed to get ETH event configuration details")?;

        // Verify and prepare abi
        let event_abi = Arc::new(EthEventAbi::new(&details.basic_configuration.event_abi)?);
        let topic_hash = event_abi.get_eth_topic_hash();
        let eth_contract_address = details.network_configuration.event_emitter;

        // Get suitable ETH subscriber for specified chain id
        let eth_subscriber = self
            .context
            .eth_subscribers
            .get_subscriber(details.network_configuration.chain_id)
            .ok_or(BridgeError::UnknownChainId)?;

        // Add unique event hash
        add_event_code_hash(
            &mut state.event_code_hashes,
            &details.basic_configuration.event_code,
            EventType::Eth,
        )?;

        // Add configuration entry
        let observer = AccountObserver::new(&self.eth_event_configurations_tx);
        match state.eth_event_configurations.entry(*account) {
            hash_map::Entry::Vacant(entry) => {
                log::info!("Added new ETH event configuration: {:?}", details);

                entry.insert(EthEventConfigurationState {
                    details,
                    event_abi,
                    topic_hash,
                    _observer: observer.clone(),
                });
            }
            hash_map::Entry::Occupied(_) => {
                log::info!("ETH event configuration already exists: {:x}", account);
                return Err(BridgeError::EventConfigurationAlreadyExists.into());
            }
        };

        // Subscribe to ETH events
        eth_subscriber.subscribe(eth_contract_address.into(), topic_hash, *account);

        // Subscribe to TON events
        self.context
            .ton_subscriber
            .add_transactions_subscription([*account], &observer);

        // Done
        Ok(())
    }

    fn add_ton_event_configuration(
        &self,
        state: &mut BridgeState,
        account: &UInt256,
        contract: &ExistingContract,
    ) -> Result<()> {
        // Get configuration details
        let details = TonEventConfigurationContract(contract)
            .get_details()
            .context("Failed to get TON event configuration details")?;

        // Check if configuration is expired
        let current_timestamp = self.context.ton_subscriber.current_utime();
        if details.is_expired(current_timestamp) {
            // Do nothing in that case
            log::warn!(
                "Ignoring TON event configuration {:x}: end timestamp {} is less then current {}",
                account,
                details.network_configuration.end_timestamp,
                current_timestamp
            );
            return Ok(());
        };

        // Verify and prepare abi
        let event_abi = decode_ton_event_abi(&details.basic_configuration.event_abi)?;

        // Add unique event hash
        add_event_code_hash(
            &mut state.event_code_hashes,
            &details.basic_configuration.event_code,
            EventType::Ton,
        )?;

        // Add configuration entry
        let observer = AccountObserver::new(&self.ton_event_configurations_tx);
        match state.ton_event_configurations.entry(*account) {
            hash_map::Entry::Vacant(entry) => {
                log::info!("Added new TON event configuration: {:?}", details);

                entry.insert(TonEventConfigurationState {
                    details,
                    event_abi,
                    _observer: observer.clone(),
                });
            }
            hash_map::Entry::Occupied(_) => {
                log::info!("TON event configuration already exists: {:x}", account);
                return Err(BridgeError::EventConfigurationAlreadyExists.into());
            }
        };

        // Subscribe to TON events
        self.context
            .ton_subscriber
            .add_transactions_subscription([*account], &observer);

        // Done
        Ok(())
    }

    async fn get_all_events(self: &Arc<Self>) -> Result<()> {
        type AccountsSet = FxHashSet<UInt256>;

        fn iterate_events(
            bridge: Arc<Bridge>,
            accounts: ton_block::ShardAccounts,
            event_code_hashes: Arc<EventCodeHashesMap>,
            unique_eth_event_configurations: Arc<AccountsSet>,
            unique_ton_event_configurations: Arc<AccountsSet>,
        ) -> Result<bool> {
            let our_public_key = bridge.context.keystore.ton.public_key();

            accounts.iterate_with_keys(|hash, shard_account| {
                // Prefetch only contract code hash
                let code_hash = match read_code_hash(&mut shard_account.account_cell().into())? {
                    Some(code_hash) => code_hash,
                    None => return Ok(true),
                };

                // Filter only known event contracts
                let event_type = match event_code_hashes.get(&code_hash) {
                    Some(event_type) => event_type,
                    None => return Ok(true),
                };

                // Read account from shard state
                let account = match shard_account.read_account()? {
                    ton_block::Account::Account(account) => account,
                    ton_block::Account::AccountNone => return Ok(true),
                };

                log::info!("FOUND EVENT {:?}: {:x}", event_type, hash);

                // Extract data
                let contract = ExistingContract {
                    account,
                    last_transaction_id: LastTransactionId::Exact(TransactionId {
                        lt: shard_account.last_trans_lt(),
                        hash: *shard_account.last_trans_hash(),
                    }),
                };

                macro_rules! check_configuration {
                    ($name: literal, $contract: ident) => {
                        match $contract(&contract).event_init_data() {
                            Ok(init_data) => init_data.configuration,
                            Err(e) => {
                                log::info!("Failed to get {} event init data: {:?}", $name, e);
                                return Ok(true);
                            }
                        }
                    };
                }

                // Process event
                match EventBaseContract(&contract).process(our_public_key) {
                    Ok(EventAction::Nop | EventAction::Vote) => match event_type {
                        EventType::Eth => {
                            let configuration = check_configuration!("ETH", EthEventContract);

                            if !unique_eth_event_configurations.contains(&configuration) {
                                log::warn!("ETH event configuration not found: {:x}", hash);
                                return Ok(true);
                            }

                            if bridge.add_pending_event(hash, &bridge.eth_events_state) {
                                bridge.spawn_background_task(
                                    "initial update ETH event",
                                    bridge.clone().update_eth_event(hash),
                                );
                            }
                        }
                        EventType::Ton => {
                            let configuration = check_configuration!("TON", TonEventContract);

                            if !unique_ton_event_configurations.contains(&configuration) {
                                log::warn!("TON event configuration not found: {:x}", hash);
                                return Ok(true);
                            }

                            if bridge.add_pending_event(hash, &bridge.ton_events_state) {
                                bridge.spawn_background_task(
                                    "initial update TON event",
                                    bridge.clone().update_ton_event(hash),
                                );
                            }
                        }
                    },
                    Ok(EventAction::Remove) => { /* do nothing */ }
                    Err(e) => {
                        log::error!("Failed to get {} event details: {:?}", event_type, e);
                    }
                }

                Ok(true)
            })
        }

        // Wait all accounts
        let shard_accounts = self.context.get_all_shard_accounts().await?;

        let start = std::time::Instant::now();

        // Lock state to prevent adding new configurations
        let state = self.state.read().await;

        // Prepare shard task context
        let event_code_hashes = Arc::new(state.event_code_hashes.clone());

        // NOTE: configuration sets are explicitly constructed from state instead of
        // just using [eth/ton]_event_counters. It is done on purpose to use the actual
        // configurations. It is acceptable that event counters will not be relevant
        let unique_eth_event_configurations = Arc::new(state.unique_eth_event_configurations());
        let unique_ton_event_configurations = Arc::new(state.unique_ton_event_configurations());

        // Process shards in parallel
        log::info!("Started searching for all events...");
        let mut results_rx = {
            let (results_tx, results_rx) = mpsc::unbounded_channel();

            for (shard_ident, accounts) in shard_accounts {
                let bridge = self.clone();
                let event_code_hashes = event_code_hashes.clone();
                let unique_eth_event_configurations = unique_eth_event_configurations.clone();
                let unique_ton_event_configurations = unique_ton_event_configurations.clone();
                let results_tx = results_tx.clone();

                tokio::spawn(tokio::task::spawn_blocking(move || {
                    let start = std::time::Instant::now();
                    let result = iterate_events(
                        bridge,
                        accounts,
                        event_code_hashes,
                        unique_eth_event_configurations,
                        unique_ton_event_configurations,
                    );
                    log::info!(
                        "Processed accounts in shard {} in {} seconds",
                        shard_ident.shard_prefix_as_str_with_tag(),
                        start.elapsed().as_secs()
                    );
                    results_tx.send(result).ok();
                }));
            }

            results_rx
        };

        // Wait until all shards are processed
        while let Some(result) = results_rx.recv().await {
            if let Err(e) = result {
                return Err(e).context("Failed to find all events");
            }
        }

        // Done
        log::info!(
            "Finished iterating all events in {} seconds",
            start.elapsed().as_secs()
        );
        Ok(())
    }

    fn start_ton_event_configurations_gc(self: &Arc<Self>) {
        let bridge = Arc::downgrade(self);

        tokio::spawn(async move {
            'outer: loop {
                tokio::time::sleep(Duration::from_secs(10)).await;

                // Get bridge if it is still alive
                let bridge = match bridge.upgrade() {
                    Some(bridge) => bridge,
                    None => return,
                };
                let ton_subscriber = &bridge.context.ton_subscriber;
                let ton_engine = &bridge.context.ton_engine;

                // Get current time from masterchain
                let current_utime = ton_subscriber.current_utime();

                // Check expired configurations
                let has_expired_configurations = {
                    let state = bridge.state.read().await;
                    state.has_expired_ton_event_configurations(current_utime)
                };

                // Do nothing if there are not expired configurations
                if !has_expired_configurations {
                    continue;
                }

                // Wait all shards
                let current_utime = match ton_subscriber.wait_shards(None).await {
                    Ok(shards) => {
                        for (_, block_id) in shards.block_ids {
                            if let Err(e) = ton_engine.wait_state(&block_id, None, false).await {
                                log::error!("Failed to wait shard state: {:?}", e);
                                continue 'outer;
                            }
                        }
                        shards.current_utime
                    }
                    Err(e) => {
                        log::error!("Failed to wait current shards info: {:?}", e);
                        continue;
                    }
                };

                // Remove all expired configurations
                let mut state = bridge.state.write().await;
                state.ton_event_configurations.retain(|account, state| {
                    if state.details.is_expired(current_utime) {
                        log::warn!("Removing TON event configuration {:x}", account);
                        false
                    } else {
                        true
                    }
                });
            }
        });
    }

    /// Creates ETH event observer if it doesn't exist and subscribes it to transactions
    fn add_pending_event<T>(&self, account: UInt256, state: &EventsState<T>) -> bool
    where
        T: std::fmt::Debug + ReadFromTransaction + 'static,
    {
        use dashmap::mapref::entry::Entry;

        let new_event = if let Entry::Vacant(entry) = state.pending.entry(account) {
            let observer = AccountObserver::new(&state.events_tx);
            entry.insert(PendingEventState {
                processing_started: AtomicBool::new(false),
                observer: observer.clone(),
            });
            self.context
                .ton_subscriber
                .add_transactions_subscription([account], &observer);
            true
        } else {
            false
        };

        // Update metrics
        // NOTE: use separate flag to reduce events map lock duration
        if new_event {
            state.count.fetch_add(1, Ordering::Release);
        }

        new_event
    }

    /// Waits future in background. In case of error does nothing but logging
    fn spawn_background_task<F>(self: &Arc<Self>, name: &'static str, fut: F)
    where
        F: Future<Output = Result<()>> + Send + 'static,
    {
        tokio::spawn(async move {
            if let Err(e) = fut.await {
                log::error!("Failed to {}: {:?}", name, e);
            }
        });
    }
}

pub struct BridgeMetrics {
    pub pending_eth_event_count: usize,
    pub pending_ton_event_count: usize,
}

struct EventsState<T> {
    pending: FxDashMap<UInt256, PendingEventState<T>>,
    count: AtomicUsize,
    events_tx: AccountEventsTx<T>,
}

impl<T> EventsState<T>
where
    T: EventExt,
{
    fn new(events_tx: AccountEventsTx<T>) -> Arc<Self> {
        Arc::new(Self {
            pending: Default::default(),
            count: Default::default(),
            events_tx,
        })
    }

    /// Returns false if event processing was already started or event didn't exist
    fn start_processing(&self, account: &UInt256) -> bool {
        match self.pending.get(account) {
            Some(entry) => !entry.processing_started.fetch_or(true, Ordering::AcqRel),
            None => false,
        }
    }

    fn remove(&self, account: &UInt256) {
        if self.pending.remove(account).is_some() {
            self.count.fetch_sub(1, Ordering::Release);
        }
    }
}

struct PendingEventState<T> {
    processing_started: AtomicBool,
    observer: Arc<AccountObserver<T>>,
}

#[async_trait::async_trait]
trait EventExt {
    async fn update_event(bridge: Arc<Bridge>, account: UInt256) -> Result<()>;
}

#[async_trait::async_trait]
impl EventExt for EthEvent {
    async fn update_event(bridge: Arc<Bridge>, account: UInt256) -> Result<()> {
        bridge.update_eth_event(account).await
    }
}

#[async_trait::async_trait]
impl EventExt for TonEvent {
    async fn update_event(bridge: Arc<Bridge>, account: UInt256) -> Result<()> {
        bridge.update_ton_event(account).await
    }
}

/// Semi-persistent bridge contracts collection
#[derive(Default)]
struct BridgeState {
    connectors: ConnectorsMap,
    eth_event_configurations: EthEventConfigurationsMap,
    ton_event_configurations: TonEventConfigurationsMap,

    /// Unique event contracts code hashes.
    ///
    /// NOTE: only built on startup and then updated on each new configuration.
    /// Elements are not removed because it is not needed (the situation when one
    /// contract code will be used for ETH and TON simultaneously)
    event_code_hashes: EventCodeHashesMap,
}

impl BridgeState {
    fn has_expired_ton_event_configurations(&self, current_timestamp: u32) -> bool {
        self.ton_event_configurations
            .iter()
            .any(|(_, state)| state.details.is_expired(current_timestamp))
    }

    fn unique_eth_event_configurations(&self) -> FxHashSet<UInt256> {
        self.eth_event_configurations
            .iter()
            .map(|(key, _)| *key)
            .collect()
    }

    fn unique_ton_event_configurations(&self) -> FxHashSet<UInt256> {
        self.ton_event_configurations
            .iter()
            .map(|(key, _)| *key)
            .collect()
    }
}

fn add_event_code_hash(
    event_code_hashes: &mut EventCodeHashesMap,
    code: &ton_types::Cell,
    event_type: EventType,
) -> Result<()> {
    match event_code_hashes.entry(code.repr_hash()) {
        // Just insert if it was not in the map
        hash_map::Entry::Vacant(entry) => {
            entry.insert(event_type);
        }
        // Do nothing if it was there with the same event type, otherwise return an error
        hash_map::Entry::Occupied(entry) => {
            if entry.get() != &event_type {
                return Err(BridgeError::InvalidEventConfiguration.into());
            }
        }
    };
    Ok(())
}

fn read_code_hash(cell: &mut ton_types::SliceData) -> Result<Option<ton_types::UInt256>> {
    // 1. Read account
    if !cell.get_next_bit()? {
        return Ok(None);
    }

    // 2. Skip non-standard address
    if cell.get_next_int(2)? != 0b10 || cell.get_next_bit()? {
        return Ok(None);
    }
    cell.move_by(8 + 256)?;

    // 3. Skip storage info
    // 3.1. Skip storage used
    ton_block::StorageUsed::skip(cell)?;
    // 3.2. Skip last paid
    cell.move_by(32)?;
    // 3.3. Skip due payment
    if cell.get_next_bit()? {
        ton_block::Grams::skip(cell)?;
    }

    // 4. Skip storage
    // 4.1. Skip last transaction lt
    cell.move_by(64)?;
    // 4.2. Skip balance
    ton_block::CurrencyCollection::skip(cell)?;

    // 5. Skip account state
    if !cell.get_next_bit()? {
        return Ok(None);
    }
    // 5.1. Skip optional split depth (`ton_block::Number5`)
    if cell.get_next_bit()? {
        cell.move_by(5)?;
    }
    // 5.2. Skip optional ticktock (`ton_block::TickTock`)
    if cell.get_next_bit()? {
        cell.move_by(2)?;
    }
    // 5.3. Skip empty code
    if !cell.get_next_bit()? {
        return Ok(None);
    }

    // Read code hash
    let code = cell.checked_drain_reference()?;
    Ok(Some(code.repr_hash()))
}

impl EventBaseContract<'_> {
    /// Determine event action
    fn process(&self, public_key: &UInt256) -> Result<EventAction> {
        Ok(match self.status()? {
            // If it is still initializing - postpone processing until relay keys are received
            EventStatus::Initializing => EventAction::Nop,
            // The only status in which we can vote
            EventStatus::Pending if self.get_voters(EventVote::Empty)?.contains(public_key) => {
                EventAction::Vote
            }
            // Discard event in other cases
            _ => EventAction::Remove,
        })
    }
}

enum EventAction {
    /// Delay event processing
    Nop,
    /// Remove pending event
    Remove,
    /// Continue voting for event
    Vote,
}

/// ETH event configuration data
#[derive(Clone)]
struct EthEventConfigurationState {
    /// Configuration details
    details: EthEventConfigurationDetails,
    /// Parsed and mapped event ABI
    event_abi: Arc<EthEventAbi>,
    /// ETH event topic
    topic_hash: [u8; 32],

    /// Observer must live as long as configuration lives
    _observer: Arc<AccountObserver<EthEventConfigurationEvent>>,
}

/// TON event configuration data
#[derive(Clone)]
struct TonEventConfigurationState {
    /// Configuration details
    details: TonEventConfigurationDetails,
    /// Parsed `eventData` ABI
    event_abi: Vec<ton_abi::Param>,

    /// Observer must live as long as configuration lives
    _observer: Arc<AccountObserver<TonEventConfigurationEvent>>,
}

impl TonEventConfigurationDetails {
    fn is_expired(&self, current_timestamp: u32) -> bool {
        (1..current_timestamp).contains(&self.network_configuration.end_timestamp)
    }
}

/// Parsed bridge event
#[derive(Debug, Clone)]
enum BridgeEvent {
    ConnectorDeployed(ConnectorDeployedEvent),
}

impl ReadFromTransaction for BridgeEvent {
    fn read_from_transaction(ctx: &TxContext<'_>) -> Option<Self> {
        let mut event = None;
        ctx.iterate_events(|id, body| {
            let connector_deployed = bridge_contract::events::connector_deployed();
            if id == connector_deployed.id {
                match connector_deployed
                    .decode_input(body)
                    .and_then(|tokens| tokens.unpack().map_err(anyhow::Error::from))
                {
                    Ok(parsed) => event = Some(BridgeEvent::ConnectorDeployed(parsed)),
                    Err(e) => {
                        log::error!("Failed to parse bridge event: {:?}", e);
                    }
                }
            }
        });
        event
    }
}

/// Parsed connector event
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
enum ConnectorEvent {
    Enable,
}

impl ReadFromTransaction for ConnectorEvent {
    fn read_from_transaction(ctx: &TxContext<'_>) -> Option<Self> {
        let mut event = None;
        ctx.iterate_events(|id, _| {
            if id == connector_contract::events::enabled().id {
                event = Some(ConnectorEvent::Enable);
            }
        });
        event
    }
}

impl TxContext<'_> {
    fn find_new_event_contract_address(&self) -> Option<UInt256> {
        let event = base_event_configuration_contract::events::new_event_contract();

        let mut address: Option<ton_block::MsgAddressInt> = None;
        self.iterate_events(|id, body| {
            if id == event.id {
                match event
                    .decode_input(body)
                    .and_then(|tokens| tokens.unpack_first().map_err(anyhow::Error::from))
                {
                    Ok(parsed) => address = Some(parsed),
                    Err(e) => {
                        log::error!("Failed to parse NewEventContract event: {:?}", e);
                    }
                }
            }
        });

        if let Some(address) = address {
            Some(only_account_hash(address))
        } else {
            log::warn!("NewEventContract was not found on deployEvent transaction");
            None
        }
    }
}

#[derive(Debug, Clone)]
enum TonEventConfigurationEvent {
    EventDeployed { address: UInt256 },
    SetEndTimestamp { end_timestamp: u32 },
}

impl ReadFromTransaction for TonEventConfigurationEvent {
    fn read_from_transaction(ctx: &TxContext<'_>) -> Option<Self> {
        let in_msg_body = ctx.in_msg_internal()?.body()?;

        let deploy_event = ton_event_configuration_contract::deploy_event();
        let set_end_timestamp = ton_event_configuration_contract::set_end_timestamp();

        match nekoton_abi::read_function_id(&in_msg_body).ok()? {
            id if id == deploy_event.input_id => Some(Self::EventDeployed {
                address: ctx.find_new_event_contract_address()?,
            }),
            id if id == set_end_timestamp.input_id => {
                let end_timestamp = set_end_timestamp
                    .decode_input(in_msg_body, true)
                    .and_then(|tokens| tokens.unpack_first().map_err(anyhow::Error::from))
                    .ok()?;

                Some(Self::SetEndTimestamp { end_timestamp })
            }
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
enum EthEventConfigurationEvent {
    EventDeployed { address: UInt256 },
    SetEndBlockNumber { end_block_number: u32 },
}

impl ReadFromTransaction for EthEventConfigurationEvent {
    fn read_from_transaction(ctx: &TxContext<'_>) -> Option<Self> {
        let in_msg_body = ctx.in_msg_internal()?.body()?;

        let deploy_event = eth_event_configuration_contract::deploy_event();
        let set_end_block_number = eth_event_configuration_contract::set_end_block_number();

        match nekoton_abi::read_function_id(&in_msg_body).ok()? {
            id if id == deploy_event.input_id => Some(Self::EventDeployed {
                address: ctx.find_new_event_contract_address()?,
            }),
            id if id == set_end_block_number.input_id => {
                let end_block_number = set_end_block_number
                    .decode_input(in_msg_body, true)
                    .and_then(|tokens| tokens.unpack_first().map_err(anyhow::Error::from))
                    .ok()?;

                Some(Self::SetEndBlockNumber { end_block_number })
            }
            _ => None,
        }
    }
}

impl TxContext<'_> {
    fn get_event_status(&self) -> Option<EventStatus> {
        let state = self.get_account_state().ok()?;
        EventBaseContract(&state).status().ok()
    }
}

#[derive(Debug, Clone)]
enum EthEvent {
    ReceiveRoundRelays {
        keys: Vec<UInt256>,
        status: EventStatus,
    },
    Confirm {
        public_key: UInt256,
        status: EventStatus,
    },
    Reject {
        public_key: UInt256,
        status: EventStatus,
    },
}

impl ReadFromTransaction for EthEvent {
    fn read_from_transaction(ctx: &TxContext<'_>) -> Option<Self> {
        let in_msg = ctx.in_msg;
        match in_msg.header() {
            ton_block::CommonMsgInfo::ExtInMsgInfo(_) => {
                let (public_key, body) = read_external_in_msg(&in_msg.body()?)?;

                match read_function_id(&body) {
                    Ok(id) if id == eth_event_contract::confirm().input_id => ctx
                        .get_event_status()
                        .map(|status| EthEvent::Confirm { public_key, status }),
                    Ok(id) if id == eth_event_contract::reject().input_id => ctx
                        .get_event_status()
                        .map(|status| EthEvent::Reject { public_key, status }),
                    _ => None,
                }
            }
            ton_block::CommonMsgInfo::IntMsgInfo(_) => {
                let body = in_msg.body()?;

                match read_function_id(&body) {
                    Ok(id) if id == base_event_contract::receive_round_relays().input_id => {
                        let RelayKeys { items } = base_event_contract::receive_round_relays()
                            .decode_input(body, true)
                            .and_then(|tokens| tokens.unpack().map_err(anyhow::Error::from))
                            .ok()?;

                        ctx.get_event_status()
                            .map(|status| EthEvent::ReceiveRoundRelays {
                                keys: items,
                                status,
                            })
                    }
                    _ => None,
                }
            }
            ton_block::CommonMsgInfo::ExtOutMsgInfo(_) => None,
        }
    }
}

#[derive(Debug, Clone)]
enum TonEvent {
    ReceiveRoundRelays {
        keys: Vec<UInt256>,
        status: EventStatus,
    },
    Confirm {
        public_key: UInt256,
        status: EventStatus,
    },
    Reject {
        public_key: UInt256,
        status: EventStatus,
    },
}

impl ReadFromTransaction for TonEvent {
    fn read_from_transaction(ctx: &TxContext<'_>) -> Option<Self> {
        let in_msg = ctx.in_msg;
        match in_msg.header() {
            ton_block::CommonMsgInfo::ExtInMsgInfo(_) => {
                let (public_key, body) = read_external_in_msg(&in_msg.body()?)?;

                match read_function_id(&body) {
                    Ok(id) if id == ton_event_contract::confirm().input_id => ctx
                        .get_event_status()
                        .map(|status| TonEvent::Confirm { public_key, status }),
                    Ok(id) if id == ton_event_contract::reject().input_id => ctx
                        .get_event_status()
                        .map(|status| TonEvent::Reject { public_key, status }),
                    _ => None,
                }
            }
            ton_block::CommonMsgInfo::IntMsgInfo(_) => {
                let body = in_msg.body()?;

                match read_function_id(&body) {
                    Ok(id) if id == base_event_contract::receive_round_relays().input_id => {
                        let RelayKeys { items } = base_event_contract::receive_round_relays()
                            .decode_input(body, true)
                            .and_then(|tokens| tokens.unpack().map_err(anyhow::Error::from))
                            .ok()?;

                        ctx.get_event_status()
                            .map(|status| TonEvent::ReceiveRoundRelays {
                                keys: items,
                                status,
                            })
                    }
                    _ => None,
                }
            }
            ton_block::CommonMsgInfo::ExtOutMsgInfo(_) => None,
        }
    }
}

fn read_external_in_msg(body: &ton_types::SliceData) -> Option<(UInt256, ton_types::SliceData)> {
    match unpack_headers::<DefaultHeaders>(body) {
        Ok(((Some(public_key), _, _), body)) => Some((public_key, body)),
        _ => None,
    }
}

type ConnectorState = Arc<AccountObserver<ConnectorEvent>>;

type DefaultHeaders = (PubkeyHeader, TimeHeader, ExpireHeader);

type ConnectorsMap = FxHashMap<UInt256, ConnectorState>;
type EthEventConfigurationsMap = FxHashMap<UInt256, EthEventConfigurationState>;
type TonEventConfigurationsMap = FxHashMap<UInt256, TonEventConfigurationState>;
type EventCodeHashesMap = FxHashMap<UInt256, EventType>;

#[derive(thiserror::Error, Debug)]
enum BridgeError {
    #[error("Unknown chain id")]
    UnknownChainId,
    #[error("Unknown connector")]
    UnknownConnector,
    #[error("Unknown event configuration")]
    UnknownConfiguration,
    #[error("Bridge account not found")]
    BridgeAccountNotFound,
    #[error("Invalid event configuration")]
    InvalidEventConfiguration,
    #[error("Event configuration already exists")]
    EventConfigurationAlreadyExists,
}
