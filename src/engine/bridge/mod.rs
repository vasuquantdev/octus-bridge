use std::collections::hash_map;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use nekoton_abi::*;
use parking_lot::RwLock;
use tiny_adnl::utils::*;
use tokio::sync::mpsc;
use ton_block::HashmapAugType;
use ton_types::UInt256;

use crate::engine::eth_subscriber::*;
use crate::engine::keystore::*;
use crate::engine::ton_contracts::*;
use crate::engine::ton_subscriber::*;
use crate::engine::EngineContext;
use crate::utils::*;

pub struct Bridge {
    context: Arc<EngineContext>,

    bridge_account: UInt256,

    bridge_observer: Arc<AccountObserver<BridgeEvent>>,
    state: Arc<RwLock<BridgeState>>,

    pending_eth_events: Arc<FxDashMap<UInt256, PendingEthEvent>>,
    pending_ton_events: Arc<FxDashMap<UInt256, PendingTonEvent>>,

    connectors_tx: AccountEventsTx<ConnectorEvent>,
    eth_event_configurations_tx: AccountEventsTx<EthEventConfigurationEvent>,
    ton_event_configurations_tx: AccountEventsTx<TonEventConfigurationEvent>,
    eth_events_tx: AccountEventsTx<EthEvent>,
    ton_events_tx: AccountEventsTx<TonEvent>,
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
            state: Arc::new(Default::default()),
            pending_eth_events: Arc::new(Default::default()),
            pending_ton_events: Arc::new(Default::default()),
            connectors_tx,
            eth_event_configurations_tx,
            ton_event_configurations_tx,
            eth_events_tx,
            ton_events_tx,
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

        bridge
            .context
            .ton_subscriber
            .add_transactions_subscription([bridge.bridge_account], &bridge.bridge_observer);

        // Initialize
        bridge.get_all_configurations().await?;
        bridge.get_all_events().await?;

        bridge.start_eth_event_configurations_gc();
        bridge.start_ton_event_configurations_gc();

        Ok(bridge)
    }

    async fn process_bridge_event(
        self: Arc<Self>,
        (_, event): (UInt256, BridgeEvent),
    ) -> Result<()> {
        match event {
            BridgeEvent::ConnectorDeployed(event) => {
                match self.state.write().connectors.entry(event.connector) {
                    hash_map::Entry::Vacant(entry) => {
                        let observer = AccountObserver::new(&self.connectors_tx);

                        let entry = entry.insert(ConnectorState {
                            details: ConnectorDetails {
                                id: event.id,
                                event_configuration: event.event_configuration,
                                enabled: false,
                            },
                            event_type: ConnectorConfigurationType::Unknown,
                            observer,
                        });

                        self.context
                            .ton_subscriber
                            .add_transactions_subscription([event.connector], &entry.observer);
                    }
                    hash_map::Entry::Occupied(_) => {
                        log::error!(
                            "Got connector deployment event but it already exists: {}",
                            event.connector.to_hex_string()
                        );
                        return Ok(());
                    }
                };

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
            EthEventConfigurationEvent::EventDeployed { address, .. } => {
                // TODO: check if vote data already exist somewhere
                self.add_pending_eth_event(address);
            }
            EthEventConfigurationEvent::SetEndBlockNumber { end_block_number } => {
                let mut state = self.state.write();
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
            TonEventConfigurationEvent::EventDeployed { address, .. } => {
                if !self.add_pending_ton_event(address) {
                    log::warn!("Got deployment message for pending event: {:x}", account);
                }
            }
            TonEventConfigurationEvent::SetEndTimestamp { end_timestamp } => {
                let mut state = self.state.write();
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

        if let Entry::Occupied(entry) = self.pending_eth_events.entry(account) {
            match event {
                EthEvent::ReceiveRoundRelays { keys } => {
                    // Check if event contains our key
                    if !keys.contains(our_public_key) {
                        entry.remove();
                        return Ok(());
                    }

                    self.spawn_background_task(
                        "update ETH event",
                        self.clone().update_eth_event(account),
                    );
                }
                // Handle our confirmation or rejection
                EthEvent::Confirm { public_key } | EthEvent::Reject { public_key }
                    if public_key == our_public_key =>
                {
                    entry.remove();
                }
                _ => { /* Ignore other votes */ }
            }
        }

        Ok(())
    }

    async fn process_ton_event(
        self: Arc<Self>,
        (account, event): (UInt256, TonEvent),
    ) -> Result<()> {
        use dashmap::mapref::entry::Entry;

        let our_public_key = self.context.keystore.ton.public_key();

        if let Entry::Occupied(entry) = self.pending_ton_events.entry(account) {
            match event {
                // Handle received keys
                TonEvent::ReceiveRoundRelays { keys } => {
                    // Check if event contains our key
                    if !keys.contains(our_public_key) {
                        entry.remove();
                        return Ok(());
                    }

                    self.spawn_background_task(
                        "update TON event",
                        self.clone().update_ton_event(account),
                    );
                }
                // Handle our confirmation or rejection
                TonEvent::Confirm { public_key, .. } | TonEvent::Reject { public_key }
                    if public_key == our_public_key =>
                {
                    entry.remove();
                }
                _ => { /* Ignore other votes */ }
            }
        }

        Ok(())
    }

    async fn update_eth_event(self: Arc<Self>, account: UInt256) -> Result<()> {
        let keystore = &self.context.keystore;
        let ton_subscriber = &self.context.ton_subscriber;
        let eth_subscribers = &self.context.eth_subscribers;

        let contract = ton_subscriber.wait_contract_state(account).await?;

        match EventBaseContract(&contract).process(keystore.ton.public_key())? {
            EventAction::Nop => return Ok(()),
            EventAction::Remove => {
                self.pending_eth_events.remove(&account);
                return Ok(());
            }
            EventAction::Vote => { /* continue voting */ }
        }

        let event_init_data = EthEventContract(&contract).event_init_data()?;

        let data = self
            .state
            .read()
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
            });

        let (eth_subscriber, event_abi, blocks_to_confirm) = match data {
            Some((chain_id, abi, blocks_to_confirm)) => {
                match eth_subscribers.get_subscriber(chain_id) {
                    Some(subscriber) => (subscriber, abi, blocks_to_confirm),
                    None => {
                        log::error!(
                            "ETH subscriber with chain id  {} was not found for event {:x}",
                            chain_id,
                            account
                        );
                        self.pending_eth_events.remove(&account);
                        return Ok(());
                    }
                }
            }
            None => {
                log::error!(
                    "ETH event configuration {:x} not found for event {:x}",
                    event_init_data.configuration,
                    account
                );
                self.pending_eth_events.remove(&account);
                return Ok(());
            }
        };

        let message = match eth_subscriber
            .verify(event_init_data.vote_data, event_abi, blocks_to_confirm)
            .await
        {
            Ok(VerificationStatus::Exists) => {
                UnsignedMessage::new(eth_event_contract::confirm(), account)
            }
            Ok(VerificationStatus::NotExists) => {
                UnsignedMessage::new(eth_event_contract::reject(), account)
            }
            Err(e) => {
                log::error!("Failed to verify ETH event {:x}: {:?}", account, e);
                self.pending_eth_events.remove(&account);
                return Ok(());
            }
        };

        let eth_event_observer = match self.pending_eth_events.get(&account) {
            Some(observer) => observer.clone(),
            None => return Ok(()),
        };

        self.context
            .deliver_message(eth_event_observer, message)
            .await?;

        Ok(())
    }

    async fn update_ton_event(self: Arc<Self>, account: UInt256) -> Result<()> {
        let keystore = &self.context.keystore;
        let ton_subscriber = &self.context.ton_subscriber;

        let contract = ton_subscriber.wait_contract_state(account).await?;

        // Check further steps based on event statuses
        match EventBaseContract(&contract).process(keystore.ton.public_key())? {
            EventAction::Nop => return Ok(()),
            EventAction::Remove => {
                self.pending_ton_events.remove(&account);
                return Ok(());
            }
            EventAction::Vote => { /* continue voting */ }
        }

        // Get event details
        let event_init_data = TonEventContract(&contract).event_init_data()?;

        // Find suitable configuration
        // NOTE: be sure to drop `self.state` before removing pending ton event.
        // It may deadlock otherwise!
        let data = self
            .state
            .read()
            .ton_event_configurations
            .get(&event_init_data.configuration)
            .map(|configuration| {
                ton_abi::TokenValue::decode_params(
                    &configuration.event_abi,
                    event_init_data.vote_data.event_data.clone().into(),
                    2,
                )
            });

        let decoded_data = match data {
            // Decode event data with event abi from configuration
            Some(data) => data.and_then(map_ton_tokens_to_eth_bytes),
            // Do nothing when configuration was not found
            None => {
                log::error!(
                    "TON event configuration {:x} not found for event {:x}",
                    event_init_data.configuration,
                    account
                );
                self.pending_ton_events.remove(&account);
                return Ok(());
            }
        };

        let message = match decoded_data {
            // Confirm with signature
            Ok(data) => UnsignedMessage::new(ton_event_contract::confirm(), account)
                .arg(keystore.eth.sign(&data).to_vec()),

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

        let ton_event_observer = match self.pending_ton_events.get(&account) {
            Some(observer) => observer.clone(),
            None => return Ok(()),
        };

        self.context
            .deliver_message(ton_event_observer, message)
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
        let mut state = self.state.write();
        self.process_event_configuration(
            &mut *state,
            &connector_account,
            &event_configuration,
            &contract,
        )?;

        Ok(())
    }

    async fn get_all_configurations(&self) -> Result<()> {
        let shard_accounts = self.context.get_all_shard_accounts().await?;

        let ton_subscriber = &self.context.ton_subscriber;

        let contract = shard_accounts
            .find_account(&self.bridge_account)?
            .ok_or(BridgeError::BridgeAccountNotFound)?;
        let bridge = BridgeContract(&contract);

        let connector_count = bridge.connector_counter()?;

        let mut state = self.state.write();

        // Iterate for all connectors
        for id in 0..connector_count {
            // Compute next connector address
            let connector_account = bridge.derive_connector_address(id)?;

            // Extract details from contract
            let details = match shard_accounts.find_account(&connector_account)? {
                Some(contract) => match ConnectorContract(&contract).get_details() {
                    Ok(details) => details,
                    Err(e) => {
                        log::error!(
                            "Failed to get connector details {}: {:?}",
                            connector_account.to_hex_string(),
                            e
                        );
                        continue;
                    }
                },
                None => {
                    log::error!("Connector not found: {}", connector_account.to_hex_string());
                    continue;
                }
            };
            log::info!("Found configuration connector {}: {:?}", id, details);

            let enabled = details.enabled;
            let configuration_account = details.event_configuration;

            let observer = AccountObserver::new(&self.connectors_tx);

            // Add new connector
            state.connectors.insert(
                connector_account,
                ConnectorState {
                    details,
                    event_type: ConnectorConfigurationType::Unknown,
                    observer: observer.clone(),
                },
            );

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
                            "Connected configuration was not found: {}",
                            details.event_configuration.to_hex_string()
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
                    "Failed to process event configuration {}: {:?}",
                    &details.event_configuration.to_hex_string(),
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
        let event_type = EventConfigurationBaseContract(configuration_contract).get_type()?;
        log::info!("Found configuration of type: {}", event_type);

        match state.connectors.get_mut(connector_account) {
            Some(connector) => connector.event_type = ConnectorConfigurationType::Known(event_type),
            None => return Err(BridgeError::UnknownConnector.into()),
        }

        match event_type {
            // Extract and populate ETH event configuration details
            EventType::Eth => self.add_eth_event_configuration(
                state,
                configuration_account,
                configuration_contract,
            )?,
            // Extract and populate TON event configuration details
            EventType::Ton => self.add_ton_event_configuration(
                state,
                configuration_account,
                configuration_contract,
            )?,
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
        let details = EthEventConfigurationContract(contract).get_details()?;

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

        // Check if configuration is expired
        let last_processed_height = eth_subscriber.get_last_processed_block();
        if details.is_expired(last_processed_height) {
            // Do nothing in that case
            log::warn!(
                "Ignoring ETH event configuration {}: end block number {} is less then current {}",
                account.to_hex_string(),
                details.network_configuration.end_block_number,
                last_processed_height
            );
            return Ok(());
        }

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
                    observer: observer.clone(),
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
        let details = TonEventConfigurationContract(contract).get_details()?;

        // Check if configuration is expired
        let current_timestamp = self.context.ton_subscriber.current_utime();
        if details.is_expired(current_timestamp) {
            // Do nothing in that case
            log::warn!(
                "Ignoring TON event configuration {}: end timestamp {} is less then current {}",
                account.to_hex_string(),
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
                    observer: observer.clone(),
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
        let shard_accounts = self.context.get_all_shard_accounts().await?;

        let our_public_key = self.context.keystore.ton.public_key();

        let state = self.state.read();

        for (_, accounts) in shard_accounts {
            accounts.iterate_with_keys(|hash, shard_account| {
                // Get account from shard state
                let account = match shard_account.read_account()? {
                    ton_block::Account::Account(account) => account,
                    ton_block::Account::AccountNone => return Ok(true),
                };

                // Try to get its hash
                let code_hash = match account.storage.state() {
                    ton_block::AccountState::AccountActive(ton_block::StateInit {
                        code: Some(code),
                        ..
                    }) => code.repr_hash(),
                    _ => return Ok(true),
                };

                // Filter only known event contracts
                let event_type = match state.event_code_hashes.get(&code_hash) {
                    Some(event_type) => event_type,
                    None => return Ok(true),
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

                match EventBaseContract(&contract).process(our_public_key) {
                    Ok(EventAction::Nop | EventAction::Vote) => match event_type {
                        EventType::Eth => {
                            if self.add_pending_eth_event(hash) {
                                self.spawn_background_task(
                                    "initial update ETH event",
                                    self.clone().update_eth_event(hash),
                                );
                            }
                        }
                        EventType::Ton => {
                            if self.add_pending_ton_event(hash) {
                                self.spawn_background_task(
                                    "initial update TON event",
                                    self.clone().update_ton_event(hash),
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
            })?;
        }

        Ok(())
    }

    fn start_eth_event_configurations_gc(self: &Arc<Self>) {
        let bridge = Arc::downgrade(self);

        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(10)).await;

                let bridge = match bridge.upgrade() {
                    Some(bridge) => bridge,
                    None => return,
                };

                let last_block_heights = bridge.context.eth_subscribers.get_last_block_numbers();

                let subscriptions_to_remove = {
                    let mut to_remove = FxHashMap::default();

                    let mut state = bridge.state.write();
                    state.eth_event_configurations.retain(|account, state| {
                        let network_configuration = &state.details.network_configuration;

                        let chain_id = network_configuration.chain_id;
                        let last_number = match last_block_heights.get(&chain_id) {
                            Some(height) => *height,
                            None => {
                                log::warn!("Found unknown chain id: {}", chain_id);
                                return true;
                            }
                        };

                        if state.details.is_expired(last_number) {
                            log::warn!(
                                "Removing ETH event configuration {}",
                                account.to_hex_string()
                            );

                            to_remove
                                .entry(chain_id)
                                .or_insert_with(|| Vec::with_capacity(1))
                                .push((
                                    ethabi::Address::from(network_configuration.event_emitter),
                                    state.topic_hash,
                                    *account,
                                ));
                            false
                        } else {
                            true
                        }
                    });

                    to_remove
                };

                for (chain_id, subscriptions) in subscriptions_to_remove {
                    let subscriber = match bridge.context.eth_subscribers.get_subscriber(chain_id) {
                        Some(subscriber) => subscriber,
                        None => continue,
                    };
                    subscriber.unsubscribe(subscriptions);
                }
            }
        });
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
                    let state = bridge.state.read();
                    state.has_expired_ton_event_configurations(current_utime)
                };

                // Do nothing if there are not expired configurations
                if !has_expired_configurations {
                    continue;
                }

                // Wait all shards
                let current_utime = match ton_subscriber.wait_shards().await {
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

                // Get all expired configurations
                let mut state = bridge.state.write();
                state.ton_event_configurations.retain(|account, state| {
                    if state.details.is_expired(current_utime) {
                        log::warn!(
                            "Removing TON event configuration {}",
                            account.to_hex_string()
                        );
                        false
                    } else {
                        true
                    }
                });
            }
        });
    }

    fn add_pending_eth_event(&self, account: UInt256) -> bool {
        use dashmap::mapref::entry::Entry;

        if let Entry::Vacant(entry) = self.pending_eth_events.entry(account) {
            let observer = AccountObserver::new(&self.eth_events_tx);
            entry.insert(observer.clone());

            self.context
                .ton_subscriber
                .add_transactions_subscription([account], &observer);

            true
        } else {
            false
        }
    }

    fn add_pending_ton_event(&self, account: UInt256) -> bool {
        use dashmap::mapref::entry::Entry;

        if let Entry::Vacant(entry) = self.pending_ton_events.entry(account) {
            let observer = AccountObserver::new(&self.ton_events_tx);
            entry.insert(observer.clone());
            self.context
                .ton_subscriber
                .add_transactions_subscription([account], &observer);
            true
        } else {
            false
        }
    }

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

#[derive(Default)]
struct BridgeState {
    connectors: ConnectorsMap,
    eth_event_configurations: EthEventConfigurationsMap,
    ton_event_configurations: TonEventConfigurationsMap,
    event_code_hashes: EventCodeHashesMap,
}

impl BridgeState {
    fn has_expired_ton_event_configurations(&self, current_timestamp: u32) -> bool {
        self.ton_event_configurations
            .iter()
            .any(|(_, state)| state.details.is_expired(current_timestamp))
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

type PendingEthEvent = Arc<AccountObserver<EthEvent>>;
type PendingTonEvent = Arc<AccountObserver<TonEvent>>;

impl EventBaseContract<'_> {
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
    Nop,
    Remove,
    Vote,
}

#[derive(Clone)]
struct ConnectorState {
    details: ConnectorDetails,
    event_type: ConnectorConfigurationType,
    observer: Arc<AccountObserver<ConnectorEvent>>,
}

/// Linked configuration event type hint
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
enum ConnectorConfigurationType {
    Unknown,
    Known(EventType),
}

#[derive(Clone)]
struct EthEventConfigurationState {
    details: EthEventConfigurationDetails,
    event_abi: Arc<EthEventAbi>,
    topic_hash: [u8; 32],
    observer: Arc<AccountObserver<EthEventConfigurationEvent>>,
}

impl EthEventConfigurationDetails {
    fn is_expired(&self, end_block_number: u64) -> bool {
        (1..end_block_number).contains(&(self.network_configuration.end_block_number as u64))
    }
}

#[derive(Clone)]
struct TonEventConfigurationState {
    details: TonEventConfigurationDetails,
    event_abi: Vec<ton_abi::Param>,
    observer: Arc<AccountObserver<TonEventConfigurationEvent>>,
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

#[derive(Debug, Clone)]
enum TonEventConfigurationEvent {
    EventDeployed {
        vote_data: TonEventVoteData,
        address: UInt256,
    },
    SetEndTimestamp {
        end_timestamp: u32,
    },
}

impl ReadFromTransaction for TonEventConfigurationEvent {
    fn read_from_transaction(ctx: &TxContext<'_>) -> Option<Self> {
        let in_msg_body = ctx.in_msg_internal()?.body()?;

        match nekoton_abi::read_function_id(&in_msg_body).ok()? {
            id if id == ton_event_configuration_contract::deploy_event().input_id => {
                let function = ton_event_configuration_contract::set_end_timestamp();

                let vote_data: TonEventVoteData = function
                    .decode_input(in_msg_body, true)
                    .and_then(|tokens| tokens.unpack_first().map_err(anyhow::Error::from))
                    .ok()?;

                let address: ton_block::MsgAddressInt =
                    ctx.find_function_output(function)?.unpack_first().ok()?;

                Some(Self::EventDeployed {
                    vote_data,
                    address: only_account_hash(address),
                })
            }
            id if id == ton_event_configuration_contract::set_end_timestamp().input_id => {
                let end_timestamp = ton_event_configuration_contract::set_end_timestamp()
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
    EventDeployed {
        vote_data: EthEventVoteData,
        address: UInt256,
    },
    SetEndBlockNumber {
        end_block_number: u32,
    },
}

impl ReadFromTransaction for EthEventConfigurationEvent {
    fn read_from_transaction(ctx: &TxContext<'_>) -> Option<Self> {
        let in_msg_body = ctx.in_msg_internal()?.body()?;

        match nekoton_abi::read_function_id(&in_msg_body).ok()? {
            id if id == eth_event_configuration_contract::deploy_event().input_id => {
                let function = eth_event_configuration_contract::deploy_event();

                let vote_data: EthEventVoteData = function
                    .decode_input(in_msg_body, true)
                    .and_then(|tokens| tokens.unpack_first().map_err(anyhow::Error::from))
                    .ok()?;

                let address: ton_block::MsgAddressInt =
                    ctx.find_function_output(function)?.unpack_first().ok()?;

                Some(Self::EventDeployed {
                    vote_data,
                    address: only_account_hash(address),
                })
            }
            id if id == eth_event_configuration_contract::set_end_block_number().input_id => {
                let end_block_number = eth_event_configuration_contract::set_end_block_number()
                    .decode_input(in_msg_body, true)
                    .and_then(|tokens| tokens.unpack_first().map_err(anyhow::Error::from))
                    .ok()?;

                Some(Self::SetEndBlockNumber { end_block_number })
            }
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
enum EthEvent {
    ReceiveRoundRelays { keys: Vec<UInt256> },
    Confirm { public_key: UInt256 },
    Reject { public_key: UInt256 },
}

impl ReadFromTransaction for EthEvent {
    fn read_from_transaction(ctx: &TxContext<'_>) -> Option<Self> {
        let in_msg = ctx.in_msg;
        match in_msg.header() {
            ton_block::CommonMsgInfo::ExtInMsgInfo(_) => {
                let (public_key, body) = read_external_in_msg(&in_msg.body()?)?;

                match read_function_id(&body) {
                    Ok(id) if id == eth_event_contract::confirm().input_id => {
                        Some(EthEvent::Confirm { public_key })
                    }
                    Ok(id) if id == eth_event_contract::reject().input_id => {
                        Some(EthEvent::Reject { public_key })
                    }
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

                        Some(EthEvent::ReceiveRoundRelays { keys: items })
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
    },
    Confirm {
        public_key: UInt256,
        signature: Vec<u8>,
    },
    Reject {
        public_key: UInt256,
    },
}

impl ReadFromTransaction for TonEvent {
    fn read_from_transaction(ctx: &TxContext<'_>) -> Option<Self> {
        let in_msg = ctx.in_msg;
        match in_msg.header() {
            ton_block::CommonMsgInfo::ExtInMsgInfo(_) => {
                let (public_key, body) = read_external_in_msg(&in_msg.body()?)?;

                match read_function_id(&body) {
                    Ok(id) if id == ton_event_contract::confirm().input_id => {
                        let signature = ton_event_contract::confirm()
                            .decode_input(body, true)
                            .and_then(|tokens| tokens.unpack_first().map_err(anyhow::Error::from))
                            .ok()?;

                        Some(TonEvent::Confirm {
                            public_key,
                            signature,
                        })
                    }
                    Ok(id) if id == ton_event_contract::reject().input_id => {
                        Some(TonEvent::Reject { public_key })
                    }
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

                        Some(TonEvent::ReceiveRoundRelays { keys: items })
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
