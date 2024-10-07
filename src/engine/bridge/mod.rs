use std::collections::hash_map;
use std::future::Future;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use borsh::BorshDeserialize;
use eth_ton_abi_converter::{
    decode_ton_event_abi, make_mapped_ton_event, map_ton_tokens_to_eth_bytes, EthEventAbi,
    EthToTonMappingContext,
};
use nekoton_abi::*;
use nekoton_utils::TrustMe;
use rustc_hash::{FxHashMap, FxHashSet};
use solana_bridge::bridge_errors::SolanaBridgeError;
use solana_client::client_error::{ClientError, ClientErrorKind};
use solana_client::rpc_request::{RpcError, RpcResponseErrorData};
use solana_client::rpc_response::RpcSimulateTransactionResult;
use solana_sdk::instruction::InstructionError;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::transaction::TransactionError;
use tokio::sync::mpsc;
use tokio::sync::RwLock;
use ton_abi::TokenValue;
use ton_types::UInt256;

use crate::engine::keystore::*;
use crate::engine::sol_subscriber::*;
use crate::engine::ton_contracts::*;
use crate::engine::ton_subscriber::*;
use crate::engine::EngineContext;
use crate::storage::*;
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

    // Observers for pending ETH->TON events
    eth_ton_events_state: Arc<EventsState<EthTonEvent>>,

    // Observers for pending TON->ETH events
    ton_eth_events_state: Arc<EventsState<TonEthEvent>>,

    // Observers for pending SOL->TON events
    sol_ton_events_state: Arc<EventsState<SolTonEvent>>,

    // Observers for pending TON->SOL events
    ton_sol_events_state: Arc<EventsState<TonSolEvent>>,

    connectors_tx: AccountEventsTx<ConnectorEvent>,
    eth_ton_event_configurations_tx: AccountEventsTx<EthTonEventConfigurationEvent>,
    ton_eth_event_configurations_tx: AccountEventsTx<TonEthEventConfigurationEvent>,
    sol_ton_event_configurations_tx: AccountEventsTx<SolTonEventConfigurationEvent>,
    ton_sol_event_configurations_tx: AccountEventsTx<TonSolEventConfigurationEvent>,

    total_active_eth_ton_event_configurations: AtomicUsize,
    total_active_ton_eth_event_configurations: AtomicUsize,
    total_active_sol_ton_event_configurations: AtomicUsize,
    total_active_ton_sol_event_configurations: AtomicUsize,
}

impl Bridge {
    pub async fn new(context: Arc<EngineContext>, bridge_account: UInt256) -> Result<Arc<Self>> {
        // Create bridge
        let (bridge_events_tx, bridge_events_rx) = mpsc::unbounded_channel();
        let (connectors_tx, connectors_rx) = mpsc::unbounded_channel();
        let (eth_ton_event_configurations_tx, eth_ton_event_configurations_rx) =
            mpsc::unbounded_channel();
        let (ton_eth_event_configurations_tx, ton_eth_event_configurations_rx) =
            mpsc::unbounded_channel();
        let (sol_ton_event_configurations_tx, sol_ton_event_configurations_rx) =
            mpsc::unbounded_channel();
        let (ton_sol_event_configurations_tx, ton_sol_event_configurations_rx) =
            mpsc::unbounded_channel();
        let (eth_ton_events_tx, eth_ton_events_rx) = mpsc::unbounded_channel();
        let (ton_eth_events_tx, ton_eth_events_rx) = mpsc::unbounded_channel();
        let (sol_ton_events_tx, sol_ton_events_rx) = mpsc::unbounded_channel();
        let (ton_sol_events_tx, ton_sol_events_rx) = mpsc::unbounded_channel();

        let bridge_observer = AccountObserver::new(&bridge_events_tx);

        let bridge = Arc::new(Bridge {
            context,
            bridge_account,
            bridge_observer: bridge_observer.clone(),
            state: Default::default(),
            eth_ton_events_state: EventsState::new(eth_ton_events_tx),
            ton_eth_events_state: EventsState::new(ton_eth_events_tx),
            sol_ton_events_state: EventsState::new(sol_ton_events_tx),
            ton_sol_events_state: EventsState::new(ton_sol_events_tx),
            connectors_tx,
            eth_ton_event_configurations_tx,
            ton_eth_event_configurations_tx,
            sol_ton_event_configurations_tx,
            ton_sol_event_configurations_tx,
            total_active_eth_ton_event_configurations: Default::default(),
            total_active_ton_eth_event_configurations: Default::default(),
            total_active_sol_ton_event_configurations: Default::default(),
            total_active_ton_sol_event_configurations: Default::default(),
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
            eth_ton_event_configurations_rx,
            Self::process_eth_ton_event_configuration_event,
        );

        start_listening_events(
            &bridge,
            "TonEthEventConfigurationContract",
            ton_eth_event_configurations_rx,
            Self::process_ton_eth_event_configuration_event,
        );

        if bridge.context.sol_subscriber.is_some() {
            start_listening_events(
                &bridge,
                "SolTonEventConfigurationContract",
                sol_ton_event_configurations_rx,
                Self::process_sol_ton_event_configuration_event,
            );

            start_listening_events(
                &bridge,
                "TonSolEventConfigurationContract",
                ton_sol_event_configurations_rx,
                Self::process_ton_sol_event_configuration_event,
            );
        }

        start_listening_events(
            &bridge,
            "EthTonEventContract",
            eth_ton_events_rx,
            Self::process_eth_ton_event,
        );

        start_listening_events(
            &bridge,
            "TonEthEventContract",
            ton_eth_events_rx,
            Self::process_ton_eth_event,
        );

        if bridge.context.sol_subscriber.is_some() {
            start_listening_events(
                &bridge,
                "SolTonEventContract",
                sol_ton_events_rx,
                Self::process_sol_ton_event,
            );

            start_listening_events(
                &bridge,
                "TonSolEventContract",
                ton_sol_events_rx,
                Self::process_ton_sol_event,
            );
        }

        // Subscribe bridge account to transactions
        bridge
            .context
            .ton_subscriber
            .add_transactions_subscription([bridge.bridge_account], &bridge.bridge_observer);

        // Initialize
        bridge.get_all_configurations().await?;
        bridge.get_all_events().await?;

        bridge.start_event_configurations_gc();

        Ok(bridge)
    }

    pub fn metrics(&self) -> BridgeMetrics {
        BridgeMetrics {
            pending_eth_ton_event_count: self.eth_ton_events_state.count.load(Ordering::Acquire),
            pending_ton_eth_event_count: self.ton_eth_events_state.count.load(Ordering::Acquire),
            pending_sol_ton_event_count: self.sol_ton_events_state.count.load(Ordering::Acquire),
            pending_ton_sol_event_count: self.ton_sol_events_state.count.load(Ordering::Acquire),
            total_active_eth_ton_event_configurations: self
                .total_active_eth_ton_event_configurations
                .load(Ordering::Acquire),
            total_active_ton_eth_event_configurations: self
                .total_active_ton_eth_event_configurations
                .load(Ordering::Acquire),
            total_active_sol_ton_event_configurations: self
                .total_active_sol_ton_event_configurations
                .load(Ordering::Acquire),
            total_active_ton_sol_event_configurations: self
                .total_active_ton_sol_event_configurations
                .load(Ordering::Acquire),
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
                        tracing::error!(
                            connector = %DisplayAddr(event.connector),
                            "got connector deployment event but it already exists",
                        );
                        return Ok(());
                    }
                };

                // Check connector contract if it was added in this iteration
                tokio::spawn(async move {
                    if let Err(e) = self.check_connector_contract(event.connector).await {
                        tracing::error!("failed to check connector contract: {e:?}");
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

    async fn process_eth_ton_event_configuration_event(
        self: Arc<Self>,
        (account, event): (UInt256, EthTonEventConfigurationEvent),
    ) -> Result<()> {
        match event {
            // Create observer on each deployment event
            EthTonEventConfigurationEvent::EventsDeployed { events } => {
                for address in events {
                    if self.add_pending_event(address, &self.eth_ton_events_state) {
                        let this = self.clone();
                        self.spawn_background_task("preprocess ETH->TON event", async move {
                            this.preprocess_event(address, &this.eth_ton_events_state)
                                .await
                        });
                    }
                }
            }
            // Update configuration state
            EthTonEventConfigurationEvent::SetEndBlockNumber { end_block_number } => {
                let mut state = self.state.write().await;
                let configuration = state
                    .eth_ton_event_configurations
                    .get_mut(&account)
                    .ok_or(BridgeError::UnknownConfiguration)?;
                configuration.details.network_configuration.end_block_number = end_block_number;
            }
        }
        Ok(())
    }

    async fn process_ton_eth_event_configuration_event(
        self: Arc<Self>,
        (account, event): (UInt256, TonEthEventConfigurationEvent),
    ) -> Result<()> {
        match event {
            // Create observer on each deployment event
            TonEthEventConfigurationEvent::EventDeployed { address, .. } => {
                if self.add_pending_event(address, &self.ton_eth_events_state) {
                    let this = self.clone();
                    self.spawn_background_task("preprocess TON->ETH event", async move {
                        this.preprocess_event(address, &this.ton_eth_events_state)
                            .await
                    });
                } else {
                    // NOTE: Each TON event must be unique on the contracts level,
                    // so receiving message with duplicated address is
                    // a signal that something went wrong
                    tracing::warn!(
                        configuration = %DisplayAddr(account),
                        event = %DisplayAddr(address),
                        "got deployment message for pending event",
                    );
                }
            }
            // Update configuration state
            TonEthEventConfigurationEvent::SetEndTimestamp { end_timestamp } => {
                let mut state = self.state.write().await;
                let configuration = state
                    .ton_eth_event_configurations
                    .get_mut(&account)
                    .ok_or(BridgeError::UnknownConfiguration)?;
                configuration.details.network_configuration.end_timestamp = end_timestamp;
            }
        }
        Ok(())
    }

    async fn process_sol_ton_event_configuration_event(
        self: Arc<Self>,
        (account, event): (UInt256, SolTonEventConfigurationEvent),
    ) -> Result<()> {
        match event {
            // Create observer on each deployment event
            SolTonEventConfigurationEvent::EventsDeployed { events } => {
                for address in events {
                    if self.add_pending_event(address, &self.sol_ton_events_state) {
                        let this = self.clone();
                        self.spawn_background_task("preprocess SOL->TON event", async move {
                            this.preprocess_event(address, &this.sol_ton_events_state)
                                .await
                        });
                    }
                }
            }
            // Update configuration state
            SolTonEventConfigurationEvent::SetEndTimestamp { end_timestamp } => {
                let mut state = self.state.write().await;
                let configuration = state
                    .sol_ton_event_configurations
                    .get_mut(&account)
                    .ok_or(BridgeError::UnknownConfiguration)?;
                configuration.details.network_configuration.end_timestamp = end_timestamp;
            }
        }
        Ok(())
    }

    async fn process_ton_sol_event_configuration_event(
        self: Arc<Self>,
        (account, event): (UInt256, TonSolEventConfigurationEvent),
    ) -> Result<()> {
        match event {
            // Create observer on each deployment event
            TonSolEventConfigurationEvent::EventDeployed { address, .. } => {
                if self.add_pending_event(address, &self.ton_sol_events_state) {
                    let this = self.clone();
                    self.spawn_background_task("preprocess TON->SOL event", async move {
                        this.preprocess_event(address, &this.ton_sol_events_state)
                            .await
                    });
                } else {
                    // NOTE: Each TON event must be unique on the contracts level,
                    // so receiving message with duplicated address is
                    // a signal that something went wrong
                    tracing::warn!(
                        configuration = %DisplayAddr(account),
                        event = %DisplayAddr(address),
                        "got deployment message for pending event",
                    );
                }
            }
            // Update configuration state
            TonSolEventConfigurationEvent::SetEndTimestamp { end_timestamp } => {
                let mut state = self.state.write().await;
                let configuration = state
                    .ton_sol_event_configurations
                    .get_mut(&account)
                    .ok_or(BridgeError::UnknownConfiguration)?;
                configuration.details.network_configuration.end_timestamp = end_timestamp;
            }
        }
        Ok(())
    }

    async fn process_eth_ton_event(
        self: Arc<Self>,
        (account, event): (UInt256, (EthTonEvent, EventStatus)),
    ) -> Result<()> {
        use dashmap::mapref::entry::Entry;

        let our_public_key = self.context.keystore.ton.public_key();

        // Use flag to update counter outside events map lock to reduce its duration
        let mut event_removed = false;

        // Handle only known ETH events
        if let Entry::Occupied(entry) = self.eth_ton_events_state.pending.entry(account) {
            let remove_entry = || {
                // Remove pending event
                entry.remove();
                event_removed = true;
            };

            match event {
                // Remove event if voting process was finished
                (EthTonEvent::Rejected, _)
                | (_, EventStatus::Confirmed | EventStatus::Rejected) => remove_entry(),
                // Handle event initialization
                (EthTonEvent::ReceiveRoundRelays { keys }, _) => {
                    // Check if event contains our key
                    if keys.contains(our_public_key) {
                        // Start voting
                        self.spawn_background_task(
                            "update ETH->TON event",
                            self.clone().update_eth_ton_event(account),
                        );
                    } else {
                        remove_entry();
                    }
                }
                // Handle our confirmation or rejection
                (EthTonEvent::Confirm { public_key } | EthTonEvent::Reject { public_key }, _)
                    if public_key == our_public_key =>
                {
                    remove_entry()
                }
                _ => { /* Ignore other events */ }
            }
        }

        // Update metrics
        if event_removed {
            self.eth_ton_events_state
                .count
                .fetch_sub(1, Ordering::Release);
        }

        Ok(())
    }

    async fn process_ton_eth_event(
        self: Arc<Self>,
        (account, event): (UInt256, (TonEthEvent, EventStatus)),
    ) -> Result<()> {
        use dashmap::mapref::entry::Entry;

        let our_public_key = self.context.keystore.ton.public_key();

        // Use flag to update counter outside events map lock to reduce its duration
        let mut event_removed = false;

        // Handle only known TON events
        if let Entry::Occupied(entry) = self.ton_eth_events_state.pending.entry(account) {
            let remove_entry = || {
                // Remove pending event
                entry.remove();
                event_removed = true;
            };

            match event {
                // Remove event in confirmed state if the balance is not enough.
                //
                // NOTE: it is not strictly necessary to collect all signatures, so the
                // contract subscription is allowed to be dropped on nearly empty balance.
                //
                // This state can be achieved by calling `close` method on transfer contract
                // or execution `confirm` or `reject` after several years so that the cost of
                // keeping the contract almost nullifies its balance.
                (TonEthEvent::Closed, EventStatus::Confirmed) => remove_entry(),
                // Remove event if it was rejected
                (TonEthEvent::Rejected, _) | (_, EventStatus::Rejected) => remove_entry(),
                // Handle event initialization
                (TonEthEvent::ReceiveRoundRelays { keys }, _) => {
                    // Check if event contains our key
                    if keys.contains(our_public_key) {
                        // Start voting
                        self.spawn_background_task(
                            "update TON->ETH event",
                            self.clone().update_ton_eth_event(account),
                        );
                    } else {
                        remove_entry();
                    }
                }
                // Handle our confirmation or rejection
                (TonEthEvent::Confirm { public_key } | TonEthEvent::Reject { public_key }, _)
                    if public_key == our_public_key =>
                {
                    remove_entry();
                }
                _ => { /* Ignore other events */ }
            }
        }

        // Update metrics
        if event_removed {
            self.ton_eth_events_state
                .count
                .fetch_sub(1, Ordering::Release);
        }

        Ok(())
    }

    async fn process_sol_ton_event(
        self: Arc<Self>,
        (account, event): (UInt256, (SolTonEvent, EventStatus)),
    ) -> Result<()> {
        use dashmap::mapref::entry::Entry;

        let our_public_key = self.context.keystore.sol.public_key_bytes();

        // Use flag to update counter outside events map lock to reduce its duration
        let mut event_removed = false;

        // Handle only known SOL events
        if let Entry::Occupied(entry) = self.sol_ton_events_state.pending.entry(account) {
            let remove_entry = || {
                // Remove pending event
                entry.remove();
                event_removed = true;
            };

            match event {
                // Remove event if voting process was finished
                (SolTonEvent::Rejected, _)
                | (_, EventStatus::Confirmed | EventStatus::Rejected) => remove_entry(),
                // Handle event initialization
                (SolTonEvent::ReceiveRoundRelays { keys }, _) => {
                    // Check if event contains our key
                    if keys.contains(our_public_key) {
                        // Start voting
                        self.spawn_background_task(
                            "update SOL->TON event",
                            self.clone().update_sol_ton_event(account),
                        );
                    } else {
                        remove_entry();
                    }
                }
                // Handle our confirmation or rejection
                (SolTonEvent::Confirm { public_key } | SolTonEvent::Reject { public_key }, _)
                    if public_key == our_public_key =>
                {
                    remove_entry()
                }
                _ => { /* Ignore other events */ }
            }
        }

        // Update metrics
        if event_removed {
            self.sol_ton_events_state
                .count
                .fetch_sub(1, Ordering::Release);
        }

        Ok(())
    }

    async fn process_ton_sol_event(
        self: Arc<Self>,
        (account, event): (UInt256, (TonSolEvent, EventStatus)),
    ) -> Result<()> {
        use dashmap::mapref::entry::Entry;

        let our_public_key = self.context.keystore.ton.public_key();

        // Use flag to update counter outside events map lock to reduce its duration
        let mut event_removed = false;

        // Handle only known TON events
        if let Entry::Occupied(entry) = self.ton_sol_events_state.pending.entry(account) {
            let remove_entry = || {
                // Remove pending event
                entry.remove();
                event_removed = true;
            };

            match event {
                // Remove event in confirmed state if the balance is not enough.
                //
                // NOTE: it is not strictly necessary to collect all signatures, so the
                // contract subscription is allowed to be dropped on nearly empty balance.
                //
                // This state can be achieved by calling `close` method on transfer contract
                // or execution `confirm` or `reject` after several years so that the cost of
                // keeping the contract almost nullifies its balance.
                (TonSolEvent::Closed, EventStatus::Confirmed) => remove_entry(),
                // Remove event if it was rejected
                (TonSolEvent::Rejected, _) | (_, EventStatus::Rejected) => remove_entry(),

                // Handle event initialization
                (TonSolEvent::ReceiveRoundRelays { keys }, _) => {
                    // Check if event contains our key
                    if keys.contains(our_public_key) {
                        // Start voting
                        self.spawn_background_task(
                            "update TON->SOL event",
                            self.clone().update_ton_sol_event(account),
                        );
                    } else {
                        remove_entry();
                    }
                }
                // Handle our confirmation or rejection
                (TonSolEvent::Confirm { public_key } | TonSolEvent::Reject { public_key }, _)
                    if public_key == our_public_key =>
                {
                    remove_entry();
                }
                _ => { /* Ignore other events */ }
            }
        }

        // Update metrics
        if event_removed {
            self.ton_sol_events_state
                .count
                .fetch_sub(1, Ordering::Release);
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
        match base_event_contract.process(
            self.context.keystore.ton.public_key(),
            T::REQUIRE_ALL_SIGNATURES,
        )? {
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

    async fn update_eth_ton_event(self: Arc<Self>, account: UInt256) -> Result<()> {
        if !self.eth_ton_events_state.start_processing(&account) {
            return Ok(());
        }

        let keystore = &self.context.keystore;
        let ton_subscriber = &self.context.ton_subscriber;
        let eth_subscribers = &self.context.eth_subscribers;

        // Wait contract state
        let contract = ton_subscriber.wait_contract_state(account).await?;

        match EventBaseContract(&contract).process(keystore.ton.public_key(), false)? {
            EventAction::Nop => return Ok(()),
            EventAction::Remove => {
                self.eth_ton_events_state.remove(&account);
                return Ok(());
            }
            EventAction::Vote => { /* continue voting */ }
        }

        let event_init_data = EthTonEventContract(&contract).event_init_data()?;

        struct ConfigData {
            chain_id: u32,
            event_emitter: [u8; 20],
            abi: Arc<EthEventAbi>,
            blocks_to_confirm: u16,
            check_token_root: bool,
        }

        // Get event configuration data
        let data = {
            let state = self.state.read().await;
            state
                .eth_ton_event_configurations
                .get(&event_init_data.configuration)
                .map(|configuration| ConfigData {
                    chain_id: configuration.details.network_configuration.chain_id,
                    event_emitter: configuration.details.network_configuration.event_emitter,
                    abi: configuration.event_abi.clone(),
                    blocks_to_confirm: configuration
                        .details
                        .network_configuration
                        .event_blocks_to_confirm,
                    check_token_root: configuration.mapping_context.check_token_root,
                })
        };

        // NOTE: be sure to drop `eth_event_configurations` lock before that
        let (
            eth_subscriber,
            event_emitter,
            event_abi,
            blocks_to_confirm,
            preliminary_checks_succeeded,
        ) = match data {
            // Configuration found
            Some(ConfigData {
                chain_id,
                event_emitter,
                abi,
                blocks_to_confirm,
                check_token_root,
            }) => {
                let mut preliminary_checks_succeeded = true;
                // Check token root if required
                if check_token_root {
                    tracing::info!(
                        event = %DisplayAddr(account),
                        chain_id,
                        "ETH->TON checking token root for token wallet",
                    );
                    let event_decoded_data = EthTonEventContract(&contract).event_decoded_data()?;

                    let token_root = event_decoded_data.token.address();
                    let token_root = UInt256::from_be_bytes(&token_root.get_bytestring(0));
                    let root_contract = ton_subscriber.wait_contract_state(token_root).await?;
                    #[cfg(feature = "ton")]
                    let proxy_wallet_address = JettonMinterContract(&root_contract)
                        .get_wallet_address(&event_decoded_data.proxy)?;
                    #[cfg(not(feature = "ton"))]
                    let proxy_wallet_address =
                        TokenRootContract(&root_contract).wallet_of(&event_decoded_data.proxy)?;

                    if event_decoded_data.token_wallet != proxy_wallet_address {
                        let proxy = UInt256::from_be_bytes(
                            &event_decoded_data.proxy.address().get_bytestring(0),
                        );
                        let expected = UInt256::from_be_bytes(
                            &proxy_wallet_address.address().get_bytestring(0),
                        );
                        let actual = UInt256::from_be_bytes(
                            &event_decoded_data.token_wallet.address().get_bytestring(0),
                        );
                        tracing::error!(
                            event = %DisplayAddr(account),
                            chain_id,
                            proxy = %DisplayAddr(proxy),
                            token_root = %DisplayAddr(token_root),
                            expected_token_wallet = %DisplayAddr(expected),
                            actual_token_wallet = %DisplayAddr(actual),
                            "ETH->TON wrong token wallet for given token root",
                        );
                        preliminary_checks_succeeded = false;
                    }
                }

                // Get required subscriber
                match eth_subscribers.get_subscriber(chain_id) {
                    Some(subscriber) => (
                        subscriber,
                        event_emitter,
                        abi,
                        blocks_to_confirm,
                        preliminary_checks_succeeded,
                    ),
                    None => {
                        tracing::error!(
                            event = %DisplayAddr(account),
                            chain_id,
                            "ETH->TON subscriber not found for event",
                        );
                        self.eth_ton_events_state.remove(&account);
                        return Ok(());
                    }
                }
            }
            // Configuration not found
            None => {
                tracing::error!(
                    event = %DisplayAddr(account),
                    configuration = %DisplayAddr(event_init_data.configuration),
                    "ETH->TON event configuration not found for event",
                );
                self.eth_ton_events_state.remove(&account);
                return Ok(());
            }
        };

        let account_addr = ton_block::MsgAddrStd::with_address(None, 0, account.into());

        // Verify ETH event and create message to event contract
        let message = match eth_subscriber
            .verify(
                event_init_data.vote_data,
                event_emitter,
                event_abi,
                blocks_to_confirm,
                preliminary_checks_succeeded,
            )
            .await
        {
            // Confirm event if transaction was found
            Ok(VerificationStatus::Exists) => {
                UnsignedMessage::new(eth_ton_event_contract::confirm(), account).arg(account_addr)
            }
            // Reject event if transaction not found
            Ok(VerificationStatus::NotExists { reason }) => {
                tracing::warn!(
                    event = %DisplayAddr(account),
                    configuration = %DisplayAddr(event_init_data.configuration),
                    reason,
                    "rejecting ETH->TON event",
                );

                UnsignedMessage::new(eth_ton_event_contract::reject(), account).arg(account_addr)
            }
            // Skip event otherwise
            Err(e) => {
                tracing::error!(event = %DisplayAddr(account), "failed to verify ETH->TON event: {e:?}");
                self.eth_ton_events_state.remove(&account);
                return Ok(());
            }
        };

        // Clone events observer and deliver message to the contract
        let eth_ton_event_observer = match self.eth_ton_events_state.pending.get(&account) {
            Some(entry) => entry.observer.clone(),
            None => return Ok(()),
        };
        let eth_ton_events_state = Arc::downgrade(&self.eth_ton_events_state);

        self.context
            .deliver_message(
                eth_ton_event_observer,
                message,
                // Stop voting for the contract if it was removed
                move || match eth_ton_events_state.upgrade() {
                    Some(state) => state.pending.contains_key(&account),
                    None => false,
                },
            )
            .await?;
        Ok(())
    }

    async fn update_ton_eth_event(self: Arc<Self>, account: UInt256) -> Result<()> {
        if !self.ton_eth_events_state.start_processing(&account) {
            return Ok(());
        }

        let keystore = &self.context.keystore;
        let ton_subscriber = &self.context.ton_subscriber;

        // Wait contract state
        let contract = ton_subscriber.wait_contract_state(account).await?;
        let base_event_contract = EventBaseContract(&contract);

        // Check further steps based on event statuses
        match base_event_contract.process(keystore.ton.public_key(), true)? {
            EventAction::Nop => return Ok(()),
            EventAction::Remove => {
                self.ton_eth_events_state.remove(&account);
                return Ok(());
            }
            EventAction::Vote => { /* continue voting */ }
        }
        let round_number = base_event_contract.round_number()?;

        // Get event details
        let event_init_data = TonEthEventContract(&contract).event_init_data()?;

        struct ConfigData {
            proxy: [u8; 20],
            data: Result<Vec<ton_abi::Token>>,
            #[cfg(feature = "ton")]
            verify_token_meta: bool,
        }

        // Find suitable configuration
        // NOTE: be sure to drop `self.state` lock before removing pending ton event.
        // It may deadlock otherwise!
        let data = {
            let state = self.state.read().await;
            state
                .ton_eth_event_configurations
                .get(&event_init_data.configuration)
                .map(|configuration| ConfigData {
                    proxy: configuration.details.network_configuration.proxy,
                    data: ton_types::SliceData::load_cell(
                        event_init_data.vote_data.event_data.clone(),
                    )
                    .and_then(|cursor| {
                        ton_abi::TokenValue::decode_params(
                            &configuration.event_abi,
                            cursor,
                            &ton_abi::contract::ABI_VERSION_2_2,
                            false,
                        )
                    }),
                    #[cfg(feature = "ton")]
                    verify_token_meta: configuration.context.verify_token_meta,
                })
        };

        let decoded_data = match data {
            // Decode event data with event abi from configuration
            Some(ConfigData {
                proxy,
                data,
                #[cfg(feature = "ton")]
                verify_token_meta,
            }) => {
                let mut verification_error = None;
                #[cfg(feature = "ton")]
                if verify_token_meta {
                    tracing::info!(
                        event = %DisplayAddr(account),
                        "TON->ETH checking token meta",
                    );
                    let event_decoded_data = TonEthEventContract(&contract).event_decoded_data()?;
                    let expected_meta = self
                        .context
                        .tokens_meta_client
                        .get_token_meta(&event_decoded_data.token.to_string())
                        .await?;

                    let mut meta_mismatch = false;
                    if event_decoded_data.name != expected_meta.name {
                        tracing::error!(
                            event = %DisplayAddr(account),
                            expected_token_name = expected_meta.name,
                            actual_token_name = event_decoded_data.name,
                            "TON->ETH token name mismatch",
                        );
                        meta_mismatch = true;
                    }
                    if event_decoded_data.symbol != expected_meta.symbol {
                        tracing::error!(
                            event = %DisplayAddr(account),
                            expected_token_symbol = expected_meta.symbol,
                            actual_token_symbol = event_decoded_data.symbol,
                            "TON->ETH token symbol mismatch",
                        );
                        meta_mismatch = true;
                    }
                    if event_decoded_data.decimals != expected_meta.decimals {
                        tracing::error!(
                            event = %DisplayAddr(account),
                            expected_token_decimals = expected_meta.decimals,
                            actual_token_decimals = event_decoded_data.decimals,
                            "TON->ETH token decimals mismatch",
                        );
                        meta_mismatch = true;
                    }

                    if meta_mismatch {
                        verification_error = Some(BridgeError::TokenMetadataMismatch.into());
                    }
                }

                if let Some(err) = verification_error {
                    Err(err)
                } else {
                    data.and_then(|data| {
                        Ok(make_mapped_ton_event(
                            event_init_data.vote_data.event_transaction_lt,
                            event_init_data.vote_data.event_timestamp,
                            map_ton_tokens_to_eth_bytes(data)?,
                            event_init_data.configuration,
                            account,
                            proxy,
                            round_number,
                        ))
                    })
                }
            }
            // Do nothing when configuration was not found
            None => {
                tracing::error!(
                    event = %DisplayAddr(account),
                    configuration = %DisplayAddr(event_init_data.configuration),
                    "TON->ETH event configuration not found for event",
                );
                self.ton_eth_events_state.remove(&account);
                return Ok(());
            }
        };

        let account_addr = ton_block::MsgAddrStd::with_address(None, 0, account.into());

        let message = match decoded_data {
            // Confirm with signature
            Ok(data) => {
                tracing::info!(
                    event = %DisplayAddr(account),
                    data = hex::encode(&data),
                    "signing event data"
                );
                UnsignedMessage::new(ton_eth_event_contract::confirm(), account)
                    .arg(keystore.eth.sign(&data).to_vec())
                    .arg(account_addr)
            }

            // Reject if event data is invalid
            Err(e) => {
                tracing::warn!(
                    event = %DisplayAddr(account),
                    "failed to compute vote data signature: {e:?}",
                );
                UnsignedMessage::new(ton_eth_event_contract::reject(), account).arg(account_addr)
            }
        };

        // Clone events observer and deliver message to the contract
        let ton_eth_event_observer = match self.ton_eth_events_state.pending.get(&account) {
            Some(entry) => entry.observer.clone(),
            None => return Ok(()),
        };
        let ton_eth_events_state = Arc::downgrade(&self.ton_eth_events_state);

        self.context
            .deliver_message(
                ton_eth_event_observer,
                message,
                // Stop voting for the contract if it was removed
                move || match ton_eth_events_state.upgrade() {
                    Some(state) => state.pending.contains_key(&account),
                    None => false,
                },
            )
            .await?;
        Ok(())
    }

    async fn update_sol_ton_event(self: Arc<Self>, account: UInt256) -> Result<()> {
        let sol_subscriber = match &self.context.sol_subscriber {
            // Continue only of SOL subscriber is enabled and it is the first time we started processing this event
            Some(sol_subscriber) if self.sol_ton_events_state.start_processing(&account) => {
                sol_subscriber
            }
            _ => return Ok(()),
        };

        let keystore = &self.context.keystore;
        let ton_subscriber = &self.context.ton_subscriber;

        // Wait contract state
        let contract = ton_subscriber.wait_contract_state(account).await?;

        match EventBaseContract(&contract).process(keystore.ton.public_key(), false)? {
            EventAction::Nop => return Ok(()),
            EventAction::Remove => {
                self.sol_ton_events_state.remove(&account);
                return Ok(());
            }
            EventAction::Vote => { /* continue voting */ }
        }

        let event_init_data = SolTonEventContract(&contract).event_init_data()?;

        // Find suitable configuration
        // NOTE: be sure to drop `self.state` lock before removing pending ton event.
        // It may deadlock otherwise!
        let data = {
            let state = self.state.read().await;
            state
                .sol_ton_event_configurations
                .get(&event_init_data.configuration)
                .map(|configuration| {
                    (
                        configuration.details.network_configuration.program,
                        ton_types::SliceData::load_cell(
                            event_init_data.vote_data.event_data.clone(),
                        )
                        .and_then(|cursor| {
                            TokenValue::decode_params(
                                &configuration.event_abi,
                                cursor,
                                &ton_abi::contract::ABI_VERSION_2_2,
                                false,
                            )
                        }),
                    )
                })
        };

        let (program, decoded_event_data) = match data {
            // Decode event data with event abi from configuration
            Some((program, data)) => (
                program,
                data.and_then(|data| eth_ton_abi_converter::borsh::serialize(&data))?,
            ),
            // Do nothing when configuration was not found
            None => {
                tracing::error!(
                    event = %DisplayAddr(account),
                    configuration = %DisplayAddr(event_init_data.configuration),
                    "SOL->TON event configuration not found for event",
                );
                self.sol_ton_events_state.remove(&account);
                return Ok(());
            }
        };

        let signature =
            solana_sdk::signature::Signature::from_str(&event_init_data.vote_data.signature)?;

        let program_id = Pubkey::new_from_array(program.inner());

        let transaction_data = SolTonTransactionData {
            program_id,
            signature,
            slot: event_init_data.vote_data.slot,
            block_time: event_init_data.vote_data.block_time as i64,
            seed: event_init_data.vote_data.account_seed,
        };

        let account_data = SolTonAccountData {
            program_id,
            seed: event_init_data.vote_data.account_seed,
            event_data: decoded_event_data,
        };

        let account_addr = ton_block::MsgAddrStd::with_address(None, 0, account.into());

        // Verify SOL->TON event and create message to event contract
        let message = match sol_subscriber
            .verify_sol_ton_event(transaction_data, account_data)
            .await
        {
            // Confirm event if transaction was found
            Ok(VerificationStatus::Exists) => {
                UnsignedMessage::new(sol_ton_event_contract::confirm(), account).arg(account_addr)
            }
            // Reject event if transaction not found
            Ok(VerificationStatus::NotExists { reason }) => {
                tracing::warn!(
                    event = %DisplayAddr(account),
                    configuration = %DisplayAddr(event_init_data.configuration),
                    reason,
                    "rejecting SOL->TON event",
                );

                UnsignedMessage::new(sol_ton_event_contract::reject(), account).arg(account_addr)
            }
            // Skip event otherwise
            Err(e) => {
                tracing::error!(
                    event = %DisplayAddr(account),
                    "failed to verify SOL->TON event: {e:?}",
                );
                self.sol_ton_events_state.remove(&account);
                return Ok(());
            }
        };

        // Clone events observer and deliver message to the contract
        let sol_ton_event_observer = match self.sol_ton_events_state.pending.get(&account) {
            Some(entry) => entry.observer.clone(),
            None => return Ok(()),
        };
        let sol_ton_events_state = Arc::downgrade(&self.sol_ton_events_state);

        self.context
            .deliver_message(
                sol_ton_event_observer,
                message,
                // Stop voting for the contract if it was removed
                move || match sol_ton_events_state.upgrade() {
                    Some(state) => state.pending.contains_key(&account),
                    None => false,
                },
            )
            .await?;

        Ok(())
    }

    async fn update_ton_sol_event(self: Arc<Self>, account: UInt256) -> Result<()> {
        let sol_subscriber = match &self.context.sol_subscriber {
            // Continue only of SOL subscriber is enabled and it is the first time we started processing this event
            Some(sol_subscriber) if self.ton_sol_events_state.start_processing(&account) => {
                sol_subscriber
            }
            _ => return Ok(()),
        };

        let keystore = &self.context.keystore;
        let ton_subscriber = &self.context.ton_subscriber;

        // Wait contract state
        let contract = ton_subscriber.wait_contract_state(account).await?;
        let base_event_contract = EventBaseContract(&contract);

        // Check further steps based on event statuses
        match base_event_contract.process(keystore.ton.public_key(), true)? {
            EventAction::Nop => return Ok(()),
            EventAction::Remove => {
                self.ton_sol_events_state.remove(&account);
                return Ok(());
            }
            EventAction::Vote => { /* continue voting */ }
        }
        let round_number = base_event_contract.round_number()?;
        let created_at = base_event_contract.created_at()?;

        // Get event details
        let event_init_data = TonSolEventContract(&contract).event_init_data()?;

        // Find suitable configuration
        // NOTE: be sure to drop `self.state` lock before removing pending ton event.
        // It may deadlock otherwise!
        let data = {
            let state = self.state.read().await;
            state
                .ton_sol_event_configurations
                .get(&event_init_data.configuration)
                .map(|configuration| {
                    (
                        configuration.details.network_configuration.program,
                        configuration.details.network_configuration.instruction,
                        configuration.details.network_configuration.execute_needed,
                        configuration
                            .details
                            .network_configuration
                            .execute_instruction,
                        configuration
                            .details
                            .network_configuration
                            .execute_payload_instruction,
                        ton_types::SliceData::load_cell(
                            event_init_data.vote_data.event_data.clone(),
                        )
                        .and_then(|cursor| {
                            TokenValue::decode_params(
                                &configuration.event_abi,
                                cursor,
                                &ton_abi::contract::ABI_VERSION_2_2,
                                false,
                            )
                        }),
                    )
                })
        };

        let (
            program_id,
            instruction,
            execute_needed,
            execute_instruction,
            execute_payload_instruction,
            decoded_event_data,
        ) = match data {
            // Decode event data with event abi from configuration
            Some((
                program,
                instruction,
                execute_needed,
                execute_instruction,
                execute_payload_instruction,
                data,
            )) => (
                Pubkey::new_from_array(program.inner()),
                instruction,
                execute_needed,
                execute_instruction,
                execute_payload_instruction,
                data.and_then(|data| eth_ton_abi_converter::borsh::serialize(&data))?,
            ),
            // Do nothing when configuration was not found
            None => {
                tracing::error!(
                    event = %DisplayAddr(account),
                    configuration = %DisplayAddr(event_init_data.configuration),
                    "TON->SOL event configuration not found for event",
                );
                self.ton_sol_events_state.remove(&account);
                return Ok(());
            }
        };

        let event_configuration = Pubkey::new_from_array(event_init_data.configuration.inner());
        let event_data = solana_sdk::hash::hash(&decoded_event_data);

        let proposal_pubkey = solana_bridge::bridge_helper::get_associated_proposal_address(
            &program_id,
            round_number,
            event_init_data.vote_data.event_timestamp,
            event_init_data.vote_data.event_transaction_lt,
            &event_configuration,
            &event_data.to_bytes(),
        );

        let voter_pubkey = self.context.keystore.sol.public_key();

        let account_addr = ton_block::MsgAddrStd::with_address(None, 0, account.into());

        let (sol_message_vote, sol_message_execute, sol_message_execute_payload, ton_message) =
            match sol_subscriber
                .verify_ton_sol_event(proposal_pubkey, decoded_event_data, created_at)
                .await
            {
                // Confirm event if transaction was found
                Ok(VerificationStatus::Exists) => {
                    let vote_ix = solana_bridge::instructions::vote_for_proposal_ix(
                        program_id,
                        instruction,
                        &voter_pubkey,
                        &proposal_pubkey,
                        round_number,
                        solana_bridge::bridge_types::Vote::Confirm,
                    );

                    let sol_message_vote =
                        solana_sdk::message::Message::new(&[vote_ix], Some(&voter_pubkey));

                    let mut sol_message_execute = None;
                    let mut sol_message_execute_payload = None;
                    if execute_needed {
                        let accounts = event_init_data
                            .vote_data
                            .execute_accounts
                            .into_iter()
                            .map(|account| {
                                (
                                    Pubkey::new_from_array(account.account.inner()),
                                    account.read_only,
                                    account.is_signer,
                                )
                            })
                            .collect();

                        let execute_ix = solana_bridge::instructions::execute_proposal_ix(
                            program_id,
                            execute_instruction,
                            proposal_pubkey,
                            accounts,
                        );

                        sol_message_execute = Some(solana_sdk::message::Message::new(
                            &[execute_ix],
                            Some(&voter_pubkey),
                        ));

                        if event_init_data.vote_data.execute_payload_needed {
                            let accounts = event_init_data
                                .vote_data
                                .execute_payload_accounts
                                .into_iter()
                                .map(|account| {
                                    (
                                        Pubkey::new_from_array(account.account.inner()),
                                        account.read_only,
                                        account.is_signer,
                                    )
                                })
                                .collect();

                            let execute_ix = solana_bridge::instructions::execute_payload_ix(
                                program_id,
                                execute_payload_instruction,
                                proposal_pubkey,
                                accounts,
                            );

                            sol_message_execute_payload = Some(solana_sdk::message::Message::new(
                                &[execute_ix],
                                Some(&voter_pubkey),
                            ));
                        }
                    }

                    let ton_message =
                        UnsignedMessage::new(ton_sol_event_contract::confirm(), account)
                            .arg(account_addr);

                    (
                        sol_message_vote,
                        sol_message_execute,
                        sol_message_execute_payload,
                        ton_message,
                    )
                }
                Ok(VerificationStatus::NotExists { reason }) => {
                    tracing::warn!(
                        event = %DisplayAddr(account),
                        configuration = %DisplayAddr(event_init_data.configuration),
                        reason,
                        "rejecting TON->SOL event",
                    );

                    let ix = solana_bridge::instructions::vote_for_proposal_ix(
                        program_id,
                        instruction,
                        &voter_pubkey,
                        &proposal_pubkey,
                        round_number,
                        solana_bridge::bridge_types::Vote::Reject,
                    );
                    let sol_message = solana_sdk::message::Message::new(&[ix], Some(&voter_pubkey));

                    let ton_message =
                        UnsignedMessage::new(ton_sol_event_contract::reject(), account)
                            .arg(account_addr);

                    (sol_message, None, None, ton_message)
                }
                // Skip event otherwise
                Err(e) => {
                    tracing::error!(
                        event = %DisplayAddr(account),
                        "failed to verify TON->SOL event: {e:?}",
                    );
                    self.ton_sol_events_state.remove(&account);
                    return Ok(());
                }
            };

        let rpc_client = sol_subscriber.get_rpc_client()?;

        if !sol_subscriber
            .is_already_voted(rpc_client, round_number, &proposal_pubkey, &voter_pubkey)
            .await?
        {
            // Extract vote and log it
            let c_ix = sol_message_vote.instructions.first().trust_me();
            let ix = solana_bridge::instructions::VoteForProposal::try_from_slice(&c_ix.data)?;

            tracing::info!(
                vote = ?ix.vote,
                %proposal_pubkey,
                "voting for solana proposal...",
            );

            // Send confirm/reject to Solana
            let signature = sol_subscriber
                .send_message(rpc_client, sol_message_vote, &self.context.keystore)
                .await
                .map_err(parse_client_error)?;

            sol_subscriber
                .get_signature_status(rpc_client, &signature)
                .await
                .map_err(parse_client_error)?;

            tracing::info!(
                vote = ?ix.vote,
                %proposal_pubkey,
                "vote was sent",
            );

            // Execute proposal
            if let Some(message) = sol_message_execute {
                tracing::info!(
                    %proposal_pubkey,
                    "executing proposal...",
                );

                match sol_subscriber
                    .send_message(rpc_client, message, &self.context.keystore)
                    .await
                    .map_err(parse_client_error)
                {
                    Ok(_) => {
                        tracing::info!(
                            %proposal_pubkey,
                            "proposal was executed",
                        );

                        // Execute payload
                        if let Some(message) = sol_message_execute_payload {
                            tracing::info!(
                                %proposal_pubkey,
                                "executing payload...",
                            );

                            match sol_subscriber
                                .send_message(rpc_client, message, &self.context.keystore)
                                .await
                                .map_err(parse_client_error)
                            {
                                Ok(_) => {
                                    tracing::info!(
                                        %proposal_pubkey,
                                        "payload was executed",
                                    );
                                }
                                Err(e) => {
                                    tracing::error!(
                                        %proposal_pubkey,
                                        "failed to execute solana payload: {e:?}",
                                    );
                                }
                            }
                        }
                    }
                    Err(e) => {
                        tracing::error!(
                            %proposal_pubkey,
                            "failed to execute solana proposal: {e:?}",
                        );
                    }
                }
            }
        }

        // Clone events observer and deliver message to the contract
        let ton_sol_event_observer = match self.ton_sol_events_state.pending.get(&account) {
            Some(entry) => entry.observer.clone(),
            None => return Ok(()),
        };
        let ton_sol_events_state = Arc::downgrade(&self.ton_sol_events_state);

        self.context
            .deliver_message(
                ton_sol_event_observer,
                ton_message,
                // Stop voting for the contract if it was removed
                move || match ton_sol_events_state.upgrade() {
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
            tracing::info!(
                connector = %DisplayAddr(connector_account),
                ?connector_details,
                "got connector details",
            );

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
        tracing::info!(
            configuration = %DisplayAddr(event_configuration),
            connector = %DisplayAddr(connector_account),
            "got configuration contract",
        );

        // Extract and process info from contract
        let mut state = self.state.write().await;
        self.process_event_configuration(
            &mut state,
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
                        tracing::error!(
                            connector = %DisplayAddr(connector_account),
                            "failed to get connector details: {e:?}",
                        );
                        continue;
                    }
                },
                None => {
                    tracing::error!(
                        connector = %DisplayAddr(connector_account),
                        "connector not found",
                    );
                    continue;
                }
            };
            tracing::info!(
                connector = %DisplayAddr(connector_account),
                id,
                ?details,
                "found configuration connector",
            );

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
                        tracing::warn!(
                            connector = %DisplayAddr(connector_account),
                            configuration = %DisplayAddr(details.event_configuration),
                            "connected configuration not found",
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
                tracing::error!(
                    connector = %DisplayAddr(connector_account),
                    configuration = %DisplayAddr(details.event_configuration),
                    "failed to process event configuration: {e:?}",
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
        tracing::info!(
            connector = %DisplayAddr(connector_account),
            configuration = %DisplayAddr(configuration_account),
            ?event_type,
            "found new configuration contract"
        );

        if !state.connectors.contains_key(connector_account) {
            return Err(BridgeError::UnknownConnector.into());
        }

        match event_type {
            // Extract and populate ETH->TON event configuration details
            EventType::EthTon => self
                .add_eth_ton_event_configuration(
                    state,
                    configuration_account,
                    configuration_contract,
                )
                .context("Failed to add ETH event configuration")?,
            // Extract and populate TON->ETH event configuration details
            EventType::TonEth => self
                .add_ton_eth_event_configuration(
                    state,
                    configuration_account,
                    configuration_contract,
                )
                .context("Failed to add TON->ETH event configuration")?,
            // Extract and populate SOL->TON event configuration details
            EventType::SolTon => self
                .add_sol_ton_event_configuration(
                    state,
                    configuration_account,
                    configuration_contract,
                )
                .context("Failed to add SOL->TON event configuration")?,
            // Extract and populate TON->SOL event configuration details
            EventType::TonSol => self
                .add_ton_sol_event_configuration(
                    state,
                    configuration_account,
                    configuration_contract,
                )
                .context("Failed to add TON->SOL event configuration")?,
        };

        // Done
        Ok(())
    }

    fn add_eth_ton_event_configuration(
        &self,
        state: &mut BridgeState,
        account: &UInt256,
        contract: &ExistingContract,
    ) -> Result<()> {
        let flags = EventConfigurationBaseContract(contract)
            .get_flags()
            .context("Failed to get ETH->TON event configuration flags")?;

        // Get configuration details
        let details = EthTonEventConfigurationContract(contract)
            .get_details()
            .context("Failed to get ETH->TON event configuration details")?;

        let ctx = flags
            .map(|flags| EthToTonMappingContext::from(flags as u8))
            .unwrap_or_default();

        // Verify and prepare abi
        let event_abi = Arc::new(EthEventAbi::new(
            &details.basic_configuration.event_abi,
            ctx,
        )?);
        let topic_hash = event_abi.get_eth_topic_hash().to_fixed_bytes();
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
            EventType::EthTon,
        )?;

        // Add configuration entry
        let observer = AccountObserver::new(&self.eth_ton_event_configurations_tx);
        match state.eth_ton_event_configurations.entry(*account) {
            hash_map::Entry::Vacant(entry) => {
                tracing::info!(
                    configuration = %DisplayAddr(account),
                    ?details, "added new ETH->TON event configuration"
                );

                self.total_active_eth_ton_event_configurations
                    .fetch_add(1, Ordering::Release);

                entry.insert(EthTonEventConfigurationState {
                    details,
                    event_abi,
                    mapping_context: ctx,
                    _observer: observer.clone(),
                });
            }
            hash_map::Entry::Occupied(_) => {
                tracing::info!(
                    configuration = %DisplayAddr(account),
                    "ETH->TON event configuration already exists",
                );
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

    fn add_ton_eth_event_configuration(
        &self,
        state: &mut BridgeState,
        account: &UInt256,
        contract: &ExistingContract,
    ) -> Result<()> {
        #[cfg(feature = "ton")]
        let flags = EventConfigurationBaseContract(contract)
            .get_flags()
            .context("Failed to get TON->ETH event configuration flags")?;

        // Get configuration details
        let details = TonEthEventConfigurationContract(contract)
            .get_details()
            .context("Failed to get TON->ETH event configuration details")?;

        #[cfg(feature = "ton")]
        let ctx = flags
            .map(|flags| eth_ton_abi_converter::TonToEthContext::from(flags as u8))
            .unwrap_or_default();

        // Check if configuration is expired
        let current_timestamp = self.context.ton_subscriber.current_utime();
        if details.is_expired(current_timestamp) {
            // Do nothing in that case
            tracing::warn!(
                configuration = %DisplayAddr(account),
                current_timestamp,
                end_timestamp = details.network_configuration.end_timestamp,
                "ignoring disabled TON->ETH event configuration",
            );
            return Ok(());
        };

        // Verify and prepare abi
        let event_abi = decode_ton_event_abi(&details.basic_configuration.event_abi)?;

        // Add unique event hash
        add_event_code_hash(
            &mut state.event_code_hashes,
            &details.basic_configuration.event_code,
            EventType::TonEth,
        )?;

        // Add configuration entry
        let observer = AccountObserver::new(&self.ton_eth_event_configurations_tx);
        match state.ton_eth_event_configurations.entry(*account) {
            hash_map::Entry::Vacant(entry) => {
                tracing::info!(
                    configuration = %DisplayAddr(account),
                    ?details,
                    "added new TON->ETH event configuration"
                );

                self.total_active_ton_eth_event_configurations
                    .fetch_add(1, Ordering::Release);

                entry.insert(TonEthEventConfigurationState {
                    details,
                    #[cfg(feature = "ton")]
                    context: ctx,
                    event_abi,
                    _observer: observer.clone(),
                });
            }
            hash_map::Entry::Occupied(_) => {
                tracing::info!(
                    configuration = %DisplayAddr(account),
                    "TON->ETH event configuration already exists",
                );
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

    fn add_sol_ton_event_configuration(
        &self,
        state: &mut BridgeState,
        account: &UInt256,
        contract: &ExistingContract,
    ) -> Result<()> {
        if self.context.sol_subscriber.is_none() {
            tracing::info!(
                configuration = %DisplayAddr(account),
                "ignoring SOL->TON event configuration: Solana subscriber is disabled",
            );
            return Ok(());
        }

        // Get configuration details
        let details = SolTonEventConfigurationContract(contract)
            .get_details()
            .context("Failed to get SOL->TON event configuration details")?;

        // Check if configuration is expired
        let current_timestamp = self.context.ton_subscriber.current_utime();
        if details.is_expired(current_timestamp as u64) {
            // Do nothing in that case
            tracing::warn!(
                configuration = %DisplayAddr(account),
                current_timestamp,
                end_timestamp = details.network_configuration.end_timestamp,
                "ignoring disabled SOL->TON event configuration",
            );
            return Ok(());
        };

        // Verify and prepare abi
        let event_abi = decode_ton_event_abi(&details.basic_configuration.event_abi)?;

        // Add unique event hash
        add_event_code_hash(
            &mut state.event_code_hashes,
            &details.basic_configuration.event_code,
            EventType::SolTon,
        )?;

        // Add configuration entry
        let observer = AccountObserver::new(&self.sol_ton_event_configurations_tx);
        match state.sol_ton_event_configurations.entry(*account) {
            hash_map::Entry::Vacant(entry) => {
                tracing::info!(
                    configuration = %DisplayAddr(account),
                    ?details,
                    "added new SOl->TON event configuration",
                );

                self.total_active_sol_ton_event_configurations
                    .fetch_add(1, Ordering::Release);

                entry.insert(SolTonEventConfigurationState {
                    details,
                    event_abi,
                    _observer: observer.clone(),
                });
            }
            hash_map::Entry::Occupied(_) => {
                tracing::info!(
                    configuration = %DisplayAddr(account),
                    "SOl->TON event configuration already exists",
                );
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

    fn add_ton_sol_event_configuration(
        &self,
        state: &mut BridgeState,
        account: &UInt256,
        contract: &ExistingContract,
    ) -> Result<()> {
        let sol_subscriber = match &self.context.sol_subscriber {
            Some(sol_subscriber) => sol_subscriber,
            None => {
                tracing::info!(
                    configuration = %DisplayAddr(account),
                    "ignoring TON->SOL event configuration: Solana subscriber is disabled",
                );
                return Ok(());
            }
        };

        // Get configuration details
        let details = TonSolEventConfigurationContract(contract)
            .get_details()
            .context("Failed to get TON->SOL event configuration details")?;

        // Check if configuration is expired
        let current_timestamp = self.context.ton_subscriber.current_utime();
        if details.is_expired(current_timestamp) {
            // Do nothing in that case
            tracing::warn!(
                configuration = %DisplayAddr(account),
                current_timestamp,
                end_timestamp = details.network_configuration.end_timestamp,
                "ignoring disabled TON->SOL event configuration",
            );
            return Ok(());
        };

        // Verify and prepare abi
        let event_abi = decode_ton_event_abi(&details.basic_configuration.event_abi)?;

        // Add unique event hash
        add_event_code_hash(
            &mut state.event_code_hashes,
            &details.basic_configuration.event_code,
            EventType::TonSol,
        )?;

        // Get solana program address to subscribe
        let program_pubkey = Pubkey::new_from_array(details.network_configuration.program.inner());

        // Add configuration entry
        let observer = AccountObserver::new(&self.ton_sol_event_configurations_tx);
        match state.ton_sol_event_configurations.entry(*account) {
            hash_map::Entry::Vacant(entry) => {
                tracing::info!(
                    configuration = %DisplayAddr(account),
                    ?details,
                    "added new TON->SOL event configuration",
                );

                self.total_active_ton_sol_event_configurations
                    .fetch_add(1, Ordering::Release);

                entry.insert(TonSolEventConfigurationState {
                    details,
                    event_abi,
                    _observer: observer.clone(),
                });
            }
            hash_map::Entry::Occupied(_) => {
                tracing::info!(
                    configuration = %DisplayAddr(account),
                    "TON->SOL event configuration already exists"
                );
                return Err(BridgeError::EventConfigurationAlreadyExists.into());
            }
        };

        // Subscribe to Solana programs
        sol_subscriber.subscribe(program_pubkey);

        // Subscribe to TON events
        self.context
            .ton_subscriber
            .add_transactions_subscription([*account], &observer);

        // Done
        Ok(())
    }

    async fn get_all_events(self: &Arc<Self>) -> Result<()> {
        type AccountsSet = FxHashSet<UInt256>;

        fn extract_address(bytes: &[u8]) -> Result<ton_block::MsgAddressInt> {
            if bytes.len() != 33 {
                anyhow::bail!("invalid address")
            }

            let workchain_id = bytes[0] as i8;
            let address = ton_types::AccountId::from(<[u8; 32]>::try_from(&bytes[1..33])?);

            ton_block::MsgAddressInt::with_standart(None, workchain_id, address)
        }

        #[allow(clippy::too_many_arguments)]
        fn iterate_events(
            bridge: Arc<Bridge>,
            code_hash: UInt256,
            event_type: EventType,
            snapshot: Arc<OwnedSnapshot>,
            unique_eth_ton_event_configurations: Arc<AccountsSet>,
            unique_ton_eth_event_configurations: Arc<AccountsSet>,
            unique_sol_ton_event_configurations: Arc<AccountsSet>,
            unique_ton_sol_event_configurations: Arc<AccountsSet>,
        ) -> Result<()> {
            let our_public_key = bridge.context.keystore.ton.public_key();
            let has_sol_subscriber = bridge.context.sol_subscriber.is_some();

            let mut key = [0u8; { tables::CodeHashes::KEY_LEN }];
            key[0..32].copy_from_slice(code_hash.as_slice());

            let mut upper_bound = Vec::with_capacity(tables::CodeHashes::KEY_LEN);
            upper_bound.extend_from_slice(&key[..32]);
            upper_bound.extend_from_slice(&[0xff; 33]);

            let mut readopts = bridge
                .context
                .persistent_storage
                .code_hashes
                .new_read_config();
            readopts.set_snapshot(&snapshot);
            readopts.set_iterate_upper_bound(upper_bound); // NOTE: somehow make the range inclusive

            let code_hashes_cf = bridge.context.persistent_storage.code_hashes.cf();
            let mut iter = bridge
                .context
                .persistent_storage
                .inner
                .raw()
                .raw_iterator_cf_opt(&code_hashes_cf, readopts);

            iter.seek(key);
            while let Some(key) = iter.key() {
                if key.len() != tables::CodeHashes::KEY_LEN {
                    tracing::warn!(
                        code_hash = %DisplayAddr(code_hash),
                        "invalid code hash key length"
                    );

                    iter.next();
                    continue;
                }

                let address = extract_address(&key[32..])?;
                let hash = UInt256::from_be_bytes(&address.address().get_bytestring(0));

                let contract = bridge
                    .context
                    .runtime_storage
                    .get_contract_state(&hash)?
                    .ok_or(BridgeError::AccountNotFound(hash.to_hex_string()))?;

                macro_rules! check_configuration {
                    ($contract: ident) => {
                        match $contract(&contract).event_init_data() {
                            Ok(init_data) => init_data.configuration,
                            Err(e) => {
                                tracing::info!(
                                    event = %DisplayAddr(hash),
                                    ?event_type,
                                    "failed to get event init data: {e:?}"
                                );
                                iter.next();
                                continue;
                            }
                        }
                    };
                }

                // Process event
                match EventBaseContract(&contract)
                    .process(our_public_key, event_type == EventType::TonEth)
                {
                    Ok(EventAction::Nop | EventAction::Vote) => match event_type {
                        EventType::EthTon => {
                            let configuration = check_configuration!(EthTonEventContract);

                            if !unique_eth_ton_event_configurations.contains(&configuration) {
                                tracing::warn!(
                                    event = %DisplayAddr(hash),
                                    configuration = %DisplayAddr(configuration),
                                    "ETH->TON event configuration not found"
                                );
                                iter.next();
                                continue;
                            }

                            if bridge.add_pending_event(hash, &bridge.eth_ton_events_state) {
                                bridge.spawn_background_task(
                                    "initial update ETH->TON event",
                                    bridge.clone().update_eth_ton_event(hash),
                                );
                            }
                        }
                        EventType::TonEth => {
                            let configuration = check_configuration!(TonEthEventContract);

                            if !unique_ton_eth_event_configurations.contains(&configuration) {
                                tracing::warn!(
                                    event = %DisplayAddr(hash),
                                    configuration = %DisplayAddr(configuration),
                                    "TON->ETH event configuration not found",
                                );
                                iter.next();
                                continue;
                            }

                            if bridge.add_pending_event(hash, &bridge.ton_eth_events_state) {
                                bridge.spawn_background_task(
                                    "initial update TON->ETH event",
                                    bridge.clone().update_ton_eth_event(hash),
                                );
                            }
                        }
                        EventType::SolTon if has_sol_subscriber => {
                            let configuration = check_configuration!(SolTonEventContract);

                            if !unique_sol_ton_event_configurations.contains(&configuration) {
                                tracing::warn!(
                                    event = %DisplayAddr(hash),
                                    configuration = %DisplayAddr(configuration),
                                    "SOL->TON event configuration not found",
                                );
                                iter.next();
                                continue;
                            }

                            if bridge.add_pending_event(hash, &bridge.sol_ton_events_state) {
                                bridge.spawn_background_task(
                                    "initial update SOL->TON event",
                                    bridge.clone().update_sol_ton_event(hash),
                                );
                            }
                        }
                        EventType::TonSol if has_sol_subscriber => {
                            let configuration = check_configuration!(TonSolEventContract);

                            if !unique_ton_sol_event_configurations.contains(&configuration) {
                                tracing::warn!(
                                    event = %DisplayAddr(hash),
                                    configuration = %DisplayAddr(configuration),
                                    "TON->SOL event configuration not found",
                                );
                                iter.next();
                                continue;
                            }

                            if bridge.add_pending_event(hash, &bridge.ton_sol_events_state) {
                                bridge.spawn_background_task(
                                    "initial update TON->SOL event",
                                    bridge.clone().update_ton_sol_event(hash),
                                );
                            }
                        }
                        _ => {}
                    },
                    Ok(EventAction::Remove) => { /* do nothing */ }
                    Err(e) => {
                        tracing::error!(
                            event = %DisplayAddr(hash),
                            ?event_type,
                            "failed to get event details: {e:?}",
                        );
                    }
                }

                iter.next();
            }

            Ok(())
        }

        // Lock state to prevent adding new configurations
        let state = self.state.read().await;

        let Some(snapshot) = self.context.persistent_storage.load_snapshot() else {
            return Err(BridgeError::StorageNotReady.into());
        };

        // Prepare shard task context
        let event_code_hashes = &state.event_code_hashes;

        // NOTE: configuration sets are explicitly constructed from state instead of
        // just using [eth/ton]_event_counters. It is done on purpose to use the actual
        // configurations. It is acceptable that event counters will not be relevant
        let unique_eth_ton_event_configurations =
            Arc::new(state.unique_eth_ton_event_configurations());
        let unique_ton_eth_event_configurations =
            Arc::new(state.unique_ton_eth_event_configurations());
        let unique_sol_ton_event_configurations =
            Arc::new(state.unique_sol_ton_event_configurations());
        let unique_ton_sol_event_configurations =
            Arc::new(state.unique_ton_sol_event_configurations());

        let start = std::time::Instant::now();

        tracing::info!("started searching for all events");
        let mut results_rx = {
            let (results_tx, results_rx) = mpsc::unbounded_channel();

            for (code_hash, event_type) in event_code_hashes {
                let code_hash = *code_hash;
                let event_type = *event_type;

                let bridge = self.clone();
                let snapshot = snapshot.clone();
                let results_tx = results_tx.clone();

                let unique_eth_ton_event_configurations =
                    unique_eth_ton_event_configurations.clone();
                let unique_ton_eth_event_configurations =
                    unique_ton_eth_event_configurations.clone();
                let unique_sol_ton_event_configurations =
                    unique_sol_ton_event_configurations.clone();
                let unique_ton_sol_event_configurations =
                    unique_ton_sol_event_configurations.clone();

                tokio::spawn(tokio::task::spawn_blocking(move || {
                    let start = std::time::Instant::now();
                    let result = iterate_events(
                        bridge,
                        code_hash,
                        event_type,
                        snapshot,
                        unique_eth_ton_event_configurations,
                        unique_ton_eth_event_configurations,
                        unique_sol_ton_event_configurations,
                        unique_ton_sol_event_configurations,
                    );
                    tracing::info!(
                        code_hash = %DisplayAddr(code_hash),
                        elapsed_sec = start.elapsed().as_secs(),
                        "processed accounts",
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
        tracing::info!(
            elapsed_sec = start.elapsed().as_secs(),
            "finished iterating all events",
        );
        Ok(())
    }

    fn start_event_configurations_gc(self: &Arc<Self>) {
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
                let has_expired_ton_eth_configurations = {
                    let state = bridge.state.read().await;
                    state.has_expired_ton_eth_event_configurations(current_utime)
                };

                let has_expired_ton_sol_configurations = {
                    let state = bridge.state.read().await;
                    state.has_expired_ton_sol_event_configurations(current_utime)
                };

                let has_expired_sol_ton_configurations = {
                    let state = bridge.state.read().await;
                    state.has_expired_sol_ton_event_configurations(current_utime)
                };

                // Do nothing if there are not expired configurations
                if !has_expired_ton_eth_configurations
                    && !has_expired_ton_sol_configurations
                    && !has_expired_sol_ton_configurations
                {
                    continue;
                }

                // Wait all shards
                let current_utime = match ton_subscriber.wait_shards(None).await {
                    Ok(shards) => {
                        for (_, block_id) in shards.block_ids {
                            if let Err(e) = ton_engine.wait_state(&block_id, None, false).await {
                                tracing::error!(%block_id, "failed to wait for shard state: {e:?}");
                                continue 'outer;
                            }
                        }
                        shards.current_utime
                    }
                    Err(e) => {
                        tracing::error!("failed to wait for current shards info: {e:?}");
                        continue;
                    }
                };

                let mut state = bridge.state.write().await;

                // Remove TON->ETH expired configurations
                let mut total_removed = 0;
                state.ton_eth_event_configurations.retain(|account, state| {
                    if state.details.is_expired(current_utime) {
                        tracing::warn!(
                            configuration = %DisplayAddr(account),
                            current_utime,
                            "removing TON->ETH event configuration",
                        );
                        total_removed += 1;
                        false
                    } else {
                        true
                    }
                });
                bridge
                    .total_active_ton_eth_event_configurations
                    .fetch_sub(total_removed, Ordering::Release);

                // Remove TON->SOL expired configurations
                let mut total_removed = 0;
                state.ton_sol_event_configurations.retain(|account, state| {
                    if state.details.is_expired(current_utime) {
                        tracing::warn!(
                            configuration = %DisplayAddr(account),
                            current_utime,
                            "removing TON->SOL event configuration",
                        );
                        total_removed += 1;
                        false
                    } else {
                        true
                    }
                });
                bridge
                    .total_active_ton_sol_event_configurations
                    .fetch_sub(total_removed, Ordering::Release);

                // Remove SOL->TON expired configurations
                let mut total_removed = 0;
                state.sol_ton_event_configurations.retain(|account, state| {
                    if state.details.is_expired(current_utime as u64) {
                        tracing::warn!(
                            configuration = %DisplayAddr(account),
                            current_utime,
                            "removing SOL->TON event configuration",
                        );
                        total_removed += 1;
                        false
                    } else {
                        true
                    }
                });
                bridge
                    .total_active_sol_ton_event_configurations
                    .fetch_sub(total_removed, Ordering::Release);
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
                tracing::error!("failed to {name}: {e:?}");
            }
        });
    }
}

pub struct BridgeMetrics {
    pub pending_eth_ton_event_count: usize,
    pub pending_ton_eth_event_count: usize,
    pub pending_sol_ton_event_count: usize,
    pub pending_ton_sol_event_count: usize,
    pub total_active_eth_ton_event_configurations: usize,
    pub total_active_ton_eth_event_configurations: usize,
    pub total_active_sol_ton_event_configurations: usize,
    pub total_active_ton_sol_event_configurations: usize,
}

struct EventsState<T> {
    pending: FxDashMap<UInt256, PendingEventState<T>>,
    count: AtomicUsize,
    events_tx: AccountEventsTx<(T, EventStatus)>,
}

impl<T> EventsState<T>
where
    T: EventExt,
{
    fn new(events_tx: AccountEventsTx<(T, EventStatus)>) -> Arc<Self> {
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
    observer: Arc<AccountObserver<(T, EventStatus)>>,
}

#[async_trait::async_trait]
trait EventExt {
    const REQUIRE_ALL_SIGNATURES: bool;

    async fn update_event(bridge: Arc<Bridge>, account: UInt256) -> Result<()>;
}

#[async_trait::async_trait]
impl EventExt for EthTonEvent {
    const REQUIRE_ALL_SIGNATURES: bool = false;

    async fn update_event(bridge: Arc<Bridge>, account: UInt256) -> Result<()> {
        bridge.update_eth_ton_event(account).await
    }
}

#[async_trait::async_trait]
impl EventExt for TonEthEvent {
    const REQUIRE_ALL_SIGNATURES: bool = true;

    async fn update_event(bridge: Arc<Bridge>, account: UInt256) -> Result<()> {
        bridge.update_ton_eth_event(account).await
    }
}

#[async_trait::async_trait]
impl EventExt for SolTonEvent {
    const REQUIRE_ALL_SIGNATURES: bool = false;

    async fn update_event(bridge: Arc<Bridge>, account: UInt256) -> Result<()> {
        bridge.update_sol_ton_event(account).await
    }
}

#[async_trait::async_trait]
impl EventExt for TonSolEvent {
    const REQUIRE_ALL_SIGNATURES: bool = true;

    async fn update_event(bridge: Arc<Bridge>, account: UInt256) -> Result<()> {
        bridge.update_ton_sol_event(account).await
    }
}

/// Semi-persistent bridge contracts collection
#[derive(Default)]
struct BridgeState {
    connectors: ConnectorsMap,
    eth_ton_event_configurations: EthTonEventConfigurationsMap,
    ton_eth_event_configurations: TonEthEventConfigurationsMap,
    sol_ton_event_configurations: SolTonEventConfigurationsMap,
    ton_sol_event_configurations: TonSolEventConfigurationsMap,

    /// Unique event contracts code hashes.
    ///
    /// NOTE: only built on startup and then updated on each new configuration.
    /// Elements are not removed because it is not needed (the situation when one
    /// contract code will be used for ETH and TON simultaneously)
    event_code_hashes: EventCodeHashesMap,
}

impl BridgeState {
    fn has_expired_ton_eth_event_configurations(&self, current_timestamp: u32) -> bool {
        self.ton_eth_event_configurations
            .iter()
            .any(|(_, state)| state.details.is_expired(current_timestamp))
    }

    fn has_expired_ton_sol_event_configurations(&self, current_timestamp: u32) -> bool {
        self.ton_sol_event_configurations
            .iter()
            .any(|(_, state)| state.details.is_expired(current_timestamp))
    }

    fn has_expired_sol_ton_event_configurations(&self, current_timestamp: u32) -> bool {
        self.sol_ton_event_configurations
            .iter()
            .any(|(_, state)| state.details.is_expired(current_timestamp as u64))
    }

    fn unique_eth_ton_event_configurations(&self) -> FxHashSet<UInt256> {
        self.eth_ton_event_configurations.keys().copied().collect()
    }

    fn unique_ton_eth_event_configurations(&self) -> FxHashSet<UInt256> {
        self.ton_eth_event_configurations.keys().copied().collect()
    }

    fn unique_sol_ton_event_configurations(&self) -> FxHashSet<UInt256> {
        self.sol_ton_event_configurations.keys().copied().collect()
    }

    fn unique_ton_sol_event_configurations(&self) -> FxHashSet<UInt256> {
        self.ton_sol_event_configurations.keys().copied().collect()
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

impl EventBaseContract<'_> {
    /// Determine event action
    fn process(&self, public_key: &UInt256, require_all_signatures: bool) -> Result<EventAction> {
        const SUPPORTED_API_VERSION: u32 = 2;

        Ok(match self.status()? {
            // If it is still initializing - postpone processing until relay keys are received
            EventStatus::Initializing => EventAction::Nop,
            // The main status in which we can vote
            EventStatus::Pending
                if self.get_voters(EventVote::Empty)?.contains(public_key)
                    && self.get_api_version().unwrap_or_default() == SUPPORTED_API_VERSION =>
            {
                EventAction::Vote
            }
            // Special case for TON->ETH event which must collect as much signatures as possible
            EventStatus::Confirmed
                if require_all_signatures
                    && self.0.account.storage.balance.grams.as_u128() >= MIN_EVENT_BALANCE
                    && self.get_voters(EventVote::Empty)?.contains(public_key)
                    && self.get_api_version().unwrap_or_default() == SUPPORTED_API_VERSION =>
            {
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

/// ETH->TON event configuration data
#[derive(Clone)]
struct EthTonEventConfigurationState {
    /// Configuration details
    details: EthTonEventConfigurationDetails,
    /// Mapping context
    mapping_context: EthToTonMappingContext,
    /// Parsed and mapped event ABI
    event_abi: Arc<EthEventAbi>,

    /// Observer must live as long as configuration lives
    _observer: Arc<AccountObserver<EthTonEventConfigurationEvent>>,
}

/// TON->ETH event configuration data
#[derive(Clone)]
struct TonEthEventConfigurationState {
    /// Configuration details
    details: TonEthEventConfigurationDetails,
    /// Context
    #[cfg(feature = "ton")]
    context: eth_ton_abi_converter::TonToEthContext,
    /// Parsed `eventData` ABI
    event_abi: Vec<ton_abi::Param>,

    /// Observer must live as long as configuration lives
    _observer: Arc<AccountObserver<TonEthEventConfigurationEvent>>,
}

impl TonEthEventConfigurationDetails {
    fn is_expired(&self, current_timestamp: u32) -> bool {
        (1..current_timestamp).contains(&self.network_configuration.end_timestamp)
    }
}

/// ETH->TON event configuration data
#[derive(Clone)]
struct SolTonEventConfigurationState {
    /// Configuration details
    details: SolTonEventConfigurationDetails,
    /// Parsed and mapped event ABI
    event_abi: Vec<ton_abi::Param>,

    /// Observer must live as long as configuration lives
    _observer: Arc<AccountObserver<SolTonEventConfigurationEvent>>,
}

impl SolTonEventConfigurationDetails {
    fn is_expired(&self, current_timestamp: u64) -> bool {
        (1..current_timestamp).contains(&self.network_configuration.end_timestamp)
    }
}

/// TON->SOL event configuration data
#[derive(Clone)]
struct TonSolEventConfigurationState {
    /// Configuration details
    details: TonSolEventConfigurationDetails,
    /// Parsed `eventData` ABI
    event_abi: Vec<ton_abi::Param>,

    /// Observer must live as long as configuration lives
    _observer: Arc<AccountObserver<TonSolEventConfigurationEvent>>,
}

impl TonSolEventConfigurationDetails {
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
                        tracing::error!(
                            tx = ctx.transaction_hash.to_hex_string(),
                            "failed to parse bridge event: {e:?}"
                        );
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
    fn find_new_event_contract_addresses(&self) -> Vec<UInt256> {
        let event = base_event_configuration_contract::events::new_event_contract();

        let mut result: Vec<UInt256> = Vec::new();
        self.iterate_events(|id, body| {
            if id == event.id {
                match event.decode_input(body).and_then(|tokens| {
                    tokens
                        .unpack_first::<ton_block::MsgAddressInt>()
                        .map_err(anyhow::Error::from)
                }) {
                    Ok(parsed) => result.push(only_account_hash(parsed)),
                    Err(e) => {
                        tracing::error!(
                            tx = self.transaction_hash.to_hex_string(),
                            "failed to parse NewEventContract event: {e:?}",
                        );
                    }
                }
            }
        });

        result
    }
}

#[derive(Debug, Clone)]
enum TonEthEventConfigurationEvent {
    EventDeployed { address: UInt256 },
    SetEndTimestamp { end_timestamp: u32 },
}

impl ReadFromTransaction for TonEthEventConfigurationEvent {
    fn read_from_transaction(ctx: &TxContext<'_>) -> Option<Self> {
        let in_msg_body = ctx.in_msg_internal()?.body()?;

        let set_end_timestamp = ton_eth_event_configuration_contract::set_end_timestamp();

        match read_function_id(&in_msg_body).ok()? {
            id if id == set_end_timestamp.input_id => {
                let end_timestamp = set_end_timestamp
                    .decode_input(in_msg_body, true)
                    .and_then(|tokens| tokens.unpack_first().map_err(anyhow::Error::from))
                    .ok()?;

                Some(Self::SetEndTimestamp { end_timestamp })
            }
            _ => {
                let events = ctx.find_new_event_contract_addresses();
                Some(Self::EventDeployed {
                    address: events.into_iter().next()?,
                })
            }
        }
    }
}

#[derive(Debug, Clone)]
enum EthTonEventConfigurationEvent {
    EventsDeployed { events: Vec<UInt256> },
    SetEndBlockNumber { end_block_number: u32 },
}

impl ReadFromTransaction for EthTonEventConfigurationEvent {
    fn read_from_transaction(ctx: &TxContext<'_>) -> Option<Self> {
        let in_msg_body = ctx.in_msg_internal()?.body()?;

        let set_end_block_number = eth_ton_event_configuration_contract::set_end_block_number();

        match read_function_id(&in_msg_body).ok()? {
            id if id == set_end_block_number.input_id => {
                let end_block_number = set_end_block_number
                    .decode_input(in_msg_body, true)
                    .and_then(|tokens| tokens.unpack_first().map_err(anyhow::Error::from))
                    .ok()?;

                Some(Self::SetEndBlockNumber { end_block_number })
            }
            _ => {
                let events = ctx.find_new_event_contract_addresses();
                if events.is_empty() {
                    return None;
                }
                Some(Self::EventsDeployed { events })
            }
        }
    }
}

#[derive(Debug, Clone)]
enum TonSolEventConfigurationEvent {
    EventDeployed { address: UInt256 },
    SetEndTimestamp { end_timestamp: u32 },
}

impl ReadFromTransaction for TonSolEventConfigurationEvent {
    fn read_from_transaction(ctx: &TxContext<'_>) -> Option<Self> {
        let in_msg_body = ctx.in_msg_internal()?.body()?;

        let set_end_timestamp = ton_sol_event_configuration_contract::set_end_timestamp();

        match nekoton_abi::read_function_id(&in_msg_body).ok()? {
            id if id == set_end_timestamp.input_id => {
                let end_timestamp = set_end_timestamp
                    .decode_input(in_msg_body, true)
                    .and_then(|tokens| tokens.unpack_first().map_err(anyhow::Error::from))
                    .ok()?;

                Some(Self::SetEndTimestamp { end_timestamp })
            }
            _ => {
                let events = ctx.find_new_event_contract_addresses();
                Some(Self::EventDeployed {
                    address: events.into_iter().next()?,
                })
            }
        }
    }
}

#[derive(Debug, Clone)]
enum SolTonEventConfigurationEvent {
    EventsDeployed { events: Vec<UInt256> },
    SetEndTimestamp { end_timestamp: u64 },
}

impl ReadFromTransaction for SolTonEventConfigurationEvent {
    fn read_from_transaction(ctx: &TxContext<'_>) -> Option<Self> {
        let in_msg_body = ctx.in_msg_internal()?.body()?;

        let set_end_timestamp = sol_ton_event_configuration_contract::set_end_timestamp();

        match nekoton_abi::read_function_id(&in_msg_body).ok()? {
            id if id == set_end_timestamp.input_id => {
                let end_timestamp = set_end_timestamp
                    .decode_input(in_msg_body, true)
                    .and_then(|tokens| tokens.unpack_first().map_err(anyhow::Error::from))
                    .ok()?;

                Some(Self::SetEndTimestamp { end_timestamp })
            }
            _ => {
                let events = ctx.find_new_event_contract_addresses();
                if events.is_empty() {
                    return None;
                }
                Some(Self::EventsDeployed { events })
            }
        }
    }
}

impl ReadFromTransaction for EventStatus {
    fn read_from_transaction(ctx: &TxContext<'_>) -> Option<Self> {
        let state = ctx.get_account_state().ok()?;
        EventBaseContract(&state).status().ok()
    }
}

#[derive(Debug, Clone)]
enum EthTonEvent {
    ReceiveRoundRelays { keys: Vec<UInt256> },
    Confirm { public_key: UInt256 },
    Reject { public_key: UInt256 },
    Rejected,
}

impl ReadFromTransaction for EthTonEvent {
    fn read_from_transaction(ctx: &TxContext<'_>) -> Option<Self> {
        if has_rejected_event(ctx) {
            return Some(Self::Rejected);
        }

        let in_msg = ctx.in_msg;
        match in_msg.header() {
            ton_block::CommonMsgInfo::ExtInMsgInfo(_) => {
                let (public_key, body) = read_external_in_msg(&in_msg.body()?)?;

                match read_function_id(&body) {
                    Ok(id) if id == eth_ton_event_contract::confirm().input_id => {
                        Some(EthTonEvent::Confirm { public_key })
                    }
                    Ok(id) if id == eth_ton_event_contract::reject().input_id => {
                        Some(EthTonEvent::Reject { public_key })
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

                        Some(EthTonEvent::ReceiveRoundRelays { keys: items })
                    }
                    _ => None,
                }
            }
            ton_block::CommonMsgInfo::ExtOutMsgInfo(_) => None,
        }
    }
}

#[derive(Debug, Clone)]
enum TonEthEvent {
    ReceiveRoundRelays { keys: Vec<UInt256> },
    Confirm { public_key: UInt256 },
    Reject { public_key: UInt256 },
    Rejected,
    Closed,
}

impl ReadFromTransaction for TonEthEvent {
    fn read_from_transaction(ctx: &TxContext<'_>) -> Option<Self> {
        if has_rejected_event(ctx) {
            return Some(Self::Rejected);
        }

        let in_msg = ctx.in_msg;
        let event = match in_msg.header() {
            ton_block::CommonMsgInfo::ExtInMsgInfo(_) => {
                let (public_key, body) = read_external_in_msg(&in_msg.body()?)?;

                match read_function_id(&body) {
                    Ok(id) if id == ton_eth_event_contract::confirm().input_id => {
                        Some(TonEthEvent::Confirm { public_key })
                    }
                    Ok(id) if id == ton_eth_event_contract::reject().input_id => {
                        Some(TonEthEvent::Reject { public_key })
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

                        Some(TonEthEvent::ReceiveRoundRelays { keys: items })
                    }
                    _ => None,
                }
            }
            ton_block::CommonMsgInfo::ExtOutMsgInfo(_) => None,
        };

        if event.is_none() {
            let balance = ctx.get_account_state().ok()?.account.storage.balance.grams;
            if balance.as_u128() < MIN_EVENT_BALANCE {
                return Some(Self::Closed);
            }
        }

        event
    }
}

#[derive(Debug, Clone)]
enum SolTonEvent {
    ReceiveRoundRelays { keys: Vec<UInt256> },
    Confirm { public_key: UInt256 },
    Reject { public_key: UInt256 },
    Rejected,
}

impl ReadFromTransaction for SolTonEvent {
    fn read_from_transaction(ctx: &TxContext<'_>) -> Option<Self> {
        if has_rejected_event(ctx) {
            return Some(Self::Rejected);
        }

        let in_msg = ctx.in_msg;
        match in_msg.header() {
            ton_block::CommonMsgInfo::ExtInMsgInfo(_) => {
                let (public_key, body) = read_external_in_msg(&in_msg.body()?)?;

                match read_function_id(&body) {
                    Ok(id) if id == sol_ton_event_contract::confirm().input_id => {
                        Some(SolTonEvent::Confirm { public_key })
                    }
                    Ok(id) if id == sol_ton_event_contract::reject().input_id => {
                        Some(SolTonEvent::Reject { public_key })
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

                        Some(SolTonEvent::ReceiveRoundRelays { keys: items })
                    }
                    _ => None,
                }
            }
            ton_block::CommonMsgInfo::ExtOutMsgInfo(_) => None,
        }
    }
}

#[derive(Debug, Clone)]
enum TonSolEvent {
    ReceiveRoundRelays { keys: Vec<UInt256> },
    Confirm { public_key: UInt256 },
    Reject { public_key: UInt256 },
    Rejected,
    Closed,
}

impl ReadFromTransaction for TonSolEvent {
    fn read_from_transaction(ctx: &TxContext<'_>) -> Option<Self> {
        if has_rejected_event(ctx) {
            return Some(Self::Rejected);
        }

        let in_msg = ctx.in_msg;
        let event = match in_msg.header() {
            ton_block::CommonMsgInfo::ExtInMsgInfo(_) => {
                let (public_key, body) = read_external_in_msg(&in_msg.body()?)?;

                match read_function_id(&body) {
                    Ok(id) if id == ton_sol_event_contract::confirm().input_id => {
                        Some(TonSolEvent::Confirm { public_key })
                    }
                    Ok(id) if id == ton_sol_event_contract::reject().input_id => {
                        Some(TonSolEvent::Reject { public_key })
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

                        Some(TonSolEvent::ReceiveRoundRelays { keys: items })
                    }
                    _ => None,
                }
            }
            ton_block::CommonMsgInfo::ExtOutMsgInfo(_) => None,
        };

        if event.is_none() {
            let balance = ctx.get_account_state().ok()?.account.storage.balance.grams;
            if balance.as_u128() < MIN_EVENT_BALANCE {
                return Some(Self::Closed);
            }
        }

        event
    }
}

fn read_external_in_msg(body: &ton_types::SliceData) -> Option<(UInt256, ton_types::SliceData)> {
    match unpack_headers::<DefaultHeaders>(body) {
        Ok(((Some(public_key), _, _), body)) => Some((public_key, body)),
        _ => None,
    }
}

fn has_rejected_event(ctx: &TxContext<'_>) -> bool {
    let mut result = false;
    ctx.iterate_events(|event_id, _| {
        if event_id == base_event_contract::events::rejected().id {
            result = true;
        }
    });
    result
}

fn parse_client_error(err: ClientError) -> anyhow::Error {
    match &err.kind {
        ClientErrorKind::RpcError(RpcError::RpcResponseError {
            data:
                RpcResponseErrorData::SendTransactionPreflightFailure(RpcSimulateTransactionResult {
                    err: Some(TransactionError::InstructionError(_, InstructionError::Custom(code))),
                    ..
                }),
            ..
        }) => {
            let error = SolanaBridgeError::try_from(*code).trust_me();
            match error {
                SolanaBridgeError::EmergencyEnabled => {
                    anyhow::Error::msg(SolanaBridgeError::EmergencyEnabled.to_string())
                }
                SolanaBridgeError::VotesOverflow => {
                    anyhow::Error::msg(SolanaBridgeError::VotesOverflow.to_string())
                }
                SolanaBridgeError::InvalidVote => {
                    anyhow::Error::msg(SolanaBridgeError::InvalidVote.to_string())
                }
                SolanaBridgeError::InvalidRelay => {
                    anyhow::Error::msg(SolanaBridgeError::InvalidRelay.to_string())
                }
                _ => anyhow::Error::msg(format!("Solana RPC error: {err}")),
            }
        }
        ClientErrorKind::TransactionError(TransactionError::InstructionError(
            _,
            InstructionError::Custom(code),
        )) => {
            let error = SolanaBridgeError::try_from(*code).trust_me();
            match error {
                SolanaBridgeError::EmergencyEnabled => {
                    anyhow::Error::msg(SolanaBridgeError::EmergencyEnabled.to_string())
                }
                SolanaBridgeError::VotesOverflow => {
                    anyhow::Error::msg(SolanaBridgeError::VotesOverflow.to_string())
                }
                SolanaBridgeError::InvalidVote => {
                    anyhow::Error::msg(SolanaBridgeError::InvalidVote.to_string())
                }
                SolanaBridgeError::InvalidRelay => {
                    anyhow::Error::msg(SolanaBridgeError::InvalidRelay.to_string())
                }
                _ => anyhow::Error::msg(format!("Solana Transaction error: {err}")),
            }
        }
        _ => anyhow::Error::msg(format!("Solana Client error: {err}")),
    }
}

const MIN_EVENT_BALANCE: u128 = 100_000_000; // 0.1 TON

type ConnectorState = Arc<AccountObserver<ConnectorEvent>>;

type DefaultHeaders = (PubkeyHeader, TimeHeader, ExpireHeader);

type ConnectorsMap = FxHashMap<UInt256, ConnectorState>;
type EthTonEventConfigurationsMap = FxHashMap<UInt256, EthTonEventConfigurationState>;
type TonEthEventConfigurationsMap = FxHashMap<UInt256, TonEthEventConfigurationState>;
type SolTonEventConfigurationsMap = FxHashMap<UInt256, SolTonEventConfigurationState>;
type TonSolEventConfigurationsMap = FxHashMap<UInt256, TonSolEventConfigurationState>;
type EventCodeHashesMap = FxHashMap<UInt256, EventType>;

#[derive(Debug, Clone, Hash)]
pub enum VerificationStatus {
    Exists,
    NotExists { reason: String },
}

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
    #[error("Storage not ready")]
    StorageNotReady,
    #[error("Account `{0}` not found")]
    AccountNotFound(String),
    #[cfg(feature = "ton")]
    #[error("Token metadata mismatch")]
    TokenMetadataMismatch,
}
