use std::collections::hash_map;
use std::sync::Arc;

use anyhow::{Context, Result};
use chrono::Utc;
use nekoton_abi::*;
use parking_lot::RwLock;
use tiny_adnl::utils::*;
use tokio::sync::{mpsc, watch};
use ton_block::{HashmapAugType, Serializable};
use ton_types::{SliceData, UInt256};

use crate::engine::ton_contracts::*;
use crate::engine::ton_subscriber::*;
use crate::engine::EngineContext;
use crate::utils::*;

pub struct Bridge {
    context: Arc<EngineContext>,

    bridge_account: UInt256,

    bridge_observer: Arc<BridgeObserver>,

    connectors_tx: AccountEventsTx<ConnectorEvent>,
    connectors: ConnectorsMap,

    eth_event_configurations_tx: AccountEventsTx<EthEventConfigurationEvent>,
    eth_event_configurations: EthEventConfigurationsMap,

    ton_event_configurations_tx: AccountEventsTx<TonEventConfigurationEvent>,
    ton_event_configurations: TonEventConfigurationsMap,

    event_code_hashes: Arc<RwLock<EventCodeHashesMap>>,
}

impl Bridge {
    pub async fn new(
        context: Arc<EngineContext>,
        bridge_account: UInt256,
        bridge_contract: ExistingContract,
    ) -> Result<Arc<Self>> {
        let (bridge_events_tx, bridge_events_rx) = mpsc::unbounded_channel();
        let (connector_events_tx, connector_events_rx) = mpsc::unbounded_channel();
        let (eth_event_configurations_tx, eth_event_configurations_rx) = mpsc::unbounded_channel();
        let (ton_event_configurations_tx, ton_event_configurations_rx) = mpsc::unbounded_channel();

        let bridge = Arc::new(Bridge {
            context: context.clone(),
            bridge_account,
            bridge_observer: Arc::new(BridgeObserver {
                events_tx: bridge_events_tx,
            }),
            connectors_tx: connector_events_tx,
            connectors: Default::default(),
            eth_event_configurations_tx,
            eth_event_configurations: Default::default(),
            ton_event_configurations_tx,
            ton_event_configurations: Default::default(),
            event_code_hashes: Arc::new(Default::default()),
        });

        bridge.start_listening_bridge_events(bridge_events_rx);
        bridge.start_listening_connector_events(connector_events_rx);

        let shard_accounts = context.get_all_shard_accounts().await?;

        bridge.get_all_configurations(&shard_accounts);

        todo!()
    }

    pub fn account(&self) -> &UInt256 {
        &self.bridge_account
    }

    fn start_listening_bridge_events(self: &Arc<Self>, mut bridge_events_rx: BridgeEventsRx) {
        use dashmap::mapref::entry::Entry;

        let engine = Arc::downgrade(self);

        tokio::spawn(async move {
            while let Some(event) = bridge_events_rx.recv().await {
                let bridge = match engine.upgrade() {
                    Some(engine) => engine,
                    None => break,
                };

                match bridge.connectors.entry(event.connector) {
                    Entry::Vacant(entry) => {
                        let observer = Arc::new(ConnectorObserver(bridge.connectors_tx.clone()));

                        let entry = entry.insert(ConnectorState {
                            details: ConnectorDetails {
                                id: event.id,
                                event_configuration: event.event_configuration,
                                enabled: false,
                            },
                            event_type: ConnectorConfigurationType::Unknown,
                            observer,
                        });

                        bridge
                            .context
                            .ton_subscriber
                            .add_transactions_subscription([event.connector], &entry.observer);
                    }
                    Entry::Occupied(_) => {
                        log::error!(
                            "Got connector deployment event but it already exists: {}",
                            event.connector.to_hex_string()
                        );
                        return;
                    }
                };

                tokio::spawn(async move {
                    if let Err(e) = bridge.process_bridge_event(event).await {
                        log::error!("Failed to process bridge event: {:?}", e);
                    }
                });
            }
        });
    }

    async fn process_bridge_event(&self, event: ConnectorDeployedEvent) -> Result<()> {
        let ton_subscriber = &self.context.ton_subscriber;

        // Process event configuration
        let event_type = {
            // Wait until event configuration state is found
            let contract = loop {
                // TODO: add finite retires for getting configuration info

                match ton_subscriber
                    .wait_contract_state(event.event_configuration)
                    .await
                {
                    Ok(contract) => break contract,
                    Err(e) => {
                        log::error!("Failed to get event configuration contract: {:?}", e);
                        continue;
                    }
                }
            };

            // Extract info from contract
            let mut event_code_hashes = self.event_code_hashes.write();
            let mut ctx = EventConfigurationsProcessingContext {
                event_code_hashes: &mut event_code_hashes,
                eth_event_configurations: &self.eth_event_configurations,
                eth_event_configurations_tx: &self.eth_event_configurations_tx,
                ton_event_configurations: &self.ton_event_configurations,
                ton_event_configurations_tx: &self.ton_event_configurations_tx,
            };
            match process_event_configuration(&event.event_configuration, &contract, &mut ctx)? {
                Some(event_type) => event_type,
                None => {
                    // It is a strange situation when connector contains an address of the contract
                    // which doesn't exist, so log it here to investigate it later
                    log::warn!(
                        "Connected configuration was not found: {}",
                        event.event_configuration.to_hex_string()
                    );
                    return Ok(());
                }
            }
        };

        // self.ton_subscriber
        //     .add_transactions_subscription([event.connector], &self.connectors_observer);

        // TODO: check connector state and remove configuration subscription if it was disabled

        Ok(())
    }

    fn start_listening_connector_events(
        self: &Arc<Self>,
        mut connectors_rx: AccountEventsRx<ConnectorEvent>,
    ) {
        let engine = Arc::downgrade(self);

        tokio::spawn(async move {
            while let Some(event) = connectors_rx.recv().await {
                let engine = match engine.upgrade() {
                    Some(engine) => engine,
                    None => break,
                };

                // TODO
            }
        });
    }

    async fn get_all_configurations(&self, shard_accounts: &ShardAccountsMap) -> Result<()> {
        let ton_subscriber = &self.context.ton_subscriber;

        let contract = shard_accounts
            .find_account(&self.bridge_account)?
            .ok_or(EngineError::BridgeAccountNotFound)?;
        let bridge = BridgeContract(&contract);

        let mut connectors = Vec::new();
        let mut active_configuration_accounts = Vec::new();
        let mut event_code_hashes = FxHashMap::with_capacity_and_hasher(2, Default::default());

        for id in 0.. {
            let connector_address = bridge.derive_connector_address(id)?;

            let contract = match shard_accounts.find_account(&connector_address)? {
                Some(contract) => contract,
                None => {
                    log::info!(
                        "Last connector not found: {}",
                        connector_address.to_hex_string()
                    );
                    break;
                }
            };

            let details = ConnectorContract(&contract).get_details()?;
            log::info!("Found configuration connector {}: {:?}", id, details);

            if details.enabled {
                active_configuration_accounts.push((id, details.event_configuration));
            }
            connectors.push((
                connector_address,
                ConnectorState {
                    details,
                    event_type: ConnectorConfigurationType::Unknown,
                    observer: Arc::new(ConnectorObserver(self.connectors_tx.clone())),
                },
            ));
        }

        let mut ctx = EventConfigurationsProcessingContext {
            event_code_hashes: &mut event_code_hashes,
            eth_event_configurations: &self.eth_event_configurations,
            eth_event_configurations_tx: &self.eth_event_configurations_tx,
            ton_event_configurations: &self.ton_event_configurations,
            ton_event_configurations_tx: &self.ton_event_configurations_tx,
        };

        for (connector_id, account) in active_configuration_accounts {
            // Find configuration contact
            let contract = match shard_accounts.find_account(&account)? {
                Some(contract) => contract,
                None => {
                    // It is a strange situation when connector contains an address of the contract
                    // which doesn't exist, so log it here to investigate it later
                    log::warn!(
                        "Connected configuration was not found: {}",
                        account.to_hex_string()
                    );
                    continue;
                }
            };

            match process_event_configuration(&account, &contract, &mut ctx) {
                Ok(Some(event_type)) => {
                    if let Some((_, state)) = connectors.get_mut(connector_id as usize) {
                        state.event_type = ConnectorConfigurationType::Known(event_type);
                    }
                }
                Ok(None) => { /* do nothing now, TODO: watch unresolved configurations */ }
                Err(e) => {
                    log::error!("Failed to process event configuration: {:?}", e);
                }
            };
        }

        log::info!("Found unique event code hashes: {:?}", event_code_hashes);

        *self.event_code_hashes.write() = event_code_hashes;

        for (account, state) in connectors {
            self.connectors.insert(account, state);
        }

        // Register
        self.connectors.iter().for_each(|item| {
            ton_subscriber
                .add_transactions_subscription(std::iter::once(*item.key()), &item.observer);
        });
        self.eth_event_configurations.iter().for_each(|item| {
            ton_subscriber
                .add_transactions_subscription(std::iter::once(*item.key()), &item.observer);
        });
        self.ton_event_configurations.iter().for_each(|item| {
            ton_subscriber
                .add_transactions_subscription(std::iter::once(*item.key()), &item.observer)
        });

        // TODO

        Ok(())
    }

    async fn get_all_events(&self, shard_accounts: &ShardAccountsMap) -> Result<()> {
        let shard_accounts = self.context.get_all_shard_accounts().await?;

        let event_code_hashes = self.event_code_hashes.read();

        for (_, accounts) in shard_accounts {
            accounts.iterate_with_keys(|hash, shard_account| {
                let account = match shard_account.read_account()? {
                    ton_block::Account::Account(account) => account,
                    ton_block::Account::AccountNone => return Ok(true),
                };

                let code_hash = match account.storage.state() {
                    ton_block::AccountState::AccountActive(ton_block::StateInit {
                        code: Some(code),
                        ..
                    }) => code.repr_hash(),
                    _ => return Ok(true),
                };

                let event_type = match event_code_hashes.get(&code_hash) {
                    Some(event_type) => event_type,
                    None => return Ok(true),
                };

                log::info!("FOUND EVENT {:?}: {}", event_type, hash.to_hex_string());

                // TODO: get details and filter current

                Ok(true)
            })?;
        }

        Ok(())
    }
}

/// Startup configurations processing context
struct EventConfigurationsProcessingContext<'a> {
    /// Unique event code hashes with event types. Used to simplify events search
    event_code_hashes: &'a mut EventCodeHashesMap,
    /// Details of parsed ETH event configurations
    eth_event_configurations: &'a EthEventConfigurationsMap,
    eth_event_configurations_tx: &'a AccountEventsTx<EthEventConfigurationEvent>,
    /// Details of parsed TON event configurations
    ton_event_configurations: &'a TonEventConfigurationsMap,
    ton_event_configurations_tx: &'a AccountEventsTx<TonEventConfigurationEvent>,
}

/// Searches for the account contract and extracts the configuration information into context
fn process_event_configuration(
    account: &UInt256,
    contract: &ExistingContract,
    ctx: &mut EventConfigurationsProcessingContext<'_>,
) -> Result<Option<EventType>> {
    // Get event type using base contract abi
    let event_type = EventConfigurationBaseContract(contract).get_type()?;

    let event_code_hashes = &mut ctx.event_code_hashes;

    // Small helper to populate unique event contract code hashes
    let mut fill_event_code_hash = |code: &ton_types::Cell| {
        match event_code_hashes.entry(code.repr_hash()) {
            // Just insert if it was not in the map
            hash_map::Entry::Vacant(entry) => {
                entry.insert(event_type);
            }
            // Do nothing if it was there with the same event type, otherwise return an error
            hash_map::Entry::Occupied(entry) => {
                if entry.get() != &event_type {
                    return Err(EngineError::InvalidEventConfiguration)
                        .with_context(|| format!(
                            "Found same code for different event configuration types: {} specified as {:?}",
                            account.to_hex_string(),
                            event_type
                        ));
                }
            }
        }
        Ok(())
    };

    match event_type {
        // Extract and populate ETH event configuration details
        EventType::Eth => {
            let details = EthEventConfigurationContract(contract).get_details()?;
            log::info!("Ethereum event configuration details: {:?}", details);

            let observer = Arc::new(EventConfigurationsObserver(
                ctx.eth_event_configurations_tx.clone(),
            ));

            fill_event_code_hash(&details.basic_configuration.event_code)?;
            ctx.eth_event_configurations
                .insert(*account, EventConfigurationState { details, observer });
        }
        // Extract and populate TON event configuration details
        EventType::Ton => {
            let details = TonEventConfigurationContract(contract).get_details()?;
            log::info!("TON event configuration details: {:?}", details);

            let observer = Arc::new(EventConfigurationsObserver(
                ctx.ton_event_configurations_tx.clone(),
            ));

            fill_event_code_hash(&details.basic_configuration.event_code)?;
            ctx.ton_event_configurations
                .insert(*account, EventConfigurationState { details, observer });
        }
    };

    // Done
    Ok(Some(event_type))
}

#[derive(Clone)]
struct ConnectorState {
    details: ConnectorDetails,
    event_type: ConnectorConfigurationType,
    observer: Arc<ConnectorObserver>,
}

/// Linked configuration event type hint
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
enum ConnectorConfigurationType {
    Unknown,
    Known(EventType),
}

/// Parsed connector event
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
enum ConnectorEvent {
    Enable,
}

#[derive(Clone)]
struct EventConfigurationState<T, E> {
    details: T,
    observer: Arc<EventConfigurationsObserver<E>>,
}

/// Listener for bridge transactions
///
/// **Registered only for bridge account**
struct BridgeObserver {
    events_tx: BridgeEventsTx,
}

impl TransactionsSubscription for BridgeObserver {
    fn handle_transaction(&self, ctx: TxContext<'_>) -> Result<()> {
        // Parse all outgoing messages and search proper event
        let mut event: Option<ConnectorDeployedEvent> = None;
        ctx.iterate_events(|id, body| {
            let connector_deployed = bridge_contract::events::connector_deployed();

            if id == connector_deployed.id {
                match connector_deployed
                    .decode_input(body)
                    .and_then(|tokens| tokens.unpack().map_err(anyhow::Error::from))
                {
                    Ok(parsed) => event = Some(parsed),
                    Err(e) => {
                        log::error!("Failed to parse bridge event: {:?}", e);
                    }
                }
            }
        });

        log::info!("Got transaction on bridge: {:?}", event);

        // Send event to event manager if it exist
        if let Some(event) = event {
            self.events_tx.send(event).ok();
        }

        // Done
        Ok(())
    }
}

/// Listener for connector transactions
///
/// **Registered for each connector account**
struct ConnectorObserver(AccountEventsTx<ConnectorEvent>);

impl TransactionsSubscription for ConnectorObserver {
    fn handle_transaction(&self, ctx: TxContext<'_>) -> Result<()> {
        // Parse all outgoing messages and search proper event
        let mut event = None;
        ctx.iterate_events(|id, _| {
            if id == connector_contract::events::enabled().id {
                event = Some(ConnectorEvent::Enable);
            }
        });

        log::info!(
            "Got transaction on connector {}: {:?}",
            ctx.account.to_hex_string(),
            event
        );

        // Send event to event manager if it exist
        if let Some(event) = event {
            self.0.send((*ctx.account, event)).ok();
        }

        // Done
        Ok(())
    }
}

/// Generic listener for event configuration transactions
///
/// **Registered for each active TON event configuration account**
struct EventConfigurationsObserver<T>(AccountEventsTx<T>);

impl<T> TransactionsSubscription for EventConfigurationsObserver<T>
where
    T: ReadFromTransaction + std::fmt::Debug + Send + Sync,
{
    fn handle_transaction(&self, ctx: TxContext<'_>) -> Result<()> {
        let mut event = T::read_from_transaction(&ctx);

        log::info!(
            "Got transaction on event configuration {}: {:?}",
            ctx.account.to_hex_string(),
            event
        );

        // Send event to event manager if it exist
        if let Some(event) = event {
            self.0.send((*ctx.account, event)).ok();
        }

        // Done
        Ok(())
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

type AccountEventsRx<T> = mpsc::UnboundedReceiver<(UInt256, T)>;
type AccountEventsTx<T> = mpsc::UnboundedSender<(UInt256, T)>;

type BridgeEventsTx = mpsc::UnboundedSender<ConnectorDeployedEvent>;
type BridgeEventsRx = mpsc::UnboundedReceiver<ConnectorDeployedEvent>;

type EventCodeHashesMap = FxHashMap<UInt256, EventType>;

type ConnectorsMap = FxDashMap<UInt256, ConnectorState>;
type EthEventConfigurationsMap = FxDashMap<
    UInt256,
    EventConfigurationState<EthEventConfigurationDetails, EthEventConfigurationEvent>,
>;
type TonEventConfigurationsMap = FxDashMap<
    UInt256,
    EventConfigurationState<TonEventConfigurationDetails, TonEventConfigurationEvent>,
>;

#[derive(thiserror::Error, Debug)]
enum EngineError {
    #[error("External ton message expected")]
    ExternalTonMessageExpected,
    #[error("Bridge account not found")]
    BridgeAccountNotFound,
    #[error("Already initialized")]
    AlreadyInitialized,
    #[error("Invalid event configuration")]
    InvalidEventConfiguration,
}
