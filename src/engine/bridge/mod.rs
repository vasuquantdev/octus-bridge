use std::collections::hash_map;
use std::sync::Arc;

use anyhow::{Context, Result};
use chrono::Utc;
use nekoton_abi::*;
use parking_lot::RwLock;
use tiny_adnl::utils::*;
use tokio::sync::{mpsc, watch};
use ton_block::{HashmapAugType, Serializable};
use ton_types::UInt256;

use crate::engine::ton_contracts::*;
use crate::engine::ton_subscriber::*;
use crate::engine::EngineContext;
use crate::utils::*;

pub struct Bridge {
    context: Arc<EngineContext>,

    bridge_account: UInt256,

    bridge_observer: Arc<BridgeObserver>,

    connector_events_tx: AccountEventsTx<ConnectorEvent>,
    connectors: ConnectorsMap,

    eth_event_configurations_tx: AccountEventsTx<EthEventConfigurationEvent>,
    eth_event_configurations: EthEventConfigurationsMap,

    ton_event_configurations_tx: AccountEventsTx<TonEventConfigurationEvent>,
    ton_event_configurations: TonEventConfigurationsMap,

    event_code_hashes: Arc<RwLock<EventCodeHashesMap>>,
}

impl Bridge {
    pub async fn new(context: Arc<EngineContext>, bridge_account: UInt256) -> Result<Arc<Self>> {
        let (bridge_events_tx, bridge_events_rx) = mpsc::unbounded_channel();
        let (connector_events_tx, connector_events_rx) = mpsc::unbounded_channel();
        let (eth_event_configurations_tx, eth_event_configurations_rx) = mpsc::unbounded_channel();
        let (ton_event_configurations_tx, ton_event_configurations_rx) = mpsc::unbounded_channel();

        let bridge = Arc::new(Bridge {
            context,
            bridge_account,
            bridge_observer: Arc::new(BridgeObserver {
                events_tx: bridge_events_tx,
            }),
            connector_events_tx,
            connectors: Default::default(),
            eth_event_configurations_tx,
            eth_event_configurations: Default::default(),
            ton_event_configurations_tx,
            ton_event_configurations: Default::default(),
            event_code_hashes: Arc::new(Default::default()),
        });

        bridge.start_listening_bridge_events(bridge_events_rx);
        bridge.start_listening_connector_events(connector_events_rx);

        let bridge_contract = match shard_accounts.find_account()? {
            Some(contract) => contract,
            None => bridge_account,
        };

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
                let engine = match engine.upgrade() {
                    Some(engine) => engine,
                    None => break,
                };

                match engine.connectors.entry(event.connector) {
                    Entry::Vacant(entry) => {
                        let observer =
                            Arc::new(ConnectorObserver(engine.connector_events_tx.clone()));

                        let entry = entry.insert(ConnectorState {
                            details: ConnectorDetails {
                                id: event.id,
                                event_configuration: event.event_configuration,
                                enabled: false,
                            },
                            event_type: ConnectorConfigurationType::Unknown,
                            observer,
                        });

                        engine
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
                    if let Err(e) = engine.process_bridge_event(event).await {
                        log::error!("Failed to process bridge event: {:?}", e);
                    }
                });
            }
        });
    }

    async fn process_bridge_event(&self, event: ConnectorDeployedEvent) -> Result<()> {
        // Process event configuration
        let event_type = {
            // Wait until event configuration state is found
            let contract = loop {
                // TODO: add finite retires for getting configuration info

                match self
                    .ton_subscriber
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
                ton_event_configurations: &self.ton_event_configurations,
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

        match event_type {
            EventType::Eth => self.ton_subscriber.add_transactions_subscription(
                [event.event_configuration],
                &self.eth_configurations_observer,
            ),
            EventType::Ton => self.ton_subscriber.add_transactions_subscription(
                [event.event_configuration],
                &self.ton_configurations_observer,
            ),
        }

        // self.ton_subscriber
        //     .add_transactions_subscription([event.connector], &self.connectors_observer);

        // TODO: check connector state and remove configuration subscription if it was disabled

        Ok(())
    }

    fn start_listening_connector_events(
        self: &Arc<Self>,
        mut connector_events_rx: ConnectorEventsRx,
    ) {
        let engine = Arc::downgrade(self);

        tokio::spawn(async move {
            while let Some(event) = connector_events_rx.recv().await {
                let engine = match engine.upgrade() {
                    Some(engine) => engine,
                    None => break,
                };

                // TODO
            }
        });
    }

    async fn get_all_configurations(&self, shard_accounts: &ShardAccountsMap) -> Result<()> {
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
                    observer: Arc::new(ConnectorObserver(self.connector_events_tx.clone())),
                },
            ));
        }

        let mut ctx = EventConfigurationsProcessingContext {
            event_code_hashes: &mut event_code_hashes,
            eth_event_configurations: &self.eth_event_configurations,
            ton_event_configurations: &self.ton_event_configurations,
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

        for item in &self.connectors {
            self.ton_subscriber
                .add_transactions_subscription([*item.key()], &item.observer);
        }

        self.ton_subscriber.add_transactions_subscription(
            self.eth_event_configurations.iter().map(|item| *item.key()),
            &self.eth_configurations_observer,
        );

        self.ton_subscriber.add_transactions_subscription(
            self.ton_event_configurations.iter().map(|item| *item.key()),
            &self.ton_configurations_observer,
        );

        // TODO

        Ok(())
    }

    async fn get_all_events(&self, shard_accounts: &ShardAccountsMap) -> Result<()> {
        let shard_accounts = self.get_all_shard_accounts().await?;

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
    /// Details of parsed TON event configurations
    ton_event_configurations: &'a TonEventConfigurationsMap,
}

/// Searches for the account contract and extracts the configuration information into context
fn process_event_configuration(
    account: &UInt256,
    contract: &ExistingContract,
    ctx: &mut EventConfigurationsProcessingContext<'_>,
) -> Result<Option<EventType>> {
    // Get event type using base contract abi
    let event_type = EventConfigurationBaseContract(contract).get_type()?;

    // Small helper to populate unique event contract code hashes
    let mut fill_event_code_hash = |code: &ton_types::Cell| {
        match ctx.event_code_hashes.entry(code.repr_hash()) {
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

            fill_event_code_hash(&details.basic_configuration.event_code)?;
            ctx.eth_event_configurations.insert(*account, details);
        }
        // Extract and populate TON event configuration details
        EventType::Ton => {
            let details = TonEventConfigurationContract(contract).get_details()?;
            log::info!("TON event configuration details: {:?}", details);

            fill_event_code_hash(&details.basic_configuration.event_code)?;
            ctx.ton_event_configurations.insert(*account, details);
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
        iterate_transaction_events(&ctx, |body| {
            let id = nekoton_abi::read_function_id(&body);
            if matches!(id, Ok(id) if id == bridge_contract::events::connector_deployed().id) {
                match bridge_contract::events::connector_deployed()
                    .decode_input(body)
                    .and_then(|tokens| tokens.unpack().map_err(anyhow::Error::from))
                {
                    Ok(parsed) => event = Some(parsed),
                    Err(e) => {
                        log::error!("Failed to parse bridge event: {:?}", e);
                    }
                };
            }
        })?;

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
struct ConnectorObserver(ConnectorEventsTx);

impl TransactionsSubscription for ConnectorObserver {
    fn handle_transaction(&self, ctx: TxContext<'_>) -> Result<()> {
        // Parse all outgoing messages and search proper event
        let mut event = None;
        iterate_transaction_events(&ctx, |body| match nekoton_abi::read_function_id(&body) {
            Ok(id) if id == connector_contract::events::enabled().id => {
                event = Some(ConnectorEvent::Enable);
            }
            _ => { /* do nothing */ }
        })?;

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
struct EventConfigurationsObserver<T> {
    events_tx: AccountEventsTx<T>,
    function_update: &'static ton_abi::Function,
}

impl<T> TransactionsSubscription for EventConfigurationsObserver<T>
where
    T: std::fmt::Debug + Send + Sync,
    ton_abi::TokenValue: nekoton_abi::UnpackAbi<T>,
{
    fn handle_transaction(&self, ctx: TxContext<'_>) -> Result<()> {
        let mut event = None;
        parse_incoming_internal_message(&ctx, |body| match nekoton_abi::read_function_id(&body) {
            Ok(id) if id == self.function_update.input_id => {
                let input = self
                    .function_update
                    .decode_input(body, true)
                    .and_then(|tokens| {
                        let mut tokens = tokens.into_unpacker();
                        Ok(EventConfigurationEvent::Update {
                            basic_configuration: tokens.unpack_next::<BasicConfiguration>()?,
                            network_configuration: tokens.unpack_next::<T>()?,
                        })
                    });
                match input {
                    Ok(input) => event = Some(input),
                    Err(e) => {
                        log::error!("Failed to parse event configuration transaction: {:?}", e);
                    }
                }
            }
            _ => { /* do nothing */ }
        });

        log::info!(
            "Got transaction on event configuration {}: {:?}",
            ctx.account.to_hex_string(),
            event
        );

        // Send event to event manager if it exist
        if let Some(event) = event {
            self.events_tx.send((*ctx.account, event)).ok();
        }

        // Done
        Ok(())
    }
}

#[derive(Debug, Clone)]
enum TonEventConfigurationEvent {
    SetEndTimestamp {
        end_timestamp: u32,
    },
    EventDeployed {
        vote_data: TonEventVoteData,
        address: UInt256,
    },
}

#[derive(Debug, Clone)]
enum EthEventConfigurationEvent {
    SetEndBlockNumber {
        end_block_number: u32,
    },
    EventDeployed {
        vote_data: EthEventVoteData,
        address: UInt256,
    },
}

type AccountEventsRx<T> = mpsc::UnboundedReceiver<(UInt256, T)>;
type AccountEventsTx<T> = mpsc::UnboundedSender<(UInt256, T)>;

type BridgeEventsTx = mpsc::UnboundedSender<ConnectorDeployedEvent>;
type BridgeEventsRx = mpsc::UnboundedReceiver<ConnectorDeployedEvent>;

type EventCodeHashesMap = FxHashMap<UInt256, EventType>;

type ConnectorsMap = FxDashMap<UInt256, ConnectorState>;
type EthEventConfigurationsMap = FxDashMap<UInt256, EthEventConfigurationDetails>;
type TonEventConfigurationsMap = FxDashMap<UInt256, TonEventConfigurationDetails>;

#[derive(thiserror::Error, Debug)]
enum EngineError {
    #[error("External ton message expected")]
    ExternalTonMessageExpected,
    #[error("Bridge account not found")]
    BridgeAccountNotFound,
    #[error("Staking account not found")]
    StakingAccountNotFound,
    #[error("Relay Round account not found")]
    RelayRoundAccountNotFound,
    #[error("Already initialized")]
    AlreadyInitialized,
    #[error("Invalid event configuration")]
    InvalidEventConfiguration,
}
