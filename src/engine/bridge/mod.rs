use std::collections::hash_map;
use std::future::Future;
use std::sync::Arc;

use anyhow::Result;
use nekoton_abi::*;
use parking_lot::RwLock;
use tiny_adnl::utils::*;
use tokio::sync::mpsc;
use ton_block::HashmapAugType;
use ton_types::UInt256;

use crate::engine::ton_contracts::*;
use crate::engine::ton_subscriber::*;
use crate::engine::EngineContext;
use crate::utils::*;

pub struct Bridge {
    context: Arc<EngineContext>,

    bridge_account: UInt256,

    bridge_observer: Arc<BridgeObserver>,

    connectors_tx: AccountEventsTx<ConnectorEvent>,
    eth_event_configurations_tx: AccountEventsTx<EthEventConfigurationEvent>,
    ton_event_configurations_tx: AccountEventsTx<TonEventConfigurationEvent>,

    state: Arc<RwLock<BridgeState>>,
}

#[derive(Default)]
struct BridgeState {
    connectors: ConnectorsMap,
    eth_event_configurations: EthEventConfigurationsMap,
    ton_event_configurations: TonEventConfigurationsMap,
    event_code_hashes: EventCodeHashesMap,
}

impl Bridge {
    pub async fn new(context: Arc<EngineContext>, bridge_account: UInt256) -> Result<Arc<Self>> {
        let (bridge_events_tx, bridge_events_rx) = mpsc::unbounded_channel();
        let (connectors_tx, connectors_rx) = mpsc::unbounded_channel();
        let (eth_event_configurations_tx, eth_event_configurations_rx) = mpsc::unbounded_channel();
        let (ton_event_configurations_tx, ton_event_configurations_rx) = mpsc::unbounded_channel();

        let bridge = Arc::new(Bridge {
            context: context.clone(),
            bridge_account,
            bridge_observer: Arc::new(BridgeObserver {
                events_tx: bridge_events_tx,
            }),
            connectors_tx,
            eth_event_configurations_tx,
            ton_event_configurations_tx,
            state: Arc::new(Default::default()),
        });

        bridge.start_listening_events(
            "BridgeContract",
            bridge_events_rx,
            |bridge, event| async move { bridge.process_bridge_event(event).await },
        );

        bridge.start_listening_events(
            "ConnectorContract",
            connectors_rx,
            |bridge, event| async move { bridge.process_connector_event(event).await },
        );

        bridge.start_listening_events(
            "EthEventConfigurationContract",
            eth_event_configurations_rx,
            |bridge, event| async move { bridge.process_eth_event_configuration_event(event).await },
        );

        bridge.start_listening_events(
            "TonEventConfigurationContract",
            ton_event_configurations_rx,
            |bridge, event| async move { bridge.process_ton_event_configuration_event(event).await },
        );

        let shard_accounts = context.get_all_shard_accounts().await?;

        bridge.get_all_configurations(&shard_accounts).await?;

        Ok(bridge)
    }

    pub fn account(&self) -> &UInt256 {
        &self.bridge_account
    }

    async fn process_bridge_event(self: Arc<Self>, event: ConnectorDeployedEvent) -> Result<()> {
        match self.state.write().connectors.entry(event.connector) {
            hash_map::Entry::Vacant(entry) => {
                let observer = Arc::new(ConnectorObserver(self.connectors_tx.clone()));

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

        Ok(())
    }

    async fn process_connector_event(
        self: &Arc<Self>,
        (connector, event): (UInt256, ConnectorEvent),
    ) -> Result<()> {
        match event {
            ConnectorEvent::Enable => self.check_connector_contract(connector).await,
        }
    }

    async fn process_eth_event_configuration_event(
        self: &Arc<Self>,
        (account, event): (UInt256, EthEventConfigurationEvent),
    ) -> Result<()> {
        // TODO

        Ok(())
    }

    async fn process_ton_event_configuration_event(
        self: &Arc<Self>,
        (account, event): (UInt256, TonEventConfigurationEvent),
    ) -> Result<()> {
        // TODO

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

    async fn get_all_configurations(&self, shard_accounts: &ShardAccountsMap) -> Result<()> {
        let ton_subscriber = &self.context.ton_subscriber;

        let contract = shard_accounts
            .find_account(&self.bridge_account)?
            .ok_or(BridgeError::BridgeAccountNotFound)?;
        let bridge = BridgeContract(&contract);

        let mut state = self.state.write();

        // Iterate for all connectors
        for id in 0.. {
            // Compute next connector address
            let connector_account = bridge.derive_connector_address(id)?;

            // Extract details from contract
            let details = match shard_accounts.find_account(&connector_account)? {
                Some(contract) => ConnectorContract(&contract).get_details()?,
                None => {
                    log::info!(
                        "Last connector not found: {}",
                        connector_account.to_hex_string()
                    );
                    break;
                }
            };
            log::info!("Found configuration connector {}: {:?}", id, details);

            let enabled = details.enabled;
            let configuration_account = details.event_configuration;

            let observer = Arc::new(ConnectorObserver(self.connectors_tx.clone()));

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
        let details = EthEventConfigurationContract(contract).get_details()?;
        let observer = EventConfigurationsObserver::new(&self.eth_event_configurations_tx);

        let eth_subscriber = self
            .context
            .eth_subscribers
            .get_subscriber(details.basic_configuration.chain_id)
            .ok_or(BridgeError::UnknownChainId)?;
        let eth_contract_address = details.network_configuration.event_emitter;

        add_event_code_hash(
            &mut state.event_code_hashes,
            &details.basic_configuration.event_code,
            EventType::Eth,
        )?;

        match state.eth_event_configurations.entry(*account) {
            hash_map::Entry::Vacant(entry) => {
                entry.insert(EventConfigurationState {
                    details,
                    observer: observer.clone(),
                });
            }
            hash_map::Entry::Occupied(_) => {
                return Err(BridgeError::EventConfigurationAlreadyExists.into())
            }
        };

        eth_subscriber.subscribe_address(eth_contract_address.into());

        self.context
            .ton_subscriber
            .add_transactions_subscription([*account], &observer);

        Ok(())
    }

    fn add_ton_event_configuration(
        &self,
        state: &mut BridgeState,
        account: &UInt256,
        contract: &ExistingContract,
    ) -> Result<()> {
        let details = TonEventConfigurationContract(contract).get_details()?;
        let observer = EventConfigurationsObserver::new(&self.ton_event_configurations_tx);

        add_event_code_hash(
            &mut state.event_code_hashes,
            &details.basic_configuration.event_code,
            EventType::Ton,
        )?;

        match state.ton_event_configurations.entry(*account) {
            hash_map::Entry::Vacant(entry) => {
                entry.insert(EventConfigurationState {
                    details,
                    observer: observer.clone(),
                });
            }
            hash_map::Entry::Occupied(_) => {
                return Err(BridgeError::EventConfigurationAlreadyExists.into())
            }
        };

        self.context
            .ton_subscriber
            .add_transactions_subscription([*account], &observer);

        Ok(())
    }

    async fn get_all_events(&self, shard_accounts: &ShardAccountsMap) -> Result<()> {
        let shard_accounts = self.context.get_all_shard_accounts().await?;

        let state = self.state.read();

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

                let event_type = match state.event_code_hashes.get(&code_hash) {
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

    fn start_listening_events<E, F, R>(
        self: &Arc<Self>,
        name: &'static str,
        mut events_rx: mpsc::UnboundedReceiver<E>,
        mut handler: F,
    ) where
        E: Send + 'static,
        F: FnMut(Arc<Self>, E) -> R + Send + 'static,
        R: Future<Output = Result<()>> + Send,
    {
        let bridge = Arc::downgrade(self);

        tokio::spawn(async move {
            while let Some(event) = events_rx.recv().await {
                let bridge = match bridge.upgrade() {
                    Some(bridge) => bridge,
                    None => break,
                };

                if let Err(e) = handler(bridge, event).await {
                    log::error!("{}: Failed to handle event: {:?}", name, e);
                }
            }

            events_rx.close();
            while events_rx.recv().await.is_some() {}
        });
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

impl<T> EventConfigurationsObserver<T> {
    fn new(tx: &AccountEventsTx<T>) -> Arc<Self> {
        Arc::new(Self(tx.clone()))
    }
}

impl<T> TransactionsSubscription for EventConfigurationsObserver<T>
where
    T: ReadFromTransaction + std::fmt::Debug + Send + Sync,
{
    fn handle_transaction(&self, ctx: TxContext<'_>) -> Result<()> {
        let event = T::read_from_transaction(&ctx);

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

type BridgeEventsTx = mpsc::UnboundedSender<ConnectorDeployedEvent>;
type BridgeEventsRx = mpsc::UnboundedReceiver<ConnectorDeployedEvent>;

type ConnectorsMap = FxHashMap<UInt256, ConnectorState>;
type EthEventConfigurationsMap = FxHashMap<
    UInt256,
    EventConfigurationState<EthEventConfigurationDetails, EthEventConfigurationEvent>,
>;
type TonEventConfigurationsMap = FxHashMap<
    UInt256,
    EventConfigurationState<TonEventConfigurationDetails, TonEventConfigurationEvent>,
>;
type EventCodeHashesMap = FxHashMap<UInt256, EventType>;

#[derive(thiserror::Error, Debug)]
enum BridgeError {
    #[error("Unknown chain id")]
    UnknownChainId,
    #[error("Unknown connector")]
    UnknownConnector,
    #[error("Bridge account not found")]
    BridgeAccountNotFound,
    #[error("Invalid event configuration")]
    InvalidEventConfiguration,
    #[error("Event configuration already exists")]
    EventConfigurationAlreadyExists,
}
