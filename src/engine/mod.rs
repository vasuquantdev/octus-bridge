use std::collections::hash_map::{self, HashMap};
use std::sync::Arc;

use anyhow::{Context, Result};
use dashmap::DashMap;
use nekoton_utils::*;
use parking_lot::RwLock;
use tokio::sync::mpsc;
use ton_block::{HashmapAugType, Serializable};
use ton_types::UInt256;

use self::ton_contracts::*;
use self::ton_subscriber::*;
use crate::config::*;
use crate::utils::*;
use nekoton_abi::UnpackAbiPlain;

mod ton_contracts;
mod ton_subscriber;

pub struct Engine {
    bridge_account: UInt256,
    ton_engine: Arc<ton_indexer::Engine>,
    ton_subscriber: Arc<TonSubscriber>,
    connectors_observer: Arc<ConnectorsObserver>,
    eth_configurations_observer: Arc<EthEventConfigurationsObserver>,
    ton_configurations_observer: Arc<TonEventConfigurationsObserver>,
    bridge_observer: Arc<BridgeObserver>,
    event_code_hashes: Arc<RwLock<EventCodeHashesMap>>,
    initialized: tokio::sync::Mutex<bool>,
}

impl Engine {
    pub async fn new(
        config: RelayConfig,
        global_config: ton_indexer::GlobalConfig,
    ) -> Result<Arc<Self>> {
        let bridge_account =
            UInt256::from_be_bytes(&config.bridge_address.address().get_bytestring(0));

        let ton_subscriber = TonSubscriber::new();

        let ton_engine = ton_indexer::Engine::new(
            config.indexer,
            global_config,
            vec![ton_subscriber.clone() as Arc<dyn ton_indexer::Subscriber>],
        )
        .await?;

        let connectors = Arc::new(ConnectorsMap::new());
        let eth_event_configurations = Arc::new(EthEventConfigurationsMap::new());
        let ton_event_configurations = Arc::new(TonEventConfigurationsMap::new());

        let (connector_events_tx, _connector_events_rx) = mpsc::unbounded_channel();
        let (bridge_events_tx, _bridge_events_rx) = mpsc::unbounded_channel();

        // TODO: create configurations manager

        Ok(Arc::new(Self {
            bridge_account,
            ton_engine,
            ton_subscriber,
            connectors_observer: Arc::new(ConnectorsObserver::new(connectors, connector_events_tx)),
            eth_configurations_observer: Arc::new(EthEventConfigurationsObserver::new(
                eth_event_configurations,
            )),
            ton_configurations_observer: Arc::new(TonEventConfigurationsObserver::new(
                ton_event_configurations,
            )),
            bridge_observer: Arc::new(BridgeObserver::new(bridge_events_tx)),
            event_code_hashes: Arc::new(Default::default()),
            initialized: tokio::sync::Mutex::new(false),
        }))
    }

    pub async fn start(&self) -> Result<()> {
        let mut initialized = self.initialized.lock().await;
        if *initialized {
            return Err(EngineError::AlreadyInitialized.into());
        }

        self.ton_engine.start().await?;
        self.ton_subscriber.start().await?;

        self.ton_subscriber
            .add_transactions_subscription([self.bridge_account], &self.bridge_observer);

        self.get_all_configurations().await?;
        self.get_all_events().await?;

        // TODO: start eth indexer

        *initialized = true;
        Ok(())
    }

    async fn get_all_events(&self) -> Result<()> {
        let shard_accounts = self.get_all_shard_accounts().await?;

        let event_code_hashes = self.event_code_hashes.read();

        for (_, accounts) in shard_accounts {
            accounts
                .iterate_with_keys(|hash, shard_account| {
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
                })
                .convert()?;
        }

        Ok(())
    }

    async fn get_all_configurations(&self) -> Result<()> {
        let shard_accounts = self.get_all_shard_accounts().await?;

        let contract = shard_accounts
            .find_account(&self.bridge_account)?
            .ok_or(EngineError::BridgeAccountNotFound)?;
        let bridge = BridgeContract(&contract);

        let mut connectors = Vec::new();
        let mut active_configuration_accounts = Vec::new();
        let mut event_code_hashes = HashMap::with_capacity(2);

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
                    id,
                    details,
                    event_type: ConnectorConfigurationType::Unknown,
                },
            ));
        }

        let mut ctx = EventConfigurationsProcessingContext {
            event_code_hashes: &mut event_code_hashes,
            eth_event_configurations: &self.eth_configurations_observer.configurations,
            ton_event_configurations: &self.ton_configurations_observer.configurations,
        };

        for (connector_id, account) in active_configuration_accounts {
            match shard_accounts.process_event_configuration(&account, &mut ctx) {
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

        connectors
            .into_iter()
            .for_each(|(account, state)| self.connectors_observer.add_connector(account, state));

        self.ton_subscriber.add_transactions_subscription(
            self.connectors_observer.connector_accounts(),
            &self.connectors_observer,
        );

        self.ton_subscriber.add_transactions_subscription(
            self.eth_configurations_observer.configuration_accounts(),
            &self.eth_configurations_observer,
        );

        self.ton_subscriber.add_transactions_subscription(
            self.ton_configurations_observer.configuration_accounts(),
            &self.ton_configurations_observer,
        );

        // TODO

        Ok(())
    }

    async fn get_all_shard_accounts(&self) -> Result<ShardAccountsMap> {
        let shard_blocks = self.ton_subscriber.wait_shards().await?;

        let mut shard_accounts = HashMap::with_capacity(shard_blocks.len());
        for (shard_ident, block_id) in shard_blocks {
            let shard = self.ton_engine.wait_state(&block_id, None, false).await?;
            let accounts = shard.state().read_accounts().convert()?;
            shard_accounts.insert(shard_ident, accounts);
        }

        Ok(shard_accounts)
    }

    async fn send_ton_message(&self, message: &ton_block::Message) -> Result<()> {
        let to = match message.header() {
            ton_block::CommonMsgInfo::ExtInMsgInfo(header) => {
                ton_block::AccountIdPrefixFull::prefix(&header.dst).convert()?
            }
            _ => return Err(EngineError::ExternalTonMessageExpected.into()),
        };

        let cells = message.write_to_new_cell().convert()?.into();
        let serialized = ton_types::serialize_toc(&cells).convert()?;

        self.ton_engine
            .broadcast_external_message(&to, &serialized)
            .await
    }
}

#[derive(Debug, Copy, Clone)]
struct ConnectorState {
    id: u64,
    details: ConnectorDetails,
    event_type: ConnectorConfigurationType,
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
    Disable,
}

/// Listener for bridge transactions
///
/// **Registered only for bridge account**
struct BridgeObserver {
    events_tx: BridgeEventsTx,
}

impl BridgeObserver {
    fn new(events_tx: BridgeEventsTx) -> Self {
        Self { events_tx }
    }
}

impl TransactionsSubscription for BridgeObserver {
    fn handle_transaction(
        &self,
        _block_info: &ton_block::BlockInfo,
        _account: &UInt256,
        _transaction_hash: &UInt256,
        transaction: &ton_block::Transaction,
    ) -> Result<()> {
        // Skip non-ordinary or aborted transactions
        let descr = transaction.description.read_struct().convert()?;
        if !matches!(descr, ton_block::TransactionDescr::Ordinary(info) if !info.aborted) {
            return Ok(());
        }

        // Parse all outgoing messages and search proper event
        let mut event: Option<ConnectorDeployedEvent> = None;
        transaction
            .out_msgs
            .iterate(|message| {
                let message = message.0;
                if !matches!(message.header(), ton_block::CommonMsgInfo::ExtOutMsgInfo(_)) {
                    return Ok(true);
                }

                let body = match message.body() {
                    Some(body) => body,
                    None => return Ok(true),
                };

                let id = nekoton_abi::read_function_id(&body);
                if matches!(id, Ok(id) if id == bridge_contract::events::connector_deployed().id) {
                    match bridge_contract::events::connector_deployed()
                        .decode_input(body)
                        .convert()
                        .and_then(|tokens| tokens.unpack().map_err(anyhow::Error::from))
                    {
                        Ok(parsed) => event = Some(parsed),
                        Err(e) => {
                            log::error!("Failed to parse bridge event: {:?}", e);
                        }
                    };
                }

                Ok(true)
            })
            .convert()?;

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
struct ConnectorsObserver {
    connectors: Arc<ConnectorsMap>,
    events_tx: ConnectorEventsTx,
}

impl ConnectorsObserver {
    fn new(connectors: Arc<ConnectorsMap>, events_tx: ConnectorEventsTx) -> Self {
        Self {
            connectors,
            events_tx,
        }
    }

    fn add_connector(&self, account: UInt256, state: ConnectorState) {
        self.connectors.insert(account, state);
    }

    fn connector_accounts(&'_ self) -> impl Iterator<Item = UInt256> + '_ {
        self.connectors.iter().map(|item| *item.key())
    }
}

impl TransactionsSubscription for ConnectorsObserver {
    fn handle_transaction(
        &self,
        _block_info: &ton_block::BlockInfo,
        account: &UInt256,
        _transaction_hash: &UInt256,
        transaction: &ton_block::Transaction,
    ) -> Result<()> {
        // Skip non-ordinary or aborted transactions
        let descr = transaction.description.read_struct().convert()?;
        if !matches!(descr, ton_block::TransactionDescr::Ordinary(info) if !info.aborted) {
            return Ok(());
        }

        // Parse all outgoing messages and search proper event
        let mut event = None;
        transaction
            .out_msgs
            .iterate(|message| {
                let message = message.0;
                if !matches!(message.header(), ton_block::CommonMsgInfo::ExtOutMsgInfo(_)) {
                    return Ok(true);
                }

                let body = match message.body() {
                    Some(body) => body,
                    None => return Ok(true),
                };

                match nekoton_abi::read_function_id(&body) {
                    Ok(id) if id == connector_contract::events::enabled().id => {
                        event = Some(ConnectorEvent::Enable);
                    }
                    Ok(id) if id == connector_contract::events::disabled().id => {
                        event = Some(ConnectorEvent::Disable);
                    }
                    _ => { /* do nothing */ }
                }

                Ok(true)
            })
            .convert()?;

        log::info!(
            "Got transaction on connector {}: {:?}",
            account.to_hex_string(),
            event
        );

        // Send event to event manager if it exist
        if let Some(event) = event {
            self.events_tx.send((*account, event)).ok();
        }

        // Done
        Ok(())
    }
}

/// Listener for ETH event configuration transactions
///
/// **Registered for each active ETH event configuration account**
struct EthEventConfigurationsObserver {
    configurations: Arc<EthEventConfigurationsMap>,
}

impl EthEventConfigurationsObserver {
    fn new(configurations: Arc<EthEventConfigurationsMap>) -> Self {
        Self { configurations }
    }

    fn configuration_accounts(&'_ self) -> impl Iterator<Item = UInt256> + '_ {
        self.configurations.iter().map(|item| *item.key())
    }
}

impl TransactionsSubscription for EthEventConfigurationsObserver {
    fn handle_transaction(
        &self,
        block_info: &ton_block::BlockInfo,
        account: &UInt256,
        transaction_hash: &UInt256,
        transaction: &ton_block::Transaction,
    ) -> Result<()> {
        log::info!(
            "Got transaction on eth event configuration {}",
            account.to_hex_string()
        );

        // TODO: parse configuration events

        Ok(())
    }
}

/// Listener for TON event configuration transactions
///
/// **Registered for each active TON event configuration account**
struct TonEventConfigurationsObserver {
    configurations: Arc<TonEventConfigurationsMap>,
}

impl TonEventConfigurationsObserver {
    fn new(configurations: Arc<TonEventConfigurationsMap>) -> Self {
        Self { configurations }
    }

    fn configuration_accounts(&'_ self) -> impl Iterator<Item = UInt256> + '_ {
        self.configurations.iter().map(|item| *item.key())
    }
}

impl TransactionsSubscription for TonEventConfigurationsObserver {
    fn handle_transaction(
        &self,
        block_info: &ton_block::BlockInfo,
        account: &UInt256,
        transaction_hash: &UInt256,
        transaction: &ton_block::Transaction,
    ) -> Result<()> {
        log::info!(
            "Got transaction on ton event configuration {}",
            account.to_hex_string()
        );

        // TODO: parse configuration events

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

/// Helper trait to reduce boilerplate for getting accounts from shards state
trait ShardAccountsMapExt {
    /// Looks for a suitable shard and tries to extract information about the contract from it
    fn find_account(&self, account: &UInt256) -> Result<Option<ExistingContract>>;

    /// Searches for the account contract and extracts the configuration information into context
    fn process_event_configuration(
        &self,
        account: &UInt256,
        ctx: &mut EventConfigurationsProcessingContext<'_>,
    ) -> Result<Option<EventType>>;
}

impl ShardAccountsMapExt for ShardAccountsMap {
    fn find_account(&self, account: &UInt256) -> Result<Option<ExistingContract>> {
        // Search suitable shard for account by prefix.
        // NOTE: In **most** cases suitable shard will be found
        let item = self
            .iter()
            .find(|(shard_ident, _)| contains_account(shard_ident, account));

        match item {
            // Search account in shard state
            Some((_, shard)) => match shard
                .get(account)
                .convert()
                .and_then(|account| ExistingContract::from_shard_account(&account))?
            {
                // Account found
                Some(contract) => Ok(Some(contract)),
                // Account was not found (it never had any transactions) or there is not AccountStuff in it
                None => Ok(None),
            },
            // Exceptional situation when no suitable shard was found
            None => Err(EngineError::InvalidContractAddress).context("No suitable shard found"),
        }
    }

    fn process_event_configuration(
        &self,
        account: &UInt256,
        ctx: &mut EventConfigurationsProcessingContext<'_>,
    ) -> Result<Option<EventType>> {
        // Find configuration contact
        let contract = match self.find_account(account)? {
            Some(contract) => contract,
            None => {
                // It is a strange situation when connector contains an address of the contract
                // which doesn't exist, so log it here to investigate it later
                log::warn!(
                    "Connected configuration was not found: {}",
                    account.to_hex_string()
                );

                // Did nothing
                return Ok(None);
            }
        };

        // Get event type using base contract abi
        let event_type = EventConfigurationBaseContract(&contract).get_type()?;

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
                let details = EthEventConfigurationContract(&contract).get_details()?;
                log::info!("Ethereum event configuration details: {:?}", details);

                fill_event_code_hash(&details.basic_configuration.event_code)?;
                ctx.eth_event_configurations.insert(*account, details);
            }
            // Extract and populate TON event configuration details
            EventType::Ton => {
                let details = TonEventConfigurationContract(&contract).get_details()?;
                log::info!("TON event configuration details: {:?}", details);

                fill_event_code_hash(&details.basic_configuration.event_code)?;
                ctx.ton_event_configurations.insert(*account, details);
            }
        };

        // Done
        Ok(Some(event_type))
    }
}

type ConnectorEventsTx = mpsc::UnboundedSender<(UInt256, ConnectorEvent)>;
type BridgeEventsTx = mpsc::UnboundedSender<ConnectorDeployedEvent>;

type ShardAccountsMap = HashMap<ton_block::ShardIdent, ton_block::ShardAccounts>;
type EventCodeHashesMap = HashMap<UInt256, EventType>;

type ConnectorsMap = DashMap<UInt256, ConnectorState>;
type EthEventConfigurationsMap = DashMap<UInt256, EthEventConfigurationDetails>;
type TonEventConfigurationsMap = DashMap<UInt256, TonEventConfigurationDetails>;

#[derive(thiserror::Error, Debug)]
enum EngineError {
    #[error("External ton message expected")]
    ExternalTonMessageExpected,
    #[error("Bridge account not found")]
    BridgeAccountNotFound,
    #[error("Invalid contract address")]
    InvalidContractAddress,
    #[error("Already initialized")]
    AlreadyInitialized,
    #[error("Invalid event configuration")]
    InvalidEventConfiguration,
}
