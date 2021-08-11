use std::collections::hash_map;
use std::str::FromStr;
use std::sync::Arc;

use anyhow::{Context, Result};
use chrono::Utc;
use nekoton_abi::*;
use nekoton_utils::*;
use parking_lot::RwLock;
use tiny_adnl::utils::*;
use tokio::sync::mpsc;
use tokio::time::{sleep_until, Duration, Instant};
use ton_block::{HashmapAugType, Serializable};
use ton_types::UInt256;

use self::ton_contracts::*;
use self::ton_subscriber::*;
use crate::config::*;
use crate::utils::*;

mod ton_contracts;
mod ton_subscriber;

pub struct Engine {
    bridge_account: UInt256,
    staking_account: UInt256,
    ton_engine: Arc<ton_indexer::Engine>,
    ton_subscriber: Arc<TonSubscriber>,

    eth_configurations_observer: Arc<EventConfigurationsObserver<EthEventConfiguration>>,
    ton_configurations_observer: Arc<EventConfigurationsObserver<TonEventConfiguration>>,
    connectors_observer: Arc<ConnectorsObserver>,
    bridge_observer: Arc<BridgeObserver>,
    staking_observer: Arc<StakingObserver>,

    eth_event_configurations: EthEventConfigurationsMap,
    ton_event_configurations: TonEventConfigurationsMap,
    connectors: ConnectorsMap,
    event_code_hashes: Arc<RwLock<EventCodeHashesMap>>,

    initialized: tokio::sync::Mutex<bool>,

    vote_state: Arc<RwLock<VoteState>>,
}

impl Engine {
    pub async fn new(
        config: RelayConfig,
        global_config: ton_indexer::GlobalConfig,
    ) -> Result<Arc<Self>> {
        let bridge_account =
            UInt256::from_be_bytes(&config.bridge_address.address().get_bytestring(0));

        let staking_account =
            UInt256::from_be_bytes(&config.staking_address.address().get_bytestring(0));

        let ton_subscriber = TonSubscriber::new();

        let ton_engine = ton_indexer::Engine::new(
            config.indexer,
            global_config,
            vec![ton_subscriber.clone() as Arc<dyn ton_indexer::Subscriber>],
        )
        .await?;

        let (eth_event_configuration_events_tx, _eth_event_configuration_events_rx) =
            mpsc::unbounded_channel();
        let (ton_event_configuration_events_tx, _ton_event_configuration_events_rx) =
            mpsc::unbounded_channel();
        let (connector_events_tx, _connector_events_rx) = mpsc::unbounded_channel();
        let (bridge_events_tx, bridge_events_rx) = mpsc::unbounded_channel();
        let (staking_events_tx, staking_events_rx) = mpsc::unbounded_channel();

        let engine = Arc::new(Self {
            bridge_account,
            staking_account,
            ton_engine,
            ton_subscriber,
            eth_configurations_observer: Arc::new(EventConfigurationsObserver {
                events_tx: eth_event_configuration_events_tx,
                function_update: eth_event_configuration_contract::update(),
            }),
            ton_configurations_observer: Arc::new(EventConfigurationsObserver {
                events_tx: ton_event_configuration_events_tx,
                function_update: ton_event_configuration_contract::update(),
            }),
            connectors_observer: Arc::new(ConnectorsObserver {
                events_tx: connector_events_tx,
            }),
            bridge_observer: Arc::new(BridgeObserver {
                events_tx: bridge_events_tx,
            }),
            staking_observer: Arc::new(StakingObserver {
                events_tx: staking_events_tx,
            }),
            eth_event_configurations: Default::default(),
            ton_event_configurations: Default::default(),
            connectors: Default::default(),
            event_code_hashes: Default::default(),
            initialized: Default::default(),
            vote_state: Arc::new(Default::default()),
        });

        engine.start_listening_bridge_events(bridge_events_rx);
        engine.start_listening_staking_events(staking_events_rx);

        Ok(engine)
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
                        entry.insert(ConnectorState {
                            details: ConnectorDetails {
                                id: event.id,
                                event_configuration: event.event_configuration,
                                enabled: true,
                            },
                            event_type: ConnectorConfigurationType::Unknown,
                        });
                    }
                    Entry::Occupied(_) => {
                        log::error!(
                            "Got connector deployment event but it already exists: {}",
                            event.connector.to_hex_string()
                        );
                        continue;
                    }
                };

                tokio::spawn(async move {
                    if let Err(e) = engine.process_bridge_event(event).await {
                        log::error!("Failed to process bridge event: {:?}", e);
                    }
                });
            }

            bridge_events_rx.close();
            while bridge_events_rx.recv().await.is_some() {}
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

        self.ton_subscriber
            .add_transactions_subscription([event.connector], &self.connectors_observer);

        // TODO: check connector state and remove configuration subscription if it was disabled

        Ok(())
    }

    fn start_listening_staking_events(self: &Arc<Self>, mut staking_events_rx: StakingEventsRx) {
        let engine = Arc::downgrade(self);

        tokio::spawn(async move {
            while let Some(event) = staking_events_rx.recv().await {
                let engine = match engine.upgrade() {
                    Some(engine) => engine,
                    None => break,
                };

                let shard_accounts = match engine.get_all_shard_accounts().await {
                    Ok(shard_accounts) => shard_accounts,
                    Err(e) => {
                        log::error!("Failed to get shard accounts: {:?}", e);
                        continue;
                    }
                };

                if let Err(e) = update_vote_state(
                    &shard_accounts,
                    &event.round_addr,
                    &mut engine.vote_state.write().new_round,
                ) {
                    log::error!("Failed to update vote state: {:?}", e);
                    continue;
                }

                let vote_state = engine.vote_state.clone();
                let deadline = Instant::now()
                    + Duration::from_secs(
                        (event.round_start_time - Utc::now().timestamp() as u128) as u64,
                    );
                run_vote_state_timer(deadline, vote_state);
            }

            staking_events_rx.close();
            while staking_events_rx.recv().await.is_some() {}
        });
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
        self.ton_subscriber
            .add_transactions_subscription([self.staking_account], &self.staking_observer);

        self.get_all_configurations().await?;
        self.get_all_events().await?;

        self.init_vote_state().await?;

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

        self.ton_subscriber.add_transactions_subscription(
            self.connectors.iter().map(|item| *item.key()),
            &self.connectors_observer,
        );

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

    async fn init_vote_state(&self) -> Result<()> {
        let shard_accounts = self.get_all_shard_accounts().await?;

        let contract = shard_accounts
            .find_account(&self.staking_account)?
            .ok_or(EngineError::StakingAccountNotFound)?;
        let staking = StakingContract(&contract);

        let round_num = staking.current_relay_round()?;
        let current_time = Utc::now().timestamp() as u128;
        let current_relay_round_start_time = staking.current_relay_round_start_time()?;

        let current_relay_round_addr;
        if current_time < current_relay_round_start_time {
            current_relay_round_addr = staking.get_relay_round_address(round_num - 1)?;

            let new_relay_round_addr = staking.get_relay_round_address(round_num)?;
            update_vote_state(
                &shard_accounts,
                &new_relay_round_addr,
                &mut self.vote_state.write().new_round,
            )?;

            let vote_state = self.vote_state.clone();
            let deadline = Instant::now()
                + Duration::from_secs((current_relay_round_start_time - current_time) as u64);
            run_vote_state_timer(deadline, vote_state);
        } else {
            current_relay_round_addr = staking.get_relay_round_address(round_num)?;
        }
        update_vote_state(
            &shard_accounts,
            &current_relay_round_addr,
            &mut self.vote_state.write().current_round,
        )?;

        Ok(())
    }

    async fn get_all_shard_accounts(&self) -> Result<ShardAccountsMap> {
        let shard_blocks = self.ton_subscriber.wait_shards().await?;

        let mut shard_accounts =
            FxHashMap::with_capacity_and_hasher(shard_blocks.len(), Default::default());
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

/// Timer task that switch round state
fn run_vote_state_timer(deadline: Instant, vote_state: Arc<RwLock<VoteState>>) {
    tokio::spawn(async move {
        sleep_until(deadline).await;

        vote_state.write().current_round = vote_state.read().new_round;
        vote_state.write().new_round = VoteStateType::None;
    });
}

fn update_vote_state(
    shard_accounts: &ShardAccountsMap,
    addr: &UInt256,
    vote_state: &mut VoteStateType,
) -> Result<()> {
    // Test key
    let relay_pubkey =
        UInt256::from_str("e4e82dd4c0df20b0467b1cf48320f4921796c6c8f76ff5999f91ad9175186635")
            .unwrap();

    let contract = shard_accounts
        .find_account(addr)?
        .ok_or(EngineError::RelayRoundAccountNotFound)?;
    let relay_round = RelayRoundContract(&contract);

    if relay_round.relay_keys()?.value0.contains(&relay_pubkey) {
        *vote_state = VoteStateType::InRound;
    } else {
        *vote_state = VoteStateType::NotInRound;
    }

    Ok(())
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

#[derive(Debug, Copy, Clone)]
struct ConnectorState {
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

/// Whether Relay is able to vote in each of round
#[derive(Debug, Copy, Clone, Default)]
struct VoteState {
    current_round: VoteStateType,
    new_round: VoteStateType,
}

#[derive(Debug, Copy, Clone)]
enum VoteStateType {
    None,
    InRound,
    NotInRound,
}

impl Default for VoteStateType {
    fn default() -> Self {
        VoteStateType::None
    }
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
                    .convert()
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

/// Listener for staking transactions
///
/// **Registered only for staking account**
struct StakingObserver {
    events_tx: StakingEventsTx,
}

impl TransactionsSubscription for StakingObserver {
    fn handle_transaction(&self, ctx: TxContext<'_>) -> Result<()> {
        // Parse all outgoing messages and search proper event
        let mut event: Option<RelayRoundInitializedEvent> = None;
        iterate_transaction_events(&ctx, |body| {
            let id = nekoton_abi::read_function_id(&body);
            if matches!(id, Ok(id) if id == staking_contract::events::relay_round_initialized().id)
            {
                match staking_contract::events::relay_round_initialized()
                    .decode_input(body)
                    .convert()
                    .and_then(|tokens| tokens.unpack().map_err(anyhow::Error::from))
                {
                    Ok(parsed) => event = Some(parsed),
                    Err(e) => {
                        log::error!("Failed to parse staking event: {:?}", e);
                    }
                };
            }
        })?;

        log::info!("Got transaction on staking: {:?}", event);

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
    events_tx: ConnectorEventsTx,
}

impl TransactionsSubscription for ConnectorsObserver {
    fn handle_transaction(&self, ctx: TxContext<'_>) -> Result<()> {
        // Parse all outgoing messages and search proper event
        let mut event = None;
        iterate_transaction_events(&ctx, |body| match nekoton_abi::read_function_id(&body) {
            Ok(id) if id == connector_contract::events::enabled().id => {
                event = Some(ConnectorEvent::Enable);
            }
            Ok(id) if id == connector_contract::events::disabled().id => {
                event = Some(ConnectorEvent::Disable);
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
            self.events_tx.send((*ctx.account, event)).ok();
        }

        // Done
        Ok(())
    }
}

/// Generic listener for event configuration transactions
///
/// **Registered for each active TON event configuration account**
struct EventConfigurationsObserver<T> {
    events_tx: EventConfigurationEventsTx<T>,
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
                    .convert()
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

#[derive(Debug)]
enum EventConfigurationEvent<T> {
    Update {
        basic_configuration: BasicConfiguration,
        network_configuration: T,
    },
}

/// Helper trait to reduce boilerplate for getting accounts from shards state
trait ShardAccountsMapExt {
    /// Looks for a suitable shard and tries to extract information about the contract from it
    fn find_account(&self, account: &UInt256) -> Result<Option<ExistingContract>>;
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
                .and_then(|account| ExistingContract::from_shard_account_opt(&account))?
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
}

fn parse_incoming_internal_message<F>(ctx: &TxContext<'_>, mut f: F)
where
    F: FnMut(ton_types::SliceData),
{
    // Read incoming message
    let message = match ctx
        .transaction
        .in_msg
        .as_ref()
        .map(|message| message.read_struct())
    {
        Some(Ok(message)) => message,
        _ => return,
    };

    // Allow only internal messages
    if !matches!(message.header(), ton_block::CommonMsgInfo::IntMsgInfo(_)) {
        return;
    }

    // Handle body if it exists
    if let Some(body) = message.body() {
        f(body);
    }
}

fn iterate_transaction_events<F>(ctx: &TxContext<'_>, mut f: F) -> Result<()>
where
    F: FnMut(ton_types::SliceData),
{
    ctx.transaction
        .out_msgs
        .iterate(|ton_block::InRefValue(message)| {
            // Skip all messages except external outgoing
            if !matches!(message.header(), ton_block::CommonMsgInfo::ExtOutMsgInfo(_)) {
                return Ok(true);
            }

            // Handle body if it exists
            if let Some(body) = message.body() {
                f(body);
            }

            // Process all messages
            Ok(true)
        })
        .convert()?;
    Ok(())
}

type EventConfigurationEventsTx<T> = mpsc::UnboundedSender<(UInt256, EventConfigurationEvent<T>)>;
type ConnectorEventsTx = mpsc::UnboundedSender<(UInt256, ConnectorEvent)>;

type BridgeEventsTx = mpsc::UnboundedSender<ConnectorDeployedEvent>;
type BridgeEventsRx = mpsc::UnboundedReceiver<ConnectorDeployedEvent>;

type StakingEventsTx = mpsc::UnboundedSender<RelayRoundInitializedEvent>;
type StakingEventsRx = mpsc::UnboundedReceiver<RelayRoundInitializedEvent>;

type ShardAccountsMap = FxHashMap<ton_block::ShardIdent, ton_block::ShardAccounts>;
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
    #[error("Invalid contract address")]
    InvalidContractAddress,
    #[error("Already initialized")]
    AlreadyInitialized,
    #[error("Invalid event configuration")]
    InvalidEventConfiguration,
}
