use std::collections::hash_map::{self, HashMap};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use anyhow::{Context, Result};
use dashmap::DashMap;
use nekoton_utils::*;
use parking_lot::RwLock;
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
    ton_engine: Arc<ton_indexer::Engine>,
    ton_subscriber: Arc<TonSubscriber>,
    connectors_observer: Arc<ConnectorsObserver>,
    eth_configurations_observer: Arc<EthEventConfigurationsObserver>,
    ton_configurations_observer: Arc<TonEventConfigurationsObserver>,
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

        Ok(Arc::new(Self {
            bridge_account,
            ton_engine,
            ton_subscriber,
            connectors_observer: Arc::new(Default::default()),
            eth_configurations_observer: Arc::new(Default::default()),
            ton_configurations_observer: Arc::new(Default::default()),
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

#[derive(Default)]
struct ConnectorsObserver {
    connectors: ConnectorsMap,
}

impl ConnectorsObserver {
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
        block_info: &ton_block::BlockInfo,
        account: &UInt256,
        transaction_hash: &UInt256,
        transaction: &ton_block::Transaction,
    ) -> Result<()> {
        log::info!("Got transaction on connector {}", account.to_hex_string());

        // TODO: parse connector events

        Ok(())
    }
}

#[derive(Debug, Copy, Clone)]
struct ConnectorState {
    id: u64,
    details: ConnectorDetails,
    event_type: ConnectorConfigurationType,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
enum ConnectorConfigurationType {
    Unknown,
    Known(EventType),
}

#[derive(Default)]
struct EthEventConfigurationsObserver {
    configurations: EthEventConfigurationsMap,
}

impl EthEventConfigurationsObserver {
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

#[derive(Default)]
struct TonEventConfigurationsObserver {
    configurations: TonEventConfigurationsMap,
}

impl TonEventConfigurationsObserver {
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

struct EventConfigurationsProcessingContext<'a> {
    event_code_hashes: &'a mut EventCodeHashesMap,
    eth_event_configurations: &'a EthEventConfigurationsMap,
    ton_event_configurations: &'a TonEventConfigurationsMap,
}

trait ShardAccountsMapExt {
    fn find_account(&self, account: &UInt256) -> Result<Option<ExistingContract>>;
    fn process_event_configuration(
        &self,
        account: &UInt256,
        ctx: &mut EventConfigurationsProcessingContext<'_>,
    ) -> Result<Option<EventType>>;
}

impl ShardAccountsMapExt for ShardAccountsMap {
    fn find_account(&self, account: &UInt256) -> Result<Option<ExistingContract>> {
        match self
            .iter()
            .find(|(shard_ident, _)| contains_account(shard_ident, account))
        {
            Some((_, shard)) => match shard
                .get(account)
                .convert()
                .and_then(|account| ExistingContract::from_shard_account(&account))?
            {
                Some(contract) => Ok(Some(contract)),
                None => Ok(None),
            },
            None => Err(EngineError::InvalidContractAddress).context("No suitable shard found"),
        }
    }

    fn process_event_configuration(
        &self,
        account: &UInt256,
        ctx: &mut EventConfigurationsProcessingContext<'_>,
    ) -> Result<Option<EventType>> {
        let contract = match self.find_account(account)? {
            Some(contract) => contract,
            None => {
                log::warn!(
                    "Connected configuration was not found: {}",
                    account.to_hex_string()
                );
                return Ok(None);
            }
        };

        let event_type = EventConfigurationBaseContract(&contract).get_type()?;

        let mut fill_event_code_hash = |code: &ton_types::Cell| {
            match ctx.event_code_hashes.entry(code.repr_hash()) {
                hash_map::Entry::Vacant(entry) => {
                    entry.insert(event_type);
                }
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
            EventType::Eth => {
                let details = EthEventConfigurationContract(&contract).get_details()?;
                log::info!("Ethereum event configuration details: {:?}", details);

                fill_event_code_hash(&details.basic_configuration.event_code)?;
                ctx.eth_event_configurations.insert(*account, details);
            }
            EventType::Ton => {
                let details = TonEventConfigurationContract(&contract).get_details()?;
                log::info!("TON event configuration details: {:?}", details);

                fill_event_code_hash(&details.basic_configuration.event_code)?;
                ctx.ton_event_configurations.insert(*account, details);
            }
        };

        Ok(Some(event_type))
    }
}

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
