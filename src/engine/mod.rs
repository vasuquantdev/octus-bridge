use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Result};
use nekoton_abi::{BuildTokenValue, FunctionBuilder, IntoUnpacker, TokenValueExt, UnpackFirst};
use nekoton_utils::NoFailure;
use parking_lot::RwLock;
use ton_block::{HashmapAugType, Serializable};
use ton_types::UInt256;

use self::ton_subscriber::*;
use crate::config::*;
use crate::utils::*;

mod ton_subscriber;

pub struct Engine {
    bridge_account: UInt256,
    ton_engine: Arc<ton_indexer::Engine>,
    ton_subscriber: Arc<TonSubscriber>,
    connectors_observer: Arc<ConnectorsObserver>,
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
        }))
    }

    pub async fn start(&self) -> Result<()> {
        self.ton_engine.start().await?;
        self.ton_subscriber.start().await?;

        self.get_all_configurations().await?;

        // TODO: start eth indexer

        Ok(())
    }

    async fn get_all_configurations(&self) -> Result<()> {
        let shard_accounts = self.get_all_shard_accounts().await?;

        let contract = shard_accounts
            .find_account(&self.bridge_account)?
            .ok_or(EngineError::BridgeAccountNotFound)?;
        let bridge = BridgeContract(&contract);

        let mut connectors = Vec::new();
        let mut active_configurations = Vec::new();

        for i in 0.. {
            let connector_address = bridge.derive_connector_address(i)?;

            let contract = match shard_accounts.find_account(&connector_address)? {
                Some(contract) => contract,
                None => break,
            };

            let details = ConnectorContract(&contract).get_details()?;
            log::info!("Found configuration connector {}: {:?}", i, details);

            if details.enabled {
                active_configurations.push(details.event_configuration);
            }
            connectors.push(details);
        }

        *self.connectors_observer.connectors.write() = connectors.clone();
        self.ton_subscriber.add_transactions_subscription(
            connectors.into_iter().map(|item| item.event_configuration),
            &self.connectors_observer,
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
    connectors: RwLock<Vec<ConnectorDetails>>,
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

struct BridgeContract<'a>(&'a ExistingContract);

impl BridgeContract<'_> {
    fn derive_connector_address(&self, id: u64) -> Result<UInt256> {
        let function = FunctionBuilder::new("deriveConnectorAddress")
            .default_headers()
            .in_arg("id", ton_abi::ParamType::Uint(128))
            .out_arg("connector", ton_abi::ParamType::Address);

        let address: ton_block::MsgAddrStd = self
            .0
            .run_local(&function.build(), &[(id as u128).token_value().named("id")])?
            .unpack_first()?;
        Ok(UInt256::from_be_bytes(&address.address.get_bytestring(0)))
    }
}

struct ConnectorContract<'a>(&'a ExistingContract);

impl ConnectorContract<'_> {
    fn get_details(&self) -> Result<ConnectorDetails> {
        let function = FunctionBuilder::new("getDetails")
            .header("time", ton_abi::ParamType::Time)
            .out_arg("id", ton_abi::ParamType::Uint(128))
            .out_arg("eventConfiguration", ton_abi::ParamType::Address)
            .out_arg("enabled", ton_abi::ParamType::Bool);

        let mut result = self.0.run_local(&function.build(), &[])?.into_unpacker();
        let _id: u128 = result.unpack_next()?;
        let event_configuration: ton_block::MsgAddrStd = result.unpack_next()?;
        let enabled: bool = result.unpack_next()?;

        Ok(ConnectorDetails {
            event_configuration: UInt256::from_be_bytes(
                &event_configuration.address.get_bytestring(0),
            ),
            enabled,
        })
    }
}

#[derive(Debug, Copy, Clone)]
struct ConnectorDetails {
    event_configuration: UInt256,
    enabled: bool,
}

trait ShardAccountsMapExt {
    fn find_account(&self, account: &UInt256) -> Result<Option<ExistingContract>>;
}

impl ShardAccountsMapExt for ShardAccountsMap {
    fn find_account(&self, account: &UInt256) -> Result<Option<ExistingContract>> {
        match self
            .iter()
            .find(|(shard_ident, _)| contains_account(shard_ident, account))
        {
            Some((_, shard)) => match shard
                .get(&account)
                .convert()
                .and_then(|account| ExistingContract::from_shard_account(&account))?
            {
                Some(contract) => Ok(Some(contract)),
                None => Ok(None),
            },
            None => Err(EngineError::InvalidContractAddress).context("No suitable shard found"),
        }
    }
}

type ShardAccountsMap = HashMap<ton_block::ShardIdent, ton_block::ShardAccounts>;

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
}
