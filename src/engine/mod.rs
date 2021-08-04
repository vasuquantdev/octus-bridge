use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::{Context, Result};
use nekoton_abi::{BuildTokenValue, FunctionBuilder, IntoUnpacker, TokenValueExt, UnpackFirst};
use nekoton_utils::NoFailure;
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
        let mut bridge_shard = None;

        let shard_blocks = self.ton_subscriber.wait_shards().await?;

        let mut shard_accounts = BTreeMap::new();
        for (shard_ident, block_id) in shard_blocks {
            log::info!(
                "Loading state: {:016x}, {}",
                shard_ident.shard_prefix_with_tag(),
                block_id.seq_no
            );
            let shard = self.ton_engine.wait_state(&block_id, None, false).await?;
            log::info!("Loaded state");
            let accounts = shard.state().read_accounts().convert()?;
            log::info!("Loaded accounts");

            if contains_account(&shard_ident, &self.bridge_account) {
                bridge_shard = Some(shard_ident);
            }

            shard_accounts.insert(shard_ident, accounts);
        }

        log::info!(
            "---\nLoaded all shards. Bridge shard: {:016x}",
            if let Some(shard) = bridge_shard {
                shard.shard_prefix_with_tag()
            } else {
                0
            }
        );

        let contract = match bridge_shard.and_then(|shard| shard_accounts.get(&shard)) {
            Some(shard) => match shard
                .get(&self.bridge_account)
                .convert()
                .and_then(|account| ExistingContract::from_shard_account(&account))?
            {
                Some(contract) => contract,
                None => return Err(EngineError::BridgeAccountNotFound.into()),
            },
            None => {
                return Err(EngineError::BridgeAccountNotFound).context("Bridge shard not found")
            }
        };

        log::info!("---\nLoaded bridge account");

        let bridge = BridgeContract(&contract);

        log::info!("GOT BRIDGE CONTRACT");

        for i in 0..u64::MAX {
            log::info!("GETTING INFO FOR CONNECTOR {}...", i);
            let connector_address = bridge.derive_connector_address(i)?;

            let contract = match shard_accounts
                .iter()
                .find(|(shard_ident, _)| contains_account(shard_ident, &connector_address))
            {
                Some((_, shard)) => match shard
                    .get(&connector_address)
                    .convert()
                    .and_then(|account| ExistingContract::from_shard_account(&account))?
                {
                    Some(contract) => contract,
                    None => break,
                },
                None => {
                    return Err(EngineError::InvalidConnectorAddress)
                        .context("No suitable shard found")
                }
            };

            log::info!("EXTRACTING INFO FOR CONNECTOR {}...", i);
            let details = ConnectorContract(&contract).get_details()?;
            log::info!("FOUND CONFIGURATION CONNECTOR {}: {:?}", i, details);
        }

        // TODO

        Ok(())
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

#[derive(Debug)]
struct ConnectorDetails {
    event_configuration: UInt256,
    enabled: bool,
}

#[derive(thiserror::Error, Debug)]
enum EngineError {
    #[error("External ton message expected")]
    ExternalTonMessageExpected,
    #[error("Bridge account not found")]
    BridgeAccountNotFound,
    #[error("Invalid connector address")]
    InvalidConnectorAddress,
}
