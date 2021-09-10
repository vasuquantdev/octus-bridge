use std::sync::Arc;

use anyhow::Result;
use parking_lot::Mutex;
use tiny_adnl::utils::*;
use ton_block::Serializable;

use self::bridge::*;
use self::eth_subscriber::*;
use self::keystore::*;
use self::staking::*;
use self::ton_contracts::*;
use self::ton_subscriber::*;
use crate::config::*;
use crate::utils::*;

mod bridge;
mod eth_subscriber;
mod keystore;
mod staking;
mod ton_contracts;
mod ton_subscriber;

pub struct Engine {
    context: Arc<EngineContext>,
    bridge: Mutex<Option<Arc<Bridge>>>,
    staking: Mutex<Option<Arc<Staking>>>,
}

impl Engine {
    pub async fn new(
        config: AppConfig,
        global_config: ton_indexer::GlobalConfig,
    ) -> Result<Arc<Self>> {
        let context = EngineContext::new(config, global_config).await?;

        Ok(Arc::new(Self {
            context,
            bridge: Mutex::new(None),
            staking: Mutex::new(None),
        }))
    }

    pub async fn start(&self) -> Result<()> {
        // Print ETH address and TON public key
        log::warn!(
            "Using ETH address: {}",
            EthAddressWrapper(self.context.keystore.eth.address())
        );
        log::warn!(
            "Using TON public key: {}",
            self.context.keystore.ton.public_key().to_hex_string()
        );

        // Sync node and subscribers
        self.context.start().await?;

        // Fetch bridge configuration
        let bridge_account = only_account_hash(&self.context.settings.bridge_address);

        let bridge_contract = match self
            .context
            .ton_subscriber
            .get_contract_state(bridge_account)
            .await?
        {
            Some(contract) => contract,
            None => return Err(EngineError::BridgeAccountNotFound.into()),
        };

        let bridge_details = BridgeContract(&bridge_contract).get_details()?;

        // Initialize bridge
        let bridge = Bridge::new(self.context.clone(), bridge_account).await?;
        *self.bridge.lock() = Some(bridge);

        // Initialize staking
        let staking = Staking::new(self.context.clone(), bridge_details.staking).await?;
        *self.staking.lock() = Some(staking);

        // Done
        Ok(())
    }
}

pub struct EngineContext {
    pub settings: RelayConfig,
    pub keystore: Arc<KeyStore>,
    pub messages_queue: Arc<PendingMessagesQueue>,
    pub ton_subscriber: Arc<TonSubscriber>,
    pub ton_engine: Arc<ton_indexer::Engine>,
    pub eth_subscribers: Arc<EthSubscriberRegistry>,
}

impl Drop for EngineContext {
    fn drop(&mut self) {
        self.ton_engine.shutdown();
    }
}

impl EngineContext {
    async fn new(config: AppConfig, global_config: ton_indexer::GlobalConfig) -> Result<Arc<Self>> {
        let settings = config.relay_settings;

        let keystore = KeyStore::new(&settings.keys_path, config.master_password)?;

        let messages_queue = PendingMessagesQueue::new(16);

        let ton_subscriber = TonSubscriber::new(messages_queue.clone());
        let ton_engine = ton_indexer::Engine::new(
            config.node_settings,
            global_config,
            vec![ton_subscriber.clone() as Arc<dyn ton_indexer::Subscriber>],
        )
        .await?;

        let eth_subscribers = EthSubscriberRegistry::new(settings.networks.clone()).await?;

        Ok(Arc::new(Self {
            settings,
            keystore,
            messages_queue,
            ton_subscriber,
            ton_engine,
            eth_subscribers,
        }))
    }

    async fn start(&self) -> Result<()> {
        self.ton_engine.start().await?;
        self.ton_subscriber.start().await?;
        Ok(())
    }

    pub async fn get_all_shard_accounts(&self) -> Result<ShardAccountsMap> {
        let shard_blocks = self.ton_subscriber.wait_shards().await?.block_ids;

        let mut shard_accounts =
            FxHashMap::with_capacity_and_hasher(shard_blocks.len(), Default::default());
        for (shard_ident, block_id) in shard_blocks {
            let shard = self.ton_engine.wait_state(&block_id, None, false).await?;
            let accounts = shard.state().read_accounts()?;
            shard_accounts.insert(shard_ident, accounts);
        }

        Ok(shard_accounts)
    }

    pub async fn send_ton_message(
        &self,
        account: &ton_types::UInt256,
        message: &ton_block::Message,
        expire_at: u32,
    ) -> Result<MessageStatus> {
        let to = match message.header() {
            ton_block::CommonMsgInfo::ExtInMsgInfo(header) => {
                ton_block::AccountIdPrefixFull::prefix(&header.dst)?
            }
            _ => return Err(EngineError::ExternalTonMessageExpected.into()),
        };

        let cells = message.write_to_new_cell()?.into();
        let serialized = ton_types::serialize_toc(&cells)?;

        let rx = self
            .messages_queue
            .add_message(*account, cells.repr_hash(), expire_at)?;

        self.ton_engine
            .broadcast_external_message(&to, &serialized)
            .await?;

        let status = rx.await?;
        Ok(status)
    }

    fn deliver_message<T>(
        self: &Arc<Self>,
        observer: Arc<AccountObserver<T>>,
        unsigned_message: UnsignedMessage,
    ) where
        T: Send + 'static,
    {
        let context = Arc::downgrade(self);

        tokio::spawn(async move {
            loop {
                // Observer must be alive
                let context = match context.upgrade() {
                    Some(bridge) => bridge,
                    _ => break,
                };

                let message = match context.keystore.ton.sign(&unsigned_message) {
                    Ok(message) => message,
                    Err(e) => {
                        log::error!("Failed to send message: {:?}", e);
                        break;
                    }
                };

                match context
                    .send_ton_message(&message.account, &message.message, message.expire_at)
                    .await
                {
                    Ok(MessageStatus::Expired) => {
                        log::info!("Message to account {:x} expired", message.account);
                    }
                    Ok(MessageStatus::Delivered) => {
                        log::info!("Successfully sent message to account {:x}", message.account);
                        break;
                    }
                    Err(e) => {
                        log::warn!("Failed to send message: {:?}", e);
                        break;
                    }
                }
            }

            // Must be dropped here to ensure observer will live enough
            drop(observer);
        });
    }
}

#[derive(thiserror::Error, Debug)]
enum EngineError {
    #[error("External ton message expected")]
    ExternalTonMessageExpected,
    #[error("Bridge account not found")]
    BridgeAccountNotFound,
}
