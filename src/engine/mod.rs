use std::sync::Arc;

use anyhow::{Context, Result};
use parking_lot::Mutex;
use pkey_mprotect::*;
use pomfrit::formatter::*;
use tiny_adnl::utils::*;
use tokio::sync::mpsc;
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
    metrics_exporter: Arc<pomfrit::MetricsExporter>,
    context: Arc<EngineContext>,
    bridge: Mutex<Option<Arc<Bridge>>>,
    staking: Mutex<Option<Arc<Staking>>>,
}

impl Engine {
    pub async fn new(
        config: AppConfig,
        global_config: ton_indexer::GlobalConfig,
        protection_keys: Arc<ProtectionKeys>,
        shutdown_requests_tx: ShutdownRequestsTx,
    ) -> Result<Arc<Self>> {
        let (metrics_exporter, metrics_writer) =
            pomfrit::create_exporter(config.metrics_settings.clone()).await?;

        let context =
            EngineContext::new(config, global_config, protection_keys, shutdown_requests_tx)
                .await?;

        let engine = Arc::new(Self {
            metrics_exporter,
            context,
            bridge: Mutex::new(None),
            staking: Mutex::new(None),
        });

        metrics_writer.spawn({
            let engine = Arc::downgrade(&engine);
            move |buffer| {
                let engine = match engine.upgrade() {
                    Some(engine) => engine,
                    None => return,
                };

                buffer
                    .write(LabeledEthSubscriberMetrics(&engine.context))
                    .write(LabeledTonSubscriberMetrics(&engine.context));

                if let Some(bridge) = &*engine.bridge.lock() {
                    buffer.write(LabeledBridgeMetrics {
                        context: &engine.context,
                        bridge,
                    });
                };

                if let Some(staking) = &*engine.staking.lock() {
                    buffer.write(LabeledStakingMetrics {
                        context: &engine.context,
                        staking,
                    });
                };
            }
        });

        Ok(engine)
    }

    pub async fn start(self: &Arc<Self>) -> Result<()> {
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

        let bridge_details = BridgeContract(&bridge_contract)
            .get_details()
            .context("Failed to get bridge details")?;

        // Initialize bridge
        let bridge = Bridge::new(self.context.clone(), bridge_account)
            .await
            .context("Failed to init bridge")?;
        *self.bridge.lock() = Some(bridge);

        // Initialize staking
        let staking = Staking::new(self.context.clone(), bridge_details.staking)
            .await
            .context("Failed to init staking")?;
        *self.staking.lock() = Some(staking);

        self.context.eth_subscribers.start();

        // Done
        Ok(())
    }

    pub async fn update_metrics_config(&self, config: Option<pomfrit::Config>) -> Result<()> {
        self.metrics_exporter
            .reload(config)
            .await
            .context("Failed to update metrics exporter config")
    }
}

pub struct EngineContext {
    pub shutdown_requests_tx: ShutdownRequestsTx,
    pub staker_account_str: String,
    pub staker_account: ton_types::UInt256,
    pub settings: BridgeConfig,
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
    async fn new(
        config: AppConfig,
        global_config: ton_indexer::GlobalConfig,
        protection_keys: Arc<ProtectionKeys>,
        shutdown_requests_tx: ShutdownRequestsTx,
    ) -> Result<Arc<Self>> {
        let staker_account =
            ton_types::UInt256::from_be_bytes(&config.staker_address.address().get_bytestring(0));
        let staker_account_str = config.staker_address.to_string();
        let settings = config.bridge_settings;

        let keystore = KeyStore::new(&settings.keys_path, config.master_password, protection_keys)
            .context("Failed to create keystore")?;

        let messages_queue = PendingMessagesQueue::new(16);

        let ton_subscriber = TonSubscriber::new(messages_queue.clone());
        let ton_engine = ton_indexer::Engine::new(
            config
                .node_settings
                .build_indexer_config()
                .await
                .context("Failed to build node config")?,
            global_config,
            vec![ton_subscriber.clone() as Arc<dyn ton_indexer::Subscriber>],
        )
        .await
        .context("Failed to start TON node")?;

        let eth_subscribers = EthSubscriberRegistry::new(settings.networks.clone())
            .await
            .context("Failed to create EVM networks registry")?;

        Ok(Arc::new(Self {
            shutdown_requests_tx,
            staker_account_str,
            staker_account,
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
        let shard_blocks = self.ton_subscriber.wait_shards(None).await?.block_ids;

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

    async fn deliver_message<T, F>(
        self: &Arc<Self>,
        observer: Arc<AccountObserver<T>>,
        unsigned_message: UnsignedMessage,
        mut condition: F,
    ) -> Result<()>
    where
        T: Send + 'static,
        F: FnMut() -> bool + 'static,
    {
        // Check if message should be sent
        while condition() {
            // Prepare and send the message
            // NOTE: it must be signed every time before sending because it uses current
            // timestamp in headers. It will not work outside this loop
            let message = self.keystore.ton.sign(&unsigned_message)?;

            match self
                .send_ton_message(&message.account, &message.message, message.expire_at)
                .await?
            {
                MessageStatus::Expired => {
                    // Do nothing on expire and just retry
                    log::warn!("Message to account {:x} expired", message.account);
                }
                MessageStatus::Delivered => {
                    log::info!("Successfully sent message to account {:x}", message.account);
                    break;
                }
            }
        }

        // Make sure that observer is living enough. Messages will not be found
        // if it is deleted too early
        drop(observer);
        Ok(())
    }
}

struct LabeledBridgeMetrics<'a> {
    context: &'a EngineContext,
    bridge: &'a Bridge,
}

impl std::fmt::Display for LabeledBridgeMetrics<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let metrics = self.bridge.metrics();

        f.begin_metric("bridge_pending_eth_event_count")
            .label(LABEL_STAKER, &self.context.staker_account_str)
            .value(metrics.pending_eth_event_count)?;

        f.begin_metric("bridge_pending_ton_event_count")
            .label(LABEL_STAKER, &self.context.staker_account_str)
            .value(metrics.pending_ton_event_count)?;

        f.begin_metric("bridge_total_active_eth_event_configurations")
            .label(LABEL_STAKER, &self.context.staker_account_str)
            .value(metrics.total_active_eth_event_configurations)?;

        f.begin_metric("bridge_total_active_ton_event_configurations")
            .label(LABEL_STAKER, &self.context.staker_account_str)
            .value(metrics.total_active_ton_event_configurations)?;

        Ok(())
    }
}

struct LabeledStakingMetrics<'a> {
    context: &'a EngineContext,
    staking: &'a Staking,
}

impl std::fmt::Display for LabeledStakingMetrics<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let metrics = self.staking.metrics();

        f.begin_metric("staking_user_data_tokens_balance")
            .label(LABEL_STAKER, &self.context.staker_account_str)
            .label(LABEL_ROUND_NUM, metrics.current_relay_round)
            .value(metrics.user_data_tokens_balance)?;

        f.begin_metric("staking_current_relay_round")
            .label(LABEL_STAKER, &self.context.staker_account_str)
            .value(metrics.current_relay_round)?;

        let status = match metrics.elections_state {
            ElectionsState::NotStarted { start_time } => {
                f.begin_metric("staking_elections_start_time")
                    .label(LABEL_STAKER, &self.context.staker_account_str)
                    .label(LABEL_ROUND_NUM, metrics.current_relay_round)
                    .value(start_time)?;
                0
            }
            ElectionsState::Started {
                start_time,
                end_time,
            } => {
                f.begin_metric("staking_elections_start_time")
                    .label(LABEL_STAKER, &self.context.staker_account_str)
                    .label(LABEL_ROUND_NUM, metrics.current_relay_round)
                    .value(start_time)?;
                f.begin_metric("staking_elections_end_time")
                    .label(LABEL_STAKER, &self.context.staker_account_str)
                    .label(LABEL_ROUND_NUM, metrics.current_relay_round)
                    .value(end_time)?;
                1
            }
            ElectionsState::Finished => 2,
        };

        f.begin_metric("staking_elections_status")
            .label(LABEL_STAKER, &self.context.staker_account_str)
            .label(LABEL_ROUND_NUM, &metrics.current_relay_round)
            .value(status)?;

        f.begin_metric("staking_ignore_elections")
            .label(LABEL_STAKER, &self.context.staker_account_str)
            .label(LABEL_ROUND_NUM, &metrics.current_relay_round)
            .value(metrics.ignore_elections as u8)?;

        if let Some(participates_in_round) = metrics.participates_in_round {
            f.begin_metric("staking_participates_in_round")
                .label(LABEL_STAKER, &self.context.staker_account_str)
                .label(LABEL_ROUND_NUM, &metrics.current_relay_round)
                .value(participates_in_round as u8)?;
        }

        if let Some(elected) = metrics.elected {
            f.begin_metric("staking_elected")
                .label(LABEL_STAKER, &self.context.staker_account_str)
                .label(LABEL_ROUND_NUM, metrics.current_relay_round)
                .value(elected as u8)?;
        }

        Ok(())
    }
}

struct LabeledTonSubscriberMetrics<'a>(&'a EngineContext);

impl std::fmt::Display for LabeledTonSubscriberMetrics<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use std::sync::atomic::Ordering;

        let metrics = self.0.ton_subscriber.metrics();
        let indexer_metrics = self.0.ton_engine.metrics();

        f.begin_metric("ton_subscriber_ready")
            .label(LABEL_STAKER, &self.0.staker_account_str)
            .value(metrics.ready as u8)?;

        if metrics.current_utime > 0 {
            let mc_time_diff = indexer_metrics.mc_time_diff.load(Ordering::Acquire);
            let shard_client_time_diff = indexer_metrics
                .shard_client_time_diff
                .load(Ordering::Acquire);

            let last_mc_block_seqno = indexer_metrics.last_mc_block_seqno.load(Ordering::Acquire);
            let last_shard_client_mc_block_seqno = indexer_metrics
                .last_shard_client_mc_block_seqno
                .load(Ordering::Acquire);

            f.begin_metric("ton_subscriber_current_utime")
                .label(LABEL_STAKER, &self.0.staker_account_str)
                .value(metrics.current_utime)?;

            f.begin_metric("ton_subscriber_time_diff")
                .label(LABEL_STAKER, &self.0.staker_account_str)
                .value(mc_time_diff)?;

            f.begin_metric("ton_subscriber_shard_client_time_diff")
                .label(LABEL_STAKER, &self.0.staker_account_str)
                .value(shard_client_time_diff)?;

            f.begin_metric("ton_subscriber_mc_block_seqno")
                .label(LABEL_STAKER, &self.0.staker_account_str)
                .value(last_mc_block_seqno)?;

            f.begin_metric("ton_subscriber_shard_client_mc_block_seqno")
                .label(LABEL_STAKER, &self.0.staker_account_str)
                .value(last_shard_client_mc_block_seqno)?;
        }

        f.begin_metric("ton_subscriber_pending_message_count")
            .label(LABEL_STAKER, &self.0.staker_account_str)
            .value(metrics.pending_message_count)?;

        Ok(())
    }
}

struct LabeledEthSubscriberMetrics<'a>(&'a EngineContext);

impl std::fmt::Display for LabeledEthSubscriberMetrics<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for subscriber in self.0.eth_subscribers.subscribers() {
            let chain_id = subscriber.chain_id_str();
            let metrics = subscriber.metrics();

            f.begin_metric("eth_subscriber_last_processed_block")
                .label(LABEL_STAKER, &self.0.staker_account_str)
                .label(LABEL_CHAIN_ID, &chain_id)
                .value(metrics.last_processed_block)?;

            f.begin_metric("eth_subscriber_pending_confirmation_count")
                .label(LABEL_STAKER, &self.0.staker_account_str)
                .label(LABEL_CHAIN_ID, &chain_id)
                .value(metrics.pending_confirmation_count)?;
        }
        Ok(())
    }
}

const LABEL_STAKER: &str = "staker";
const LABEL_CHAIN_ID: &str = "chain_id";
const LABEL_ROUND_NUM: &str = "round_num";

pub type ShutdownRequestsRx = mpsc::UnboundedReceiver<()>;
pub type ShutdownRequestsTx = mpsc::UnboundedSender<()>;

#[derive(thiserror::Error, Debug)]
enum EngineError {
    #[error("External ton message expected")]
    ExternalTonMessageExpected,
    #[error("Bridge account not found")]
    BridgeAccountNotFound,
}
