use std::collections::hash_map;
use std::ops::Deref;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Weak};

use anyhow::{Context, Result};
use parking_lot::Mutex;
use rustc_hash::FxHashMap;
use tokio::sync::{mpsc, oneshot, watch, Notify};
use ton_block::{BinTreeType, Deserializable, HashmapAugType};
use ton_indexer::{BriefBlockMeta, EngineStatus, ProcessBlockContext};
use ton_types::{HashmapType, UInt256};

use crate::utils::*;

pub struct TonSubscriber {
    ready: AtomicBool,
    ready_signal: Notify,
    current_utime: AtomicU32,
    signature_id: SignatureId,
    state_subscriptions: Mutex<FxHashMap<UInt256, StateSubscription>>,
    mc_block_awaiters: Mutex<FxHashMap<usize, Box<dyn BlockAwaiter>>>,
    messages_queue: Arc<PendingMessagesQueue>,
}

impl TonSubscriber {
    pub fn new(messages_queue: Arc<PendingMessagesQueue>) -> Arc<Self> {
        Arc::new(Self {
            ready: AtomicBool::new(false),
            ready_signal: Notify::new(),
            current_utime: AtomicU32::new(0),
            signature_id: SignatureId::default(),
            state_subscriptions: Mutex::new(FxHashMap::with_capacity_and_hasher(
                128,
                Default::default(),
            )),
            mc_block_awaiters: Mutex::new(FxHashMap::with_capacity_and_hasher(
                4,
                Default::default(),
            )),
            messages_queue,
        })
    }

    pub fn metrics(&self) -> TonSubscriberMetrics {
        TonSubscriberMetrics {
            ready: self.ready.load(Ordering::Acquire),
            current_utime: self.current_utime(),
            signature_id: self.signature_id(),
            pending_message_count: self.messages_queue.len(),
        }
    }

    pub async fn start(self: &Arc<Self>, engine: &ton_indexer::Engine) -> Result<()> {
        let last_key_block = engine.load_last_key_block().await?;
        self.update_signature_id(last_key_block.block())?;

        self.wait_sync().await;

        let current_utime = chrono::Utc::now().timestamp() as u32;
        tracing::info!(current_utime, "waiting for the masterchain block");
        self.wait_shards(Some(current_utime)).await?;
        tracing::info!(current_utime, "finished waiting for the masterchain block");

        Ok(())
    }

    pub fn current_utime(&self) -> u32 {
        self.current_utime.load(Ordering::Acquire)
    }

    pub fn signature_id(&self) -> Option<i32> {
        self.signature_id.load()
    }

    pub async fn wait_shards(&self, since: Option<u32>) -> Result<LatestShardBlocks> {
        struct Handler {
            since: Option<u32>,
            tx: Option<oneshot::Sender<Result<LatestShardBlocks>>>,
        }

        impl BlockAwaiter for Handler {
            fn handle_block(
                &mut self,
                block: &ton_block::Block,
                block_info: &ton_block::BlockInfo,
            ) -> Result<BlockAwaiterAction> {
                if matches!(self.since, Some(since) if block_info.gen_utime().as_u32() < since) {
                    return Ok(BlockAwaiterAction::Retain);
                }

                if let Some(tx) = self.tx.take() {
                    let _ = tx.send(extract_shards(block, block_info));
                }
                Ok(BlockAwaiterAction::Remove)
            }
        }

        fn extract_shards(
            block: &ton_block::Block,
            block_info: &ton_block::BlockInfo,
        ) -> Result<LatestShardBlocks> {
            let current_utime = block_info.gen_utime().as_u32();
            let extra = block.extra.read_struct()?;
            let custom = match extra.read_custom()? {
                Some(custom) => custom,
                None => {
                    return Ok(LatestShardBlocks {
                        current_utime,
                        block_ids: Default::default(),
                    })
                }
            };

            let mut block_ids = FxHashMap::with_capacity_and_hasher(16, Default::default());

            custom.shards().iterate_with_keys(
                |wc_id: i32, ton_block::InRefValue(shards_tree)| {
                    if wc_id == ton_block::MASTERCHAIN_ID {
                        return Ok(true);
                    }

                    shards_tree.iterate(|prefix, descr| {
                        let shard_id = ton_block::ShardIdent::with_prefix_slice(wc_id, prefix)?;

                        block_ids.insert(
                            shard_id,
                            ton_block::BlockIdExt::with_params(
                                shard_id,
                                descr.seq_no,
                                descr.root_hash,
                                descr.file_hash,
                            ),
                        );
                        Ok(true)
                    })
                },
            )?;

            Ok(LatestShardBlocks {
                current_utime,
                block_ids,
            })
        }

        let (tx, rx) = oneshot::channel();
        self.mc_block_awaiters.lock().insert(
            BLOCK_AWAITER_ID.fetch_add(1, Ordering::Relaxed),
            Box::new(Handler {
                since,
                tx: Some(tx),
            }),
        );
        rx.await?
    }

    pub fn add_transactions_subscription<I, T>(&self, accounts: I, subscription: &Arc<T>)
    where
        I: IntoIterator<Item = UInt256>,
        T: TransactionsSubscription + 'static,
    {
        let mut state_subscriptions = self.state_subscriptions.lock();

        let weak = Arc::downgrade(subscription) as Weak<dyn TransactionsSubscription>;

        for account in accounts {
            match state_subscriptions.entry(account) {
                hash_map::Entry::Vacant(entry) => {
                    let (state_tx, state_rx) = watch::channel(None);
                    entry.insert(StateSubscription {
                        state_tx,
                        state_rx,
                        transaction_subscriptions: vec![weak.clone()],
                    });
                }
                hash_map::Entry::Occupied(mut entry) => {
                    entry.get_mut().transaction_subscriptions.push(weak.clone());
                }
            };
        }
    }

    pub async fn get_contract_state(&self, account: UInt256) -> Result<Option<ExistingContract>> {
        let mut state_rx = match self.state_subscriptions.lock().entry(account) {
            hash_map::Entry::Vacant(entry) => {
                let (state_tx, state_rx) = watch::channel(None);
                entry
                    .insert(StateSubscription {
                        state_tx,
                        state_rx,
                        transaction_subscriptions: Vec::new(),
                    })
                    .state_rx
                    .clone()
            }
            hash_map::Entry::Occupied(entry) => entry.get().state_rx.clone(),
        };

        state_rx.changed().await?;
        let account = state_rx.borrow_and_update();
        ExistingContract::from_shard_account_opt(account.deref())
    }

    pub async fn wait_contract_state(&self, account: UInt256) -> Result<ExistingContract> {
        let mut state_rx = match self.state_subscriptions.lock().entry(account) {
            hash_map::Entry::Vacant(entry) => {
                let (state_tx, state_rx) = watch::channel(None);
                entry
                    .insert(StateSubscription {
                        state_tx,
                        state_rx,
                        transaction_subscriptions: Vec::new(),
                    })
                    .state_rx
                    .clone()
            }
            hash_map::Entry::Occupied(entry) => entry.get().state_rx.clone(),
        };

        loop {
            state_rx.changed().await?;

            let shard_account = match state_rx.borrow_and_update().deref() {
                Some(shard_account) => ExistingContract::from_shard_account(shard_account)?,
                None => continue,
            };

            match shard_account {
                Some(account) => match &account.account.storage.state {
                    ton_block::AccountState::AccountActive { .. } => {
                        return Ok(account);
                    }
                    ton_block::AccountState::AccountFrozen { .. } => {
                        return Err(TonSubscriberError::AccountIsFrozen.into())
                    }
                    ton_block::AccountState::AccountUninit => continue,
                },
                _ => continue,
            }
        }
    }

    fn handle_masterchain_block(
        &self,
        meta: BriefBlockMeta,
        block_id: &ton_block::BlockIdExt,
        block: &ton_block::Block,
    ) -> Result<()> {
        let gen_utime = meta.gen_utime();
        self.current_utime.store(gen_utime, Ordering::Release);

        let block_info = block.info.read_struct()?;
        if block_info.key_block() {
            self.update_signature_id(block)?;
        }

        if !self.ready.load(Ordering::Acquire) {
            return Ok(());
        }

        let mut mc_block_awaiters = self.mc_block_awaiters.lock();
        mc_block_awaiters.retain(
            |_, awaiter| match awaiter.handle_block(block, &block_info) {
                Ok(action) => action == BlockAwaiterAction::Retain,
                Err(e) => {
                    tracing::error!(%block_id, "failed to handle masterchain block: {e:?}");
                    true
                }
            },
        );

        Ok(())
    }

    fn handle_shard_block(
        &self,
        block_id: &ton_block::BlockIdExt,
        block: &ton_block::Block,
        shard_state: &ton_block::ShardStateUnsplit,
    ) -> Result<()> {
        if !self.ready.load(Ordering::Acquire) {
            return Ok(());
        }

        let block_info = block.info.read_struct()?;
        let extra = block.extra.read_struct()?;
        let account_blocks = extra.read_account_blocks()?;
        let shard_accounts = shard_state.read_accounts()?;

        {
            let mut subscriptions = self.state_subscriptions.lock();
            subscriptions.retain(|account, subscription| {
                let subscription_status = subscription.update_status();
                if subscription_status == StateSubscriptionStatus::Stopped {
                    return false;
                }

                if !contains_account(block_info.shard(), account) {
                    return true;
                }

                let mut keep = true;

                if subscription_status == StateSubscriptionStatus::Alive {
                    match shard_accounts.get(account) {
                        Ok(mut state) => {
                            let should_send = match &mut state {
                                // If account exists, all its cells must be preloaded
                                // to get rid of rocksdb access
                                Some(state) => {
                                    match state.account_cell().preload_with_depth_hint::<20>() {
                                        // Account is now fully in memory
                                        Ok(()) => true,
                                        // Failed to load all cells
                                        Err(e) => {
                                            tracing::error!(
                                                account = %DisplayAddr(account),
                                                "failed to preload account: {e:?}",
                                            );
                                            false
                                        }
                                    }
                                }
                                // Empty accounts can be sent without preloading
                                None => true,
                            };

                            if should_send && subscription.state_tx.send(state).is_err() {
                                tracing::error!(
                                    account = %DisplayAddr(account),
                                    "shard subscription somehow dropped",
                                );
                                keep = false;
                            }
                        }
                        Err(e) => {
                            tracing::error!(
                                account = %DisplayAddr(account),
                                "failed to get account: {e:?}",
                            );
                        }
                    };
                } else {
                    subscription.state_rx.borrow_and_update();
                }

                if let Err(e) = subscription.handle_block(
                    &self.messages_queue,
                    &shard_accounts,
                    block_id,
                    &block_info,
                    &account_blocks,
                    account,
                ) {
                    tracing::error!(%block_id, "failed to handle block: {e:?}");
                }

                keep
            });
        }

        self.messages_queue
            .update(block_info.shard(), block_info.gen_utime().as_u32());

        Ok(())
    }

    fn update_signature_id(&self, key_block: &ton_block::Block) -> Result<()> {
        let extra = key_block.read_extra()?;
        let custom = extra
            .read_custom()?
            .context("McBlockExtra not found in the masterchain block")?;
        let config = custom
            .config()
            .context("Config not found in the key block")?;

        self.signature_id
            .store(config.capabilities(), key_block.global_id);

        Ok(())
    }

    async fn wait_sync(&self) {
        if self.ready.load(Ordering::Acquire) {
            return;
        }
        self.ready_signal.notified().await;
    }
}

#[async_trait::async_trait]
impl ton_indexer::Subscriber for TonSubscriber {
    async fn engine_status_changed(&self, status: EngineStatus) {
        if status == EngineStatus::Synced {
            tracing::info!("TON subscriber is ready");
            self.ready.store(true, Ordering::Release);
            self.ready_signal.notify_waiters();
        }
    }

    async fn process_block(&self, ctx: ProcessBlockContext<'_>) -> Result<()> {
        if let Some(shard_state) = ctx.shard_state() {
            if ctx.is_masterchain() {
                self.handle_masterchain_block(ctx.meta(), ctx.id(), ctx.block())?;
            } else {
                self.handle_shard_block(ctx.id(), ctx.block(), shard_state)?;
            }
        }
        Ok(())
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct TonSubscriberMetrics {
    pub ready: bool,
    pub current_utime: u32,
    pub signature_id: Option<i32>,
    pub pending_message_count: usize,
}

static BLOCK_AWAITER_ID: AtomicUsize = AtomicUsize::new(0);

struct StateSubscription {
    state_tx: ShardAccountTx,
    state_rx: ShardAccountRx,
    transaction_subscriptions: Vec<Weak<dyn TransactionsSubscription>>,
}

impl StateSubscription {
    fn update_status(&mut self) -> StateSubscriptionStatus {
        self.transaction_subscriptions
            .retain(|item| item.strong_count() > 0);

        if self.state_tx.receiver_count() > 1 {
            StateSubscriptionStatus::Alive
        } else if !self.transaction_subscriptions.is_empty() {
            StateSubscriptionStatus::PartlyAlive
        } else {
            StateSubscriptionStatus::Stopped
        }
    }

    fn handle_block(
        &self,
        messages_queue: &PendingMessagesQueue,
        shard_accounts: &ton_block::ShardAccounts,
        block_id: &ton_block::BlockIdExt,
        block_info: &ton_block::BlockInfo,
        account_blocks: &ton_block::ShardAccountBlocks,
        account: &UInt256,
    ) -> Result<()> {
        if self.transaction_subscriptions.is_empty() {
            return Ok(());
        }

        let account_block = match account_blocks
            .get_with_aug(account)
            .with_context(|| format!("Failed to get account block for {account:x}"))?
        {
            Some((account_block, _)) => account_block,
            None => return Ok(()),
        };

        for transaction in account_block.transactions().iter() {
            let (hash, transaction) = match transaction.and_then(|(_, value)| {
                let cell = value.into_cell().reference(0)?;
                let hash = cell.repr_hash();

                ton_block::Transaction::construct_from_cell(cell)
                    .map(|transaction| (hash, transaction))
            }) {
                Ok(tx) => tx,
                Err(e) => {
                    tracing::error!(
                        %block_id,
                        account = %DisplayAddr(account),
                        "failed to parse transaction: {e:?}",
                    );
                    continue;
                }
            };

            // Skip non-ordinary or aborted transactions
            let transaction_info = match transaction.description.read_struct() {
                Ok(ton_block::TransactionDescr::Ordinary(info)) if !info.aborted => info,
                _ => continue,
            };

            let in_msg = match transaction
                .in_msg
                .as_ref()
                .map(|message| (message, message.read_struct()))
            {
                Some((message_cell, Ok(message))) => {
                    if matches!(message.header(), ton_block::CommonMsgInfo::ExtInMsgInfo(_)) {
                        messages_queue.deliver_message(*account, message_cell.hash());
                    }
                    message
                }
                _ => continue,
            };

            let ctx = TxContext {
                shard_accounts,
                block_info,
                account,
                transaction_hash: &hash,
                transaction_info: &transaction_info,
                transaction: &transaction,
                in_msg: &in_msg,
            };

            // Handle transaction
            for subscription in self.iter_transaction_subscriptions() {
                if let Err(e) = subscription.handle_transaction(ctx) {
                    tracing::error!(
                        %block_id,
                        tx = hash.to_hex_string(),
                        account = %DisplayAddr(account),
                        "Failed to handle transaction: {e:?}",
                    );
                }
            }
        }

        Ok(())
    }

    fn iter_transaction_subscriptions(
        &'_ self,
    ) -> impl Iterator<Item = Arc<dyn TransactionsSubscription>> + '_ {
        self.transaction_subscriptions
            .iter()
            .filter_map(Weak::upgrade)
    }
}

#[derive(Copy, Clone, Eq, PartialEq)]
enum StateSubscriptionStatus {
    Alive,
    PartlyAlive,
    Stopped,
}

trait BlockAwaiter: Send + Sync {
    fn handle_block(
        &mut self,
        block: &ton_block::Block,
        block_info: &ton_block::BlockInfo,
    ) -> Result<BlockAwaiterAction>;
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
enum BlockAwaiterAction {
    Retain,
    Remove,
}

pub trait TransactionsSubscription: Send + Sync {
    fn handle_transaction(&self, ctx: TxContext<'_>) -> Result<()>;
}

/// Generic listener for transactions
pub struct AccountObserver<T>(AccountEventsTx<T>);

impl<T> AccountObserver<T> {
    pub fn new(tx: &AccountEventsTx<T>) -> Arc<Self> {
        Arc::new(Self(tx.clone()))
    }
}

impl<T> TransactionsSubscription for AccountObserver<T>
where
    T: ReadFromTransaction + std::fmt::Debug + Send + Sync,
{
    fn handle_transaction(&self, ctx: TxContext<'_>) -> Result<()> {
        let event = T::read_from_transaction(&ctx);

        tracing::info!(
            account = %DisplayAddr(ctx.account),
            "got transaction on account: {event:?}",
        );

        // Send event to event manager if it exist
        if let Some(event) = event {
            if self.0.send((*ctx.account, event)).is_err() {
                tracing::error!(
                    account = %DisplayAddr(ctx.account),
                    "failed to send event: channel is dropped",
                );
            }
        }

        // Done
        Ok(())
    }
}

pub fn start_listening_events<S, E, R>(
    service: &Arc<S>,
    name: &'static str,
    mut events_rx: mpsc::UnboundedReceiver<E>,
    handler: fn(Arc<S>, E) -> R,
) where
    S: Send + Sync + 'static,
    E: Send + 'static,
    R: futures_util::Future<Output = Result<()>> + Send + 'static,
{
    let service = Arc::downgrade(service);

    tokio::spawn(async move {
        while let Some(event) = events_rx.recv().await {
            let service = match service.upgrade() {
                Some(service) => service,
                None => break,
            };

            if let Err(e) = handler(service, event).await {
                tracing::error!(contract = name, "failed to handle event: {e:?}");
            }
        }

        tracing::warn!(contract = name, "stopped listening for events");

        events_rx.close();
        while events_rx.recv().await.is_some() {}
    });
}

#[derive(Default)]
struct SignatureId(AtomicU64);

impl SignatureId {
    const WITH_SIGNATURE_ID: u64 = 1 << 32;

    fn load(&self) -> Option<i32> {
        let id = self.0.load(Ordering::Acquire);
        if id & Self::WITH_SIGNATURE_ID != 0 {
            Some(id as i32)
        } else {
            None
        }
    }

    fn store(&self, capabilities: u64, global_id: i32) {
        const CAP_WITH_SIGNATURE_ID: u64 = 0x4000000;
        let id = if capabilities & CAP_WITH_SIGNATURE_ID != 0 {
            Self::WITH_SIGNATURE_ID | (global_id as u32 as u64)
        } else {
            0
        };
        self.0.store(id, Ordering::Release);
    }
}

type ShardAccountTx = watch::Sender<Option<ton_block::ShardAccount>>;
type ShardAccountRx = watch::Receiver<Option<ton_block::ShardAccount>>;

pub type AccountEventsTx<T> = mpsc::UnboundedSender<(UInt256, T)>;

#[derive(thiserror::Error, Debug)]
enum TonSubscriberError {
    #[error("Account is frozen")]
    AccountIsFrozen,
}
