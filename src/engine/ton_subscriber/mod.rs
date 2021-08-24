use std::collections::{hash_map, HashMap};
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Weak};

use anyhow::{Context, Result};
use parking_lot::Mutex;
use tokio::sync::{oneshot, watch, Notify};
use ton_block::{BinTreeType, Deserializable, HashmapAugType};
use ton_indexer::utils::{BlockIdExtExtension, BlockProofStuff, BlockStuff, ShardStateStuff};
use ton_indexer::EngineStatus;
use ton_types::{HashmapType, UInt256};

use crate::utils::*;
use std::ops::Deref;

pub struct TonSubscriber {
    ready: AtomicBool,
    ready_signal: Notify,
    current_utime: AtomicU32,
    state_subscriptions: Mutex<HashMap<UInt256, StateSubscription>>,
    mc_block_awaiters: Mutex<Vec<Box<dyn BlockAwaiter>>>,
}

impl TonSubscriber {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            ready: AtomicBool::new(false),
            ready_signal: Notify::new(),
            current_utime: AtomicU32::new(0),
            state_subscriptions: Mutex::new(HashMap::new()),
            mc_block_awaiters: Mutex::new(Vec::with_capacity(4)),
        })
    }

    pub async fn start(self: &Arc<Self>) -> Result<()> {
        self.wait_sync().await;
        Ok(())
    }

    pub async fn wait_shards(&self) -> Result<ShardsMap> {
        struct Handler {
            tx: Option<oneshot::Sender<Result<ShardsMap>>>,
        }

        impl BlockAwaiter for Handler {
            fn handle_block(&mut self, block: &ton_block::Block) -> Result<()> {
                if let Some(tx) = self.tx.take() {
                    let _ = tx.send(extract_shards(block));
                }
                Ok(())
            }
        }

        fn extract_shards(block: &ton_block::Block) -> Result<ShardsMap> {
            let extra = block.extra.read_struct()?;
            let custom = match extra.read_custom()? {
                Some(custom) => custom,
                None => return Ok(ShardsMap::default()),
            };

            let mut shards = HashMap::with_capacity(16);

            custom.shards().iterate_with_keys(
                |wc_id: i32, ton_block::InRefValue(shards_tree)| {
                    if wc_id == ton_block::MASTERCHAIN_ID {
                        return Ok(true);
                    }

                    shards_tree.iterate(|prefix, descr| {
                        let shard_id = ton_block::ShardIdent::with_prefix_slice(wc_id, prefix)?;

                        shards.insert(
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

            Ok(shards)
        }

        let (tx, rx) = oneshot::channel();
        self.mc_block_awaiters
            .lock()
            .push(Box::new(Handler { tx: Some(tx) }));
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
        let account = state_rx.borrow();
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

            let shard_account = match state_rx.borrow().deref() {
                Some(shard_account) => ExistingContract::from_shard_account(shard_account)?,
                None => continue,
            };

            if let Some(shard_account) = shard_account {
                return Ok(shard_account);
            }
        }
    }

    fn handle_masterchain_block(&self, block: &ton_block::Block) -> Result<()> {
        let info = block.info.read_struct()?;
        self.current_utime
            .store(info.gen_utime().0, Ordering::Release);

        let awaiters = std::mem::take(&mut *self.mc_block_awaiters.lock());
        for mut awaiter in awaiters {
            if let Err(e) = awaiter.handle_block(block) {
                log::error!("Failed to handle masterchain block: {:?}", e);
            }
        }

        Ok(())
    }

    fn handle_shard_block(
        &self,
        block: &ton_block::Block,
        shard_state: &ton_block::ShardStateUnsplit,
    ) -> Result<()> {
        let block_info = block.info.read_struct()?;
        let extra = block.extra.read_struct()?;
        let account_blocks = extra.read_account_blocks()?;
        let shard_accounts = shard_state.read_accounts()?;

        let mut blocks = self.state_subscriptions.lock();

        blocks.retain(|account, subscription| {
            let subscription_status = subscription.update_status();
            if subscription_status == StateSubscriptionStatus::Stopped {
                return false;
            }

            if !contains_account(block_info.shard(), account) {
                return true;
            }

            if let Err(e) =
                subscription.handle_block(&shard_accounts, &block_info, &account_blocks, account)
            {
                log::error!("Failed to handle block: {:?}", e);
            }

            let mut keep = true;

            if subscription_status == StateSubscriptionStatus::Alive {
                let account = match shard_accounts.get(account) {
                    Ok(account) => account,
                    Err(e) => {
                        log::error!("Failed to get account {}: {:?}", account.to_hex_string(), e);
                        return true;
                    }
                };

                if subscription.state_tx.send(account).is_err() {
                    log::error!("Shard subscription somehow dropped");
                    keep = false;
                }
            }

            keep
        });

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
            log::info!("TON subscriber is ready");
            self.ready.store(true, Ordering::Release);
            self.ready_signal.notify_waiters();
        }
    }

    async fn process_block(
        &self,
        block: &BlockStuff,
        _block_proof: Option<&BlockProofStuff>,
        shard_state: &ShardStateStuff,
    ) -> Result<()> {
        if !self.ready.load(Ordering::Acquire) {
            return Ok(());
        }

        if block.id().is_masterchain() {
            self.handle_masterchain_block(block.block())?;
        } else {
            self.handle_shard_block(block.block(), shard_state.state())?;
        }

        Ok(())
    }
}

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
        shard_accounts: &ton_block::ShardAccounts,
        block_info: &ton_block::BlockInfo,
        account_blocks: &ton_block::ShardAccountBlocks,
        account: &UInt256,
    ) -> Result<()> {
        if self.transaction_subscriptions.is_empty() {
            return Ok(());
        }

        let account_block = match account_blocks.get_with_aug(account).with_context(|| {
            format!(
                "Failed to get account block for {}",
                account.to_hex_string()
            )
        })? {
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
                    log::error!(
                        "Failed to parse transaction in block {} for account {}: {:?}",
                        block_info.seq_no(),
                        account.to_hex_string(),
                        e
                    );
                    continue;
                }
            };

            // Skip non-ordinary or aborted transactions
            let transaction_info = match transaction.description.read_struct() {
                Ok(ton_block::TransactionDescr::Ordinary(info)) if !info.aborted => info,
                _ => continue,
            };

            let ctx = TxContext {
                shard_accounts,
                block_info,
                account,
                transaction_hash: &hash,
                transaction_info: &transaction_info,
                transaction: &transaction,
            };

            // Handle transaction
            for subscription in self.iter_transaction_subscriptions() {
                if let Err(e) = subscription.handle_transaction(ctx) {
                    log::error!(
                        "Failed to handle transaction {} for account {}: {:?}",
                        hash.to_hex_string(),
                        account.to_hex_string(),
                        e
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
            .map(Weak::upgrade)
            .flatten()
    }
}

#[derive(Copy, Clone, Eq, PartialEq)]
enum StateSubscriptionStatus {
    Alive,
    PartlyAlive,
    Stopped,
}

pub trait BlockAwaiter: Send + Sync {
    fn handle_block(&mut self, block: &ton_block::Block) -> Result<()>;
}

pub trait TransactionsSubscription: Send + Sync {
    fn handle_transaction(&self, ctx: TxContext<'_>) -> Result<()>;
}

#[derive(Copy, Clone)]
pub struct TxContext<'a> {
    pub shard_accounts: &'a ton_block::ShardAccounts,
    pub block_info: &'a ton_block::BlockInfo,
    pub account: &'a UInt256,
    pub transaction_hash: &'a UInt256,
    pub transaction_info: &'a ton_block::TransactionDescrOrdinary,
    pub transaction: &'a ton_block::Transaction,
}

type ShardAccountTx = watch::Sender<Option<ton_block::ShardAccount>>;
type ShardAccountRx = watch::Receiver<Option<ton_block::ShardAccount>>;

type ShardsMap = HashMap<ton_block::ShardIdent, ton_block::BlockIdExt>;
