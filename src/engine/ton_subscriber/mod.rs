use std::collections::{hash_map, HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Weak};

use anyhow::{Context, Result};
use nekoton_utils::NoFailure;
use parking_lot::{Mutex, RwLock};
use tokio::sync::{watch, Notify};
use ton_block::{BinTreeType, Deserializable, HashmapAugType};
use ton_indexer::utils::{BlockIdExtExtension, BlockProofStuff, BlockStuff, ShardStateStuff};
use ton_indexer::EngineStatus;
use ton_types::{HashmapType, UInt256};

pub struct TonSubscriber {
    ready: AtomicBool,
    ready_signal: Notify,
    current_utime: AtomicU32,
    state_subscriptions: Mutex<HashMap<UInt256, StateSubscription>>,
    shards: RwLock<HashSet<ton_block::ShardIdent>>,
}

impl TonSubscriber {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            ready: AtomicBool::new(false),
            ready_signal: Notify::new(),
            current_utime: AtomicU32::new(0),
            state_subscriptions: Mutex::new(HashMap::new()),
            shards: RwLock::new(HashSet::new()),
        })
    }

    pub async fn start(self: &Arc<Self>) -> Result<()> {
        self.wait_sync().await;
        Ok(())
    }

    pub fn add_transactions_subscription(
        &self,
        account: UInt256,
        subscription: &Arc<dyn TransactionsSubscription>,
    ) {
        let mut state_subscriptions = self.state_subscriptions.lock();

        match state_subscriptions.entry(account) {
            hash_map::Entry::Vacant(entry) => {
                let (state_tx, state_rx) = watch::channel(None);
                entry.insert(StateSubscription {
                    state_tx,
                    state_rx,
                    transaction_subscriptions: vec![Arc::downgrade(subscription)],
                });
            }
            hash_map::Entry::Occupied(mut entry) => {
                entry
                    .get_mut()
                    .transaction_subscriptions
                    .push(Arc::downgrade(subscription));
            }
        };
    }

    pub async fn get_contract_state(
        &self,
        account: UInt256,
    ) -> Result<Option<ton_block::ShardAccount>> {
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

        let value = (*state_rx.borrow()).clone();
        Ok(value)
    }

    fn handle_masterchain_block(&self, block: &ton_block::Block) -> Result<()> {
        let info = block.info.read_struct().convert()?;
        self.current_utime
            .store(info.gen_utime().0, Ordering::Release);

        let extra = block.extra.read_struct().convert()?;
        let custom = match extra.read_custom().convert()? {
            Some(custom) => custom,
            None => return Ok(()),
        };

        let mut shards = HashSet::with_capacity(16);

        custom
            .shards()
            .iterate_with_keys(|wc_id: i32, ton_block::InRefValue(shards_tree)| {
                if wc_id == ton_block::MASTERCHAIN_ID {
                    return Ok(true);
                }

                shards_tree.iterate(|prefix, _| {
                    shards.insert(ton_block::ShardIdent::with_prefix_slice(wc_id, prefix)?);
                    Ok(true)
                })
            })
            .convert()?;

        *self.shards.write() = shards;

        Ok(())
    }

    fn handle_shard_block(
        &self,
        block: &ton_block::Block,
        shard_state: &ton_block::ShardStateUnsplit,
    ) -> Result<()> {
        let block_info = block.info.read_struct().convert()?;
        let extra = block.extra.read_struct().convert()?;
        let account_blocks = extra.read_account_blocks().convert()?;
        let accounts = shard_state.read_accounts().convert()?;

        let mut blocks = self.state_subscriptions.lock();

        blocks.retain(|account, subscription| {
            let subscription_status = subscription.update_status();
            if subscription_status == StateSubscriptionStatus::Stopped {
                return false;
            }

            if !contains_account(block_info.shard(), account) {
                return true;
            }

            if let Err(e) = subscription.handle_block(&block_info, &account_blocks, account) {
                log::error!("Failed to handle block: {:?}", e);
            }

            let mut keep = true;

            if subscription_status == StateSubscriptionStatus::Alive {
                let account = match accounts.get(account) {
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

fn contains_account(shard: &ton_block::ShardIdent, account: &UInt256) -> bool {
    let shard_prefix = shard.shard_prefix_with_tag();
    if shard_prefix == ton_block::SHARD_FULL {
        true
    } else {
        let len = shard.prefix_len();
        let account_prefix = account_prefix(account, len as usize) >> (64 - len);
        let shard_prefix = shard_prefix >> (64 - len);
        account_prefix == shard_prefix
    }
}

fn account_prefix(account: &UInt256, len: usize) -> u64 {
    debug_assert!(len <= 64);

    let account = account.as_slice();

    let mut value: u64 = 0;

    let bytes = len / 8;
    for i in 0..bytes {
        value |= (account[i] as u64) << (8 * (7 - i));
    }

    let remainder = len % 8;
    if remainder > 0 {
        let r = account[bytes] >> (8 - remainder);
        value |= (r as u64) << (8 * (7 - bytes) + 8 - remainder);
    }

    value
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
        block_info: &ton_block::BlockInfo,
        account_blocks: &ton_block::ShardAccountBlocks,
        account: &UInt256,
    ) -> Result<()> {
        if self.transaction_subscriptions.is_empty() {
            return Ok(());
        }

        let account_block = match account_blocks
            .get_with_aug(&account)
            .convert()
            .with_context(|| {
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

            for subscription in self.iter_transaction_subscriptions() {
                if let Err(e) = subscription.handle_transaction(&block_info, &hash, &transaction) {
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

pub trait TransactionsSubscription: Send + Sync {
    fn handle_transaction(
        &self,
        block_info: &ton_block::BlockInfo,
        transaction_hash: &UInt256,
        transaction: &ton_block::Transaction,
    ) -> Result<()>;
}

type ShardAccountTx = watch::Sender<Option<ton_block::ShardAccount>>;
type ShardAccountRx = watch::Receiver<Option<ton_block::ShardAccount>>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_account_prefix() {
        let mut account_id = [0u8; 32];
        for i in 0..8 {
            account_id[i] = 0xff;
        }

        let account_id = UInt256::from(account_id);
        for i in 0..64 {
            let prefix = account_prefix(&account_id, i);
            assert_eq!(64 - prefix.trailing_zeros(), i as u32);
        }
    }

    #[test]
    fn test_contains_account() {
        let account = ton_types::UInt256::from_be_bytes(
            &hex::decode("459b6795bf4d4c3b930c83fe7625cfee99a762e1e114c749b62bfa751b781fa5")
                .unwrap(),
        );

        let mut shards =
            vec![ton_block::ShardIdent::with_tagged_prefix(0, ton_block::SHARD_FULL).unwrap()];
        for _ in 0..4 {
            let mut new_shards = vec![];
            for shard in &shards {
                let (left, right) = shard.split().unwrap();
                new_shards.push(left);
                new_shards.push(right);
            }

            shards = new_shards;
        }

        let mut target_shard = None;
        for shard in shards {
            if !contains_account(&shard, &account) {
                continue;
            }

            if target_shard.is_some() {
                panic!("Account can't be in two shards");
            }
            target_shard = Some(shard);
        }

        assert!(
            matches!(target_shard, Some(shard) if shard.shard_prefix_with_tag() == 0x4800000000000000)
        );
    }
}
