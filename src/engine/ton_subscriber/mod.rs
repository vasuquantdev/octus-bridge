use std::collections::{hash_map, HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;

use anyhow::Result;
use nekoton_utils::NoFailure;
use parking_lot::{Mutex, RwLock};
use tokio::sync::{mpsc, watch};
use ton_block::{BinTreeType, HashmapAugType};
use ton_indexer::utils::{BlockIdExtExtension, BlockProofStuff, BlockStuff, ShardStateStuff};
use ton_indexer::EngineStatus;
use ton_types::UInt256;

pub struct TonSubscriber {
    ready: AtomicBool,
    external_messages_tx: ExternalMessagesTx,
    current_utime: AtomicU32,
    state_subscriptions: Mutex<HashMap<UInt256, StateSubscription>>,
    shards: RwLock<HashSet<ton_block::ShardIdent>>,
}

impl TonSubscriber {
    pub fn new(external_messages_tx: ExternalMessagesTx) -> Arc<Self> {
        Arc::new(Self {
            ready: AtomicBool::new(false),
            external_messages_tx,
            current_utime: AtomicU32::new(0),
            state_subscriptions: Mutex::new(HashMap::new()),
            shards: RwLock::new(HashSet::new()),
        })
    }

    pub fn start(self: &Arc<Self>) {
        tokio::spawn(async move {
            // TODO
        });
    }

    pub async fn get_contract_state(
        &self,
        account: UInt256,
    ) -> Result<Option<ton_block::ShardAccount>> {
        let mut state_rx = match self.state_subscriptions.lock().entry(account) {
            hash_map::Entry::Vacant(entry) => {
                let (state_tx, state_rx) = watch::channel(None);
                entry
                    .insert(StateSubscription { state_tx, state_rx })
                    .state_rx
                    .clone()
            }
            hash_map::Entry::Occupied(entry) => entry.get().state_rx.clone(),
        };

        state_rx.changed().await?;

        let value = (*state_rx.borrow()).clone();
        Ok(value)
    }

    async fn send_message(&self, message: ton_block::Message) -> Result<()> {
        self.external_messages_tx.send(message).await?;
        Ok(())
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
        let info = block.info.read_struct().convert()?;
        let accounts = shard_state.read_accounts().convert()?;

        let mut blocks = self.state_subscriptions.lock();

        blocks.retain(|account, subscription| {
            if subscription.state_tx.is_closed() || subscription.state_tx.receiver_count() <= 1 {
                log::info!("RESETTING SUBSCRIPTION: {}", account);
                return false;
            }

            if !contains_account(info.shard(), account) {
                return true;
            }

            let mut keep = true;

            let account = match accounts.get(account) {
                Ok(account) => account,
                Err(e) => {
                    log::error!("Failed to get account: {}", account);
                    return true;
                }
            };

            log::info!("FOUND ACCOUNT STATE: {:?}", account);

            if subscription.state_tx.send(account).is_err() {
                log::error!("Shard subscription somehow dropped");
                keep = false;
            }

            keep
        });

        Ok(())
    }
}

#[async_trait::async_trait]
impl ton_indexer::Subscriber for TonSubscriber {
    async fn engine_status_changed(&self, status: EngineStatus) {
        if status == EngineStatus::Synced {
            log::info!("TON subscriber is ready");
            self.ready.store(true, Ordering::Release);
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
}

type ShardAccountTx = watch::Sender<Option<ton_block::ShardAccount>>;
type ShardAccountRx = watch::Receiver<Option<ton_block::ShardAccount>>;

pub type ExternalMessagesTx = mpsc::Sender<ton_block::Message>;
pub type ExternalMessagesRx = mpsc::Receiver<ton_block::Message>;

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
