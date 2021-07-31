use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;

use anyhow::Result;
use nekoton_utils::NoFailure;
use parking_lot::{Mutex, RwLock};
use tokio::sync::{mpsc, oneshot};
use ton_block::BinTreeType;
use ton_indexer::utils::{BlockIdExtExtension, BlockProofStuff, BlockStuff, ShardStateStuff};
use ton_indexer::EngineStatus;
use ton_types::UInt256;

pub struct TonSubscriber {
    ready: AtomicBool,
    external_messages_tx: ExternalMessagesTx,
    current_utime: AtomicU32,
    state_requests: Mutex<HashMap<UInt256, oneshot::Sender<ton_block::ShardAccount>>>,
    shards: RwLock<HashSet<ton_block::ShardIdent>>,
}

impl TonSubscriber {
    pub fn new(external_messages_tx: ExternalMessagesTx) -> Arc<Self> {
        Arc::new(Self {
            ready: AtomicBool::new(false),
            external_messages_tx,
            current_utime: AtomicU32::new(0),
            state_requests: Mutex::new(HashMap::new()),
            shards: RwLock::new(HashSet::new()),
        })
    }

    pub fn start(self: &Arc<Self>) {
        tokio::spawn(async move { todo!() });
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

        let shard_prefix_len = info.shard().prefix_len();

        let mut blocks = self.state_requests.lock();
        for (account, sender) in blocks.iter_mut() {
            let account_prefix = account_prefix(account, shard_prefix_len as usize);

            //sender.send()
        }

        todo!()
    }
}

#[async_trait::async_trait]
impl ton_indexer::Subscriber for TonSubscriber {
    async fn engine_status_changed(&self, status: EngineStatus) {
        if status == EngineStatus::Synced {
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

fn account_prefix(account: &UInt256, len: usize) -> u64 {
    debug_assert!(len <= 64);

    let account = account.as_slice();

    let mut value: u64 = 0;

    let bytes = len / 8;
    for i in 0..bytes {
        value |= (account[31 - i] as u64) << (8 * (7 - i));
    }

    let remainder = len % 8;
    if remainder > 0 {
        let r = account[31 - bytes] >> (8 - remainder);
        value |= (r as u64) << (8 * (7 - bytes) + 8 - remainder);
    }

    value
}

pub type ExternalMessagesTx = mpsc::Sender<ton_block::Message>;
pub type ExternalMessagesRx = mpsc::Receiver<ton_block::Message>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_account_prefix() {
        let mut account_id = [0u8; 32];
        for i in 0..8 {
            account_id[31 - i] = 0xff;
        }

        let account_id = UInt256::from(account_id);
        for i in 0..32 {
            let prefix = account_prefix(&account_id, i);
            assert_eq!(64 - prefix.trailing_zeros(), i as u32);
        }
    }
}
