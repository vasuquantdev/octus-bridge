/// This tables stores met ethereum transactions
/// For example, we have 10 blocks to confirm. On block 123 we 've met tx
/// Then we will enqueue it in the table with block number 133 and on block 133 we will confirm all met transactions
/// On the current block - 10
pub mod eth_queue;
/// Here all lifetime stats are stored. key is relay pubkey, hash is Stats
pub mod stats_db;
///
pub mod votes_queues;

pub mod constants;
mod prelude;

use std::collections::HashMap;

pub use self::eth_queue::*;
pub use self::stats_db::*;
pub use self::votes_queues::*;

/// This module contains all db related operations.
/// We are using `sled` as kv storage and `bincode` for encoding in the binary format.
pub trait Table {
    type Key;
    type Value;
    fn dump_elements(&self) -> HashMap<Self::Key, Self::Value>;
}
