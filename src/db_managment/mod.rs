/// This tables stores met ethereum transactions
/// For example, we have 10 blocks to confirm. On block 123 we 've met tx
/// Then we will enqueue it in the table with block number 133 and on block 133 we will confirm all met transactions
/// On the current block - 10
pub mod eth_queue;
/// Here all lifetime stats are stored. key is relay pubkey, hash is Stats
pub mod stats_db;
///
pub mod ton_queue;

pub mod constants;
pub mod models;
mod prelude;

use std::collections::HashMap;
use std::io::Write;


pub use self::eth_queue::*;
pub use self::stats_db::*;
pub use self::ton_queue::*;

pub use self::models::*;
use self::prelude::*;

/// This module contains all db related operations.
/// We are using `sled` as kv storage and `bincode` for encoding in the binary format.
pub trait Table {
    type Key;
    type Value;
    fn dump_elements(&self) -> HashMap<Self::Key, Self::Value>;
}

pub fn dump_all_trees<W>(db: &Db, _ton_writer: W, eth_writer: W)
where
    W: Write,
{
    let eth_queue = EthQueue::new(db).unwrap();
    let eth_elements = eth_queue.dump_elements();
    serde_json::to_writer_pretty(eth_writer, &eth_elements).unwrap();

    //
    // let ton_queue = TonQueue::new(db).unwrap();
    // let ton_elements = ton_queue.dump_elements();
    // serde_json::to_writer_pretty(ton_writer, &ton_elements).unwrap();
}
