pub mod constants;
pub mod eth_queue;
pub mod models;
mod prelude;
pub mod stats;
pub mod ton_db;
pub mod tx_monitor;

use std::io::Write;

use super::db_managment::prelude::*;
use crate::db_managment::eth_queue::EthQueue;
use crate::db_managment::ton_db::TonTree;
use relay_ton::prelude::HashMap;

pub trait Table {
    type Key;
    type Value;
    fn dump_elements(&self) -> HashMap<Self::Key, Self::Value>;
}

pub fn dump_all_trees<W>(db: &Db, ton_writer: W, eth_writer: W)
where
    W: Write,
{
    let ton_queue = TonTree::new(&db).unwrap();
    let eth_queue = EthQueue::new(db).unwrap();
    let eth_elements = eth_queue.dump_elements();
    let ton_elemnts = ton_queue.dump_elements();
    serde_json::to_writer_pretty(eth_writer, &eth_elements).unwrap();
    serde_json::to_writer_pretty(ton_writer, &ton_elemnts).unwrap();
}
