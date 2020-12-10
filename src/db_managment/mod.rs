use std::io::Write;

use crate::db_managment::eth_queue::EthQueue;
use crate::db_managment::ton_db::TonTree;

use super::db_managment::prelude::*;

pub mod constants;
pub mod eth_queue;
pub mod models;
mod prelude;
pub mod stats;
pub mod ton_db;

pub trait Table {
    type Key;
    type Value;
    fn dump_elements(&self) -> Vec<(Self::Key, Self::Value)>;
}

pub fn dump_all_trees<W>(db: &Db, writer: W)
where
    W: Write + Clone,
{
    let ton_queue = TonTree::new(&db).unwrap();
    let eth_queue = EthQueue::new(db).unwrap();
    let eth_elements = eth_queue.dump_elements();
    let ton_elemnts = ton_queue.dump_elements();
    serde_json::to_writer_pretty(writer.clone(), &eth_elements).unwrap();
    serde_json::to_writer_pretty(writer, &ton_elemnts).unwrap();
}
