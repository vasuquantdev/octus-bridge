use std::io::Write;

use crate::db_managment::models::EthTonConfirmationData;

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
    W: Write,
{
    let eth_queue = db.open_tree(constants::ETH_QUEUE_TREE_NAME).unwrap();
    let events: Vec<(u64, Vec<EthTonConfirmationData>)> = eth_queue
        .iter()
        .filter_map(|x| x.ok())
        .map(|(k, v)| {
            let (k, v) = (
                bincode::deserialize::<u64>(&k),
                bincode::deserialize::<Vec<EthTonConfirmationData>>(&v),
            );
            if k.is_ok() && v.is_ok() {
                return (k.ok(), v.ok());
            }
            (None, None)
        })
        .filter(|x| x.0.is_some() && x.1.is_some())
        .map(|x| (x.0.unwrap(), x.1.unwrap()))
        .collect();
    log::info!("{}", events.len());
    serde_json::to_writer_pretty(writer, &events).unwrap();
}
