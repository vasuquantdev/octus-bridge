use std::collections::HashMap;

use relay_eth::ws::H256;
use relay_ton::prelude::UInt256;
use ton_block::MsgAddrStd;

use crate::db_managment::{constants::STATS_TREE_NAME, Table};

use super::prelude::{Error, Tree};
use super::Db;
use crate::engine::bridge::models::ExtendedEventInfo;

#[derive(Clone)]
pub struct StatsDb {
    tree: Tree,
}

impl StatsDb {
    pub fn new(db: &Db) -> Result<Self, Error> {
        Ok(Self {
            tree: db.open_tree(STATS_TREE_NAME)?,
        })
    }

    pub fn update_relay_stats(&self, event: &ExtendedEventInfo) -> Result<(), Error> {
        log::debug!("Inserting stats");
        let current_time = chrono::Utc::now();

        let mut key = [0; 64];

        let addr = event.event_addr.address.get_bytestring(0);
        key[0..32].copy_from_slice(&addr);

        key[32..64].copy_from_slice(event.relay_key.as_slice());

        self.tree
            .insert(key, bincode::serialize(&current_time).unwrap())?;

        Ok(())
    }

    pub fn has_event(&self, event_addr: &MsgAddrStd) -> Result<bool, Error> {
        let prefix = event_addr.address.get_bytestring(0);

        Ok(self
            .tree
            .scan_prefix(&prefix)
            .keys()
            .next()
            .and_then(|key| key.ok())
            .is_some())
    }
}
//
// impl Table for StatsDb {
//     type Key = String;
//     type Value = Vec<TxStat>;
//
//     fn dump_elements(&self) -> HashMap<Self::Key, Self::Value> {
//         self.tree
//             .iter()
//             .filter_map(|x| x.ok())
//             .map(|(k, v)| {
//                 (
//                     String::from_utf8(k.to_vec()).unwrap(),
//                     bincode::deserialize(&v).expect("Shouldn't fail"),
//                 )
//             })
//             .collect()
//     }
// }
