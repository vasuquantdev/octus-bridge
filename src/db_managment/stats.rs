use std::collections::HashMap;

use relay_eth::ws::H256;

use relay_ton::prelude::UInt256;

use crate::db_managment::constants::STATS_TREE_NAME;
use crate::db_managment::models::TxStat;
use crate::db_managment::Table;

use super::prelude::{Error, Tree};
use super::Db;

pub struct StatsProvider {
    tree: Tree,
}

impl StatsProvider {
    pub fn new(db: &Db) -> Result<Self, Error> {
        Ok(Self {
            tree: db.open_tree(STATS_TREE_NAME)?,
        })
    }

    pub fn update_relay_stats(&self, relay_key: &UInt256, tx_hash: H256) -> Result<(), Error> {
        log::debug!("Inserting stats");
        let current_time = chrono::Utc::now();
        let pubkey = hex::encode(&relay_key.as_slice());

        let mut list_of_transactions = match self.tree.get(&pubkey.as_bytes())? {
            Some(a) => bincode::deserialize(&*a)?,
            None => Vec::new(),
        };
        let stat = TxStat {
            tx_hash,
            met: current_time,
        };
        list_of_transactions.push(stat);
        self.tree.insert(
            &pubkey,
            bincode::serialize(&list_of_transactions).expect("Shouldn't fail"),
        )?;
        Ok(())
    }
}

impl Table for StatsProvider {
    type Key = String;
    type Value = Vec<TxStat>;

    fn dump_elements(&self) -> HashMap<Self::Key, Self::Value> {
        self.tree
            .iter()
            .filter_map(|x| x.ok())
            .map(|(k, v)| {
                (
                    String::from_utf8(k.to_vec()).unwrap(),
                    bincode::deserialize(&v).expect("Shouldn't fail"),
                )
            })
            .collect()
    }
}
