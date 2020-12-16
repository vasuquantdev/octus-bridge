use std::collections::HashMap;

use relay_eth::ws::H256;

use ton_block::MsgAddrStd;

use crate::db_managment::{constants::STATS_TREE_NAME, Table, TxStat};

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
        let event_addr = event.event_addr.address.get_bytestring(0);

        let mut key = [0; 64];
        key[0..32].copy_from_slice(&event_addr);
        key[32..64].copy_from_slice(event.relay_key.as_slice());

        let stats = TxStat {
            tx_hash: H256::from_slice(&event.data.ethereum_event_transaction),
            met: chrono::Utc::now(),
        };

        self.tree.insert(key, bincode::serialize(&stats).unwrap())?;

        Ok(())
    }

    pub fn has_event(&self, event_addr: &MsgAddrStd) -> Result<bool, Error> {
        let event_addr = event_addr.address.get_bytestring(0);

        Ok(self
            .tree
            .scan_prefix(&event_addr)
            .keys()
            .next()
            .and_then(|key| key.ok())
            .is_some())
    }
}

impl Table for StatsDb {
    type Key = String;
    type Value = Vec<TxStat>;

    fn dump_elements(&self) -> HashMap<Self::Key, Self::Value> {
        self.tree
            .iter()
            .filter_map(|x| x.ok())
            .fold(HashMap::new(), |mut result, (k, v)| {
                let relay_key = H256::from_slice(&k[32..64]);

                let stats: TxStat = bincode::deserialize(&v).expect("Shouldn't fail");

                result
                    .entry(hex::encode(&relay_key))
                    .or_insert(Vec::new())
                    .push(stats);

                result
            })
    }
}
