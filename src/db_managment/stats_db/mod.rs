use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use ton_block::MsgAddrStd;

use relay_eth::ws::H256;
use relay_ton::prelude::UInt256;

use super::prelude::{Error, Tree};
use super::Db;
use crate::db_managment::{constants::STATS_TREE_NAME, Table, TxStat};
use crate::engine::bridge::models::{EventVote, ExtendedEventInfo};

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

        let mut key = [0; 65];
        key[0..32].copy_from_slice(&event_addr);
        key[32..64].copy_from_slice(event.relay_key.as_slice());
        key[64] = (event.vote == EventVote::Confirm) as u8;

        self.tree.insert(
            key,
            bincode::serialize(&StoredTxStat {
                tx_hash: event.data.ethereum_event_transaction,
                met: chrono::Utc::now(),
            })
            .unwrap(),
        )?;

        Ok(())
    }

    pub fn has_already_voted(
        &self,
        event_addr: &MsgAddrStd,
        relay_key: &UInt256,
    ) -> Result<bool, Error> {
        let mut key = Vec::with_capacity(64);
        key.extend_from_slice(&event_addr.address.get_bytestring(0));
        key.extend_from_slice(relay_key.as_slice());

        Ok(self
            .tree
            .scan_prefix(&key)
            .keys()
            .next()
            .and_then(|key| {
                key.ok()
                    .and_then(|item| bincode::deserialize::<TxStat>(&item).ok())
            })
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
            .fold(HashMap::new(), |mut result, (key, value)| {
                let event_addr = MsgAddrStd {
                    anycast: None,
                    workchain_id: 0,
                    address: UInt256::from(&key[0..32]).into(),
                };
                let relay_key = H256::from_slice(&key[32..64]);
                let vote = if key[64] == 0 {
                    EventVote::Reject
                } else {
                    EventVote::Confirm
                };

                let stats: StoredTxStat = bincode::deserialize(&value).expect("Shouldn't fail");

                result
                    .entry(hex::encode(&relay_key))
                    .or_insert(Vec::new())
                    .push(TxStat {
                        tx_hash: stats.tx_hash,
                        met: stats.met,
                        event_addr,
                        vote,
                    });

                result
            })
    }
}

#[derive(Deserialize, Serialize)]
pub struct StoredTxStat {
    pub tx_hash: H256,
    pub met: DateTime<Utc>,
}
