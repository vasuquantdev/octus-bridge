use anyhow::Error;
use ethereum_types::H256;
use futures::StreamExt;
use sled::{Db, Tree};

use relay_ton::prelude::BigUint;

use crate::db_managment::Table;
use crate::engine::bridge::models::ExtendedEventInfo;

pub const PERSISTENT_TREE_NAME: &str = "unconfirmed_events";

pub struct TonTree {
    inner: Tree,
}

impl TonTree {
    pub fn new(db: &Db) -> Result<Self, Error> {
        let tree = db.open_tree(super::constants::TON_EVENTS_TREE_NAME)?;
        Ok(Self { inner: tree })
    }

    pub fn insert(&self, key: H256, value: &ExtendedEventInfo) -> Result<(), Error> {
        let key = bincode::serialize(&key).expect("Shouldn't fail");
        let value = bincode::serialize(&value).expect("Shouldn't fail");
        self.inner.insert(key, value)?;
        Ok(())
    }

    pub fn remove_event_by_hash(&self, key: &H256) -> Result<(), Error> {
        self.inner.remove(key.0)?;
        Ok(())
    }

    pub fn get_event_by_hash(&self, hash: &H256) -> Result<Option<ExtendedEventInfo>, Error> {
        Ok(self
            .inner
            .get(hash)?
            .and_then(|x| bincode::deserialize(&x).expect("Shouldn't fail")))
    }

    /// Get all blocks before specified block number
    pub fn scan_for_block_lower_bound(&self, block_number: BigUint) -> Vec<ExtendedEventInfo> {
        self.inner
            .iter()
            .values()
            .filter_map(|x| match x {
                Ok(a) => Some(a),
                Err(e) => {
                    log::error!("Bad value in {}: {}", PERSISTENT_TREE_NAME, e);
                    None
                }
            })
            .map(|x| bincode::deserialize::<ExtendedEventInfo>(&x))
            .filter_map(|x| x.ok()) // shouldn't fail
            .filter(|x| x.data.event_block_number == block_number)
            .collect()
    }
}

impl Table for TonTree {
    type Key = H256;
    type Value = ExtendedEventInfo;

    fn dump_elements(&self) -> Vec<(Self::Key, Self::Value)> {
        self.inner
            .iter()
            .filter_map(|x| x.ok())
            .map(|(k, v)| {
                (
                    bincode::deserialize(&k).expect("Shouldn't fail"),
                    bincode::deserialize(&v).expect("Shouldn't fail"),
                )
            })
            .collect()
    }
}
