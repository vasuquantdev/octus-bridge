use std::collections::HashMap;

use anyhow::Error;
use ethereum_types::H256;
use num_bigint::BigUint;
use sled::{Db, Tree};

use crate::db_managment::Table;
use crate::engine::bridge::models::ExtendedEventInfo;

use super::constants::TON_EVENTS_TREE_NAME;

pub struct TonTree {
    inner: Tree,
}

impl TonTree {
    pub fn new(db: &Db) -> Result<Self, Error> {
        let tree = db.open_tree(super::constants::TON_EVENTS_TREE_NAME)?;
        Ok(Self { inner: tree })
    }

    pub fn insert(&self, key: H256, value: &ExtendedEventInfo) -> Result<(), Error> {
        let value = bincode::serialize(&value).expect("Shouldn't fail");
        log::warn!("WRITING: {:?}", key);
        self.inner.insert(key.as_fixed_bytes(), value)?;
        Ok(())
    }

    pub fn remove_event_by_hash(&self, key: &H256) -> Result<(), Error> {
        log::warn!("REMOVING: {:?}", key);
        self.inner.remove(key.as_fixed_bytes())?;
        Ok(())
    }

    pub fn get_event_by_hash(&self, key: &H256) -> Result<Option<ExtendedEventInfo>, Error> {
        log::warn!("GETTING: {:?}", key);
        Ok(self
            .inner
            .get(key.as_fixed_bytes())?
            .map(|x| bincode::deserialize::<ExtendedEventInfo>(&x).unwrap()))
    }

    /// Get all blocks before specified block number
    pub fn scan_for_block_lower_bound(&self, block_number: BigUint) -> Vec<ExtendedEventInfo> {
        self.inner
            .iter()
            .values()
            .filter_map(|x| match x {
                Ok(a) => Some(a),
                Err(e) => {
                    log::error!("Bad value in {}: {}", TON_EVENTS_TREE_NAME, e);
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

    fn dump_elements(&self) -> HashMap<Self::Key, Self::Value> {
        self.inner
            .iter()
            .filter_map(|x| x.ok())
            .map(|(k, v)| {
                (
                    H256::from_slice(&k),
                    bincode::deserialize(&v).expect("Shouldn't fail"),
                )
            })
            .collect()
    }
}
