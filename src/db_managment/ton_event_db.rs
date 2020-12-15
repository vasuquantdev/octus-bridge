use std::collections::{HashMap, HashSet};

use anyhow::Error;
use ethereum_types::H256;
use num_bigint::BigUint;
use sled::{Db, IVec, Tree};

use crate::db_managment::Table;
use crate::engine::bridge::models::{ExtendedEventInfo, ReducedEventInfo};

use super::constants::TON_EVENTS_TREE_NAME;

pub struct TonTree {
    inner: Tree,
}

impl TonTree {
    pub fn new(db: &Db) -> Result<Self, Error> {
        let tree = db.open_tree(super::constants::TON_EVENTS_TREE_NAME)?;
        Ok(Self { inner: tree })
    }

    pub fn insert(&self, key: ExtendedEventInfo) -> Result<(), Error> {
        let reduced_info = ReducedEventInfo::from(key);
        let key = bincode::serialize(&reduced_info).expect("Shouldn't fail");
        log::warn!("WRITING: {:?}", key);
        self.inner.insert(key, &[0; 0])?;
        Ok(())
    }

    /// Get all blocks before specified block number where event is not rejected
    pub fn scan_for_block_lower_bound(&self, block_number: BigUint) -> Vec<ReducedEventInfo> {
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
            .map(|x| bincode::deserialize::<ReducedEventInfo>(&x))
            .filter_map(|x| x.ok()) // shouldn't fail
            .filter(|v| v.event_block_number < block_number && !v.event_rejected)
            .collect()
    }

    /// removes `ExtendedEventInfo`, having event_block_number lower than `block_number`
    pub fn gc_old_blocks(&self, block_number: BigUint) -> Result<(), Error> {
        let mut batch = sled::Batch::default();
        self.inner
            .iter()
            .keys()
            .filter_map(|x| match x {
                Ok(a) => Some(a),
                Err(e) => {
                    log::error!("Failed getting from db: {:?}", e);
                    None
                }
            })
            .map(|x| bincode::deserialize::<ReducedEventInfo>(&x))
            .filter_map(|x| match x {
                Ok(a) => Some(a),
                Err(e) => {
                    log::error!("Failed deserializing: {:?}", e);
                    None
                }
            })
            .filter(|x| x.event_block_number < block_number && !x.event_rejected)
            .map(|x| bincode::serialize(&x).unwrap())
            .for_each(|x| batch.remove(x));
        self.inner.apply_batch(batch)?;
        Ok(())
    }
}

impl Table for TonTree {
    type Key = H256;
    type Value = HashSet<ExtendedEventInfo>;

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
