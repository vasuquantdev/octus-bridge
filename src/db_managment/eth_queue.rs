use std::collections::HashSet;

use crate::db_managment::models::EthTonConfirmationData;
use crate::db_managment::Table;

use super::prelude::*;
use relay_ton::prelude::HashMap;

#[derive(Clone)]
pub struct EthQueue {
    db: Tree,
}

impl EthQueue {
    pub fn new(db: &Db) -> Result<Self, Error> {
        Ok(Self {
            db: db.open_tree(super::constants::ETH_QUEUE_TREE_NAME)?,
        })
    }

    pub fn get(&self, key: u64) -> Result<Option<HashSet<EthTonConfirmationData>>, Error> {
        match self.db.get(key.to_be_bytes())? {
            Some(data) => Ok(Some(bincode::deserialize(&data)?)),
            None => Ok(None),
        }
    }

    pub fn remove(&self, key: u64) -> Result<(), Error> {
        self.db.remove(key.to_be_bytes())?;
        Ok(())
    }

    pub fn insert(&self, key: &u64, value: &HashSet<EthTonConfirmationData>) -> Result<(), Error> {
        self.db
            .insert(key.to_be_bytes(), bincode::serialize(&value).unwrap())?;
        Ok(())
    }
}

impl Table for EthQueue {
    type Key = u64;
    type Value = HashSet<EthTonConfirmationData>;

    fn dump_elements(&self) -> HashMap<Self::Key, Self::Value> {
        self.db
            .iter()
            .filter_map(|x| x.ok())
            .map(|(k, v)| {
                let mut key = [0; 8];
                key.copy_from_slice(&k);
                (
                    u64::from_be_bytes(key),
                    bincode::deserialize(&v).expect("Shouldn't fail"),
                )
            })
            .collect()
    }
}
