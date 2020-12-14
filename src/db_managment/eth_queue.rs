use std::collections::HashSet;

use tokio::sync::Mutex;

use relay_ton::prelude::HashMap;

use crate::db_managment::models::EthTonConfirmationData;
use crate::db_managment::Table;

use super::prelude::*;

#[derive(Clone)]
pub struct EthQueue {
    db: Tree,
    table_lock: Arc<Mutex<()>>,
}

impl EthQueue {
    pub fn new(db: &Db) -> Result<Self, Error> {
        Ok(Self {
            db: db.open_tree(super::constants::ETH_QUEUE_TREE_NAME)?,
            table_lock: Arc::new(Mutex::new(())),
        })
    }

    pub async fn get(&self, key: u64) -> Result<Option<HashSet<EthTonConfirmationData>>, Error> {
        let guard = self.table_lock.lock().await;
        match self.db.get(key.to_be_bytes())? {
            Some(data) => Ok(Some(bincode::deserialize(&data)?)),
            None => Ok(None),
        }
    }

    pub async fn remove(&self, key: u64) -> Result<(), Error> {
        let guard = self.table_lock.lock().await;
        self.db.remove(key.to_be_bytes())?;
        Ok(())
    }

    pub async fn insert(
        &self,
        key: &u64,
        value: &HashSet<EthTonConfirmationData>,
    ) -> Result<(), Error> {
        let guard = self.table_lock.lock().await;
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
