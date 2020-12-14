use anyhow::Error;
use sled::Db;

use relay_eth::ws::H256;

use crate::db_managment::models::EthTonTransaction;

use super::constants::TX_TABLE_TREE_NAME;

pub struct TxTable {
    inner: super::Tree,
}

impl TxTable {
    pub fn new(db: &Db) -> Result<Self, Error> {
        Ok(Self {
            inner: db.open_tree(TX_TABLE_TREE_NAME)?,
        })
    }

    pub fn insert(&self, tx_hash: &H256, data: &EthTonTransaction) -> Result<(), Error> {
        self.inner
            .insert(tx_hash.as_bytes(), bincode::serialize(&data).unwrap())?;
        Ok(())
    }

    pub fn get(&self, tx_hash: &H256) -> Result<Option<EthTonTransaction>, Error> {
        Ok(self
            .inner
            .get(tx_hash.as_bytes())?
            .map(|x| bincode::deserialize::<EthTonTransaction>(&*x).unwrap()))
    }

    pub fn remove(&self, tx_hash: &H256) -> Result<(), Error> {
        self.inner.remove(tx_hash.as_bytes())?;
        Ok(())
    }
}
