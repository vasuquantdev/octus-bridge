use anyhow::Error;
use chrono::{DateTime, Utc};
use sled::transaction::{ConflictableTransactionError, ConflictableTransactionResult};
use sled::{Batch, Db, Transactional, Tree};

use relay_eth::ws::H256;

use crate::db_managment::constants::{TX_TABLE_TREE_FAILED_NAME, TX_TABLE_TREE_PENDING_NAME};
use crate::db_managment::EthTonTransaction;

/// Stores sent transactions for our relay
#[derive(Clone)]
pub struct TonQueue {
    pending: Tree,
    failed: Tree,
}

impl TonQueue {
    pub fn new(db: &Db) -> Result<Self, Error> {
        Ok(Self {
            pending: db.open_tree(TX_TABLE_TREE_PENDING_NAME)?,
            failed: db.open_tree(TX_TABLE_TREE_FAILED_NAME)?,
        })
    }

    pub fn insert_pending(&self, tx_hash: &H256, data: &EthTonTransaction) -> Result<(), Error> {
        self.pending
            .insert(tx_hash.as_bytes(), bincode::serialize(&data).unwrap())?;
        Ok(())
    }

    pub fn mark_complete(&self, tx_hash: &H256) -> Result<(), Error> {
        (&self.pending, &self.failed).transaction(|(pending, failed)| {
            match pending.remove(tx_hash.as_bytes())? {
                Some(_) => Ok(()),
                None => {
                    failed.remove(tx_hash.as_bytes())?;
                    ConflictableTransactionResult::<(), std::io::Error>::Ok(())
                }
            }
        })?;

        Ok(())
    }

    pub fn mark_failed(&self, tx_hash: &H256) -> Result<(), Error> {
        (&self.pending, &self.failed)
            .transaction(
                |(pending, failed)| match pending.remove(tx_hash.as_bytes())? {
                    Some(transaction) => {
                        failed.insert(tx_hash.as_bytes(), transaction)?;
                        Ok(())
                    }
                    None => Err(ConflictableTransactionError::Abort(
                        TransactionNotFoundError,
                    )),
                },
            )
            .map_err(Error::from)
    }

    pub fn has_event(&self, tx_hash: &H256) -> Result<bool, Error> {
        Ok(self.pending.contains_key(tx_hash.as_bytes())?
            || self.failed.contains_key(tx_hash.as_bytes())?)
    }

    pub fn get_pending(&self, tx_hash: &H256) -> Result<Option<EthTonTransaction>, Error> {
        Ok(self
            .pending
            .get(tx_hash.as_bytes())?
            .map(|x| bincode::deserialize::<EthTonTransaction>(x.as_ref()).unwrap()))
    }

    pub fn remove_pending(&self, tx_hash: &H256) -> Result<(), Error> {
        self.pending.remove(tx_hash.as_bytes())?;
        Ok(())
    }

    pub fn get_all_pending(&self) -> impl Iterator<Item = (H256, EthTonTransaction)> {
        self.pending.iter().filter_map(|x| x.ok()).map(|x| {
            (
                H256::from_slice(&*x.0),
                bincode::deserialize::<EthTonTransaction>(&x.1).unwrap(),
            )
        })
    }

    pub fn get_all_failed(&self) -> impl Iterator<Item = (H256, EthTonTransaction)> {
        self.failed.iter().filter_map(|x| x.ok()).map(|x| {
            (
                H256::from_slice(&*x.0),
                bincode::deserialize::<EthTonTransaction>(&x.1).unwrap(),
            )
        })
    }

    /// removes all transactions, older than `upper_threshold`
    pub fn remove_failed_older_than(
        &self,
        upper_threshold: &DateTime<Utc>,
    ) -> Result<usize, Error> {
        let mut batch = Batch::default();
        let mut count = 0;
        self.failed
            .iter()
            .filter_map(|x| x.ok())
            .map(|x| {
                (
                    x.0,
                    bincode::deserialize::<EthTonTransaction>(&x.1).unwrap(),
                )
            })
            .filter(|(_, v)| v.get_construction_time() < upper_threshold)
            .for_each(|(k, _)| {
                count += 1;
                batch.remove(k)
            });
        self.failed.apply_batch(batch)?;
        Ok(count)
    }
}

#[derive(Debug, thiserror::Error)]
#[error("cannot mark transaction as failed when it is not pending")]
struct TransactionNotFoundError;
