use anyhow::Error;
use sled::transaction::{
    ConflictableTransactionError, ConflictableTransactionResult, TransactionError,
    TransactionResult,
};
use sled::{Db, Transactional, Tree};

use anyhow::anyhow;
use relay_eth::ws::H256;

use crate::db_managment::constants::{TX_TABLE_TREE_FAILED_NAME, TX_TABLE_TREE_PENDING_NAME};
use crate::db_managment::{EthTonTransaction, Table};

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
}

#[derive(Debug, thiserror::Error)]
#[error("cannot mark transaction as failed when it is not pending")]
struct TransactionNotFoundError;
