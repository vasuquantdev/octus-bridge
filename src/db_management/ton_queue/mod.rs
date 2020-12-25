use anyhow::Error;
use sled::transaction::{ConflictableTransactionError, ConflictableTransactionResult};
use sled::{Db, Transactional, Tree};

use relay_ton::prelude::{MsgAddrStd, UInt256};

use crate::db_management::constants::{TX_TABLE_TREE_FAILED_NAME, TX_TABLE_TREE_PENDING_NAME};
use crate::db_management::EthTonTransaction;

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

    pub fn insert_pending(
        &self,
        event_address: &MsgAddrStd,
        data: &EthTonTransaction,
    ) -> Result<(), Error> {
        let key = make_key(event_address);

        (&self.pending, &self.failed).transaction(|(pending, failed)| {
            failed.remove(key.clone())?;
            pending.insert(key.clone(), bincode::serialize(data).unwrap())?;
            ConflictableTransactionResult::<(), std::io::Error>::Ok(())
        })?;

        Ok(())
    }

    pub fn mark_complete(&self, event_address: &MsgAddrStd) -> Result<(), Error> {
        let key = make_key(event_address);

        (&self.pending, &self.failed).transaction(|(pending, failed)| {
            pending.remove(key.clone())?;
            failed.remove(key.clone())?;
            ConflictableTransactionResult::<(), std::io::Error>::Ok(())
        })?;

        Ok(())
    }

    pub fn mark_failed(&self, event_address: &MsgAddrStd) -> Result<(), Error> {
        let key = make_key(event_address);

        (&self.pending, &self.failed)
            .transaction(|(pending, failed)| match pending.remove(key.clone())? {
                Some(transaction) => {
                    failed.insert(key.clone(), transaction)?;
                    Ok(())
                }
                None => Err(ConflictableTransactionError::Abort(
                    TransactionNotFoundError,
                )),
            })
            .map_err(Error::from)
    }

    pub fn has_event(&self, event_address: &MsgAddrStd) -> Result<bool, Error> {
        let key = make_key(event_address);

        Ok(self.pending.contains_key(&key)? || self.failed.contains_key(&key)?)
    }

    pub fn get_all_pending(&self) -> impl Iterator<Item = (MsgAddrStd, EthTonTransaction)> {
        self.pending
            .iter()
            .filter_map(|x| x.ok())
            .map(|(key, value)| {
                (
                    parse_key(&key),
                    bincode::deserialize::<EthTonTransaction>(&value).unwrap(),
                )
            })
    }

    pub fn get_all_failed(&self) -> impl Iterator<Item = (MsgAddrStd, EthTonTransaction)> {
        self.failed
            .iter()
            .filter_map(|x| x.ok())
            .map(|(key, value)| {
                (
                    parse_key(&key),
                    bincode::deserialize::<EthTonTransaction>(&value).unwrap(),
                )
            })
    }
}

#[derive(Debug, thiserror::Error)]
#[error("cannot mark transaction as failed when it is not pending")]
struct TransactionNotFoundError;

fn make_key(event_address: &MsgAddrStd) -> Vec<u8> {
    event_address.address.get_bytestring(0)
}

fn parse_key(key: &[u8]) -> MsgAddrStd {
    MsgAddrStd {
        anycast: None,
        workchain_id: 0,
        address: UInt256::from(&key[32..]).into(),
    }
}
