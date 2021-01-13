use anyhow::Error;
use serde::{de::DeserializeOwned, Serialize};
use sled::transaction::{ConflictableTransactionError, ConflictableTransactionResult};
use sled::{Db, Transactional, Tree};

use relay_ton::prelude::{MsgAddrStd, UInt256};

use super::constants::*;
use crate::models::*;

pub type TonEventVotesQueue = VotesQueue<TonEventTransaction>;

impl TonEventVotesQueue {
    pub fn new_ton_votes_queue(db: &Db) -> Result<Self, Error> {
        Ok(Self {
            pending: db.open_tree(TON_EVENTS_QUEUE_PENDING)?,
            failed: db.open_tree(TON_EVENTS_QUEUE_FAILED)?,
            _marker: Default::default(),
        })
    }
}

pub type EthEventVotesQueue = VotesQueue<EthEventTransaction>;

impl EthEventVotesQueue {
    pub fn new_eth_votes_queue(db: &Db) -> Result<Self, Error> {
        Ok(Self {
            pending: db.open_tree(ETH_EVENTS_QUEUE_PENDING)?,
            failed: db.open_tree(ETH_EVENTS_QUEUE_FAILED)?,
            _marker: Default::default(),
        })
    }
}

#[derive(Clone)]
pub struct VotesQueue<T> {
    pending: Tree,
    failed: Tree,
    _marker: std::marker::PhantomData<T>,
}

impl<T> VotesQueue<T> {
    fn new(db: &Db, pending: &str, failed: &str) -> Result<Self, Error> {
        Ok(Self {
            pending: db.open_tree(pending)?,
            failed: db.open_tree(failed)?,
            _marker: Default::default(),
        })
    }
}

impl<T> VotesQueue<T>
where
    T: Serialize + DeserializeOwned,
{
    pub fn insert_pending(&self, event_address: &MsgAddrStd, data: &T) -> Result<(), Error> {
        let key = make_key(event_address);

        (&self.pending, &self.failed).transaction(|(pending, failed)| {
            failed.remove(key.clone())?;
            pending.insert(
                key.clone(),
                bincode::serialize(data).expect("Shouldn't fail"),
            )?;
            ConflictableTransactionResult::<(), std::io::Error>::Ok(())
        })?;

        #[cfg(feature = "paranoid")]
        {
            self.failed.flush()?;
            self.pending.flush()?;
        }

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

    pub fn get_all_pending(&self) -> impl Iterator<Item = (MsgAddrStd, T)> {
        self.pending
            .iter()
            .filter_map(|x| match x {
                Ok(a) => Some(a),
                Err(e) => {
                    log::error!("Failed getting pending from db. Db corruption?: {}", e);
                    None
                }
            })
            .map(|(key, value)| {
                (
                    parse_key(&key),
                    bincode::deserialize::<T>(&value).expect("Shouldn't fail"),
                )
            })
    }

    pub fn get_all_failed(&self) -> impl Iterator<Item = (MsgAddrStd, T)> {
        self.failed
            .iter()
            .filter_map(|x| match x {
                Ok(a) => Some(a),
                Err(e) => {
                    log::error!("Failed getting failed from db. Db corruption?: {}", e);
                    None
                }
            })
            .map(|(key, value)| {
                (
                    parse_key(&key),
                    bincode::deserialize::<T>(&value).expect("Shouldn't fail"),
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
        address: UInt256::from(key).into(),
    }
}
