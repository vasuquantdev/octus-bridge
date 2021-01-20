use std::collections::HashMap;

use borsh::{BorshDeserialize, BorshSerialize};
use tokio::sync::{Mutex, MutexGuard};

use relay_models::models::EthEventVoteDataView;
use relay_ton::contracts::EthEventVoteData;

use crate::models::*;

use super::prelude::*;

const RANGE_LOWER_BOUND: [u8; 8] = [0; 8];

#[derive(Clone)]
pub struct EthQueue {
    db: Tree,
    guard: Arc<Mutex<()>>,
}

impl EthQueue {
    pub fn new(db: &Db) -> Result<Self, Error> {
        Ok(Self {
            db: db.open_tree(super::constants::ETH_QUEUE)?,
            guard: Arc::new(Default::default()),
        })
    }

    pub async fn get_prepared_blocks(
        &self,
        block_number: u64,
    ) -> EthQueueLock<'_, impl Iterator<Item = (sled::IVec, EthEventVoteData)>> {
        let guard = self.guard.lock().await;

        let results = self
            .db
            .range(RANGE_LOWER_BOUND..=((block_number + 1).to_be_bytes()))
            .keys()
            .filter_map(|key| {
                let key = key.ok()?;
                let value = BorshDeserialize::deserialize(&mut &key[8..]).ok()?;
                Some((key, value))
            });

        EthQueueLock {
            results,
            queue: self,
            _guard: guard,
        }
    }

    pub async fn get_bad_blocks(
        &self,
        block_number: u64,
    ) -> EthQueueLock<'_, impl Iterator<Item = (sled::IVec, EthEventVoteData)>> {
        let guard = self.guard.lock().await;
        let results = self
            .db
            .range(block_number.to_be_bytes()..)
            .keys()
            .filter_map(|key| {
                let key = key.ok()?;
                let value = BorshDeserialize::deserialize(&mut &key[8..]).ok()?;
                Some((key, value))
            });

        EthQueueLock {
            results,
            queue: self,
            _guard: guard,
        }
    }

    pub async fn insert(
        &self,
        target_block_number: u64,
        value: &EthEventVoteData,
    ) -> Result<(), Error> {
        let _guard = self.guard.lock().await;
        self.db.insert(make_key(target_block_number, value), &[])?;
        #[cfg(feature = "paranoid")]
        self.db.flush()?;
        Ok(())
    }
}

#[inline]
fn make_key(target_block_number: u64, value: &EthEventVoteData) -> Vec<u8> {
    let value = value.try_to_vec().expect("Shouldn't fail");

    let mut key = target_block_number.to_be_bytes().to_vec();
    key.extend_from_slice(&value);

    key
}

impl Table for EthQueue {
    type Key = u64;
    type Value = Vec<EthEventVoteDataView>;

    fn dump_elements(&self) -> HashMap<Self::Key, Self::Value> {
        self.db
            .iter()
            .filter_map(|x| match x {
                Ok(a) => Some(a),
                Err(e) => {
                    log::error!("Failed getting stats from db. Db corruption?: {}", e);
                    None
                }
            })
            .fold(HashMap::new(), |mut result, (k, _)| {
                let mut block_number = [0; 8];
                block_number.copy_from_slice(&k[0..8]);
                let block_number = u64::from_be_bytes(block_number);

                let value: EthEventVoteData =
                    BorshDeserialize::deserialize(&mut &k[8..]).expect("Shouldn't fail");

                result
                    .entry(block_number)
                    .or_insert_with(Vec::new)
                    .push(value.into_view());
                result
            })
    }
}

pub struct EthQueueLock<'a, I> {
    results: I,
    queue: &'a EthQueue,
    _guard: MutexGuard<'a, ()>,
}

impl<'a, I> Iterator for EthQueueLock<'a, I>
where
    I: Iterator<Item = (sled::IVec, EthEventVoteData)>,
{
    type Item = (EthQueueLockEntry<'a>, EthEventVoteData);

    fn next(&mut self) -> Option<Self::Item> {
        self.results.next().map(|(key, value)| {
            (
                EthQueueLockEntry {
                    key,
                    queue: self.queue,
                },
                value,
            )
        })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.results.size_hint()
    }
}

pub struct EthQueueLockEntry<'a> {
    key: sled::IVec,
    queue: &'a EthQueue,
}

impl<'a> EthQueueLockEntry<'a> {
    pub fn remove(self) -> Result<(), Error> {
        self.queue.db.remove(self.key)?;
        #[cfg(feature = "paranoid")]
        self.queue.db.flush()?;
        Ok(())
    }
}
