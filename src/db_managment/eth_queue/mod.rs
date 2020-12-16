use std::collections::{HashMap, HashSet};

use relay_eth::ws::H256;
use sled::Batch;
use tokio::sync::{Mutex, MutexGuard};

use crate::db_managment::models::EthTonConfirmationData;
use crate::db_managment::Table;

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
            db: db.open_tree(super::constants::ETH_QUEUE_TREE_NAME)?,
            guard: Arc::new(Default::default()),
        })
    }

    pub async fn get_prepared_blocks(
        &self,
        block_number: u64,
    ) -> EthQueueLock<'_, impl Iterator<Item = (sled::IVec, EthTonConfirmationData)>> {
        let guard = self.guard.lock().await;

        let results = self
            .db
            .range(RANGE_LOWER_BOUND..=(block_number.to_be_bytes()))
            .keys()
            .filter_map(|key| {
                let key = key.ok()?;
                let value = bincode::deserialize(&key[8..]).ok()?;
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
        value: &EthTonConfirmationData,
    ) -> Result<(), Error> {
        let _guard = self.guard.lock().await;
        self.db.insert(make_key(target_block_number, value), &[])?;
        Ok(())
    }
}

#[inline]
fn make_key(target_block_number: u64, value: &EthTonConfirmationData) -> Vec<u8> {
    let value = bincode::serialize(value).unwrap();

    let mut key = target_block_number.to_be_bytes().to_vec();
    key.extend_from_slice(&value);

    key
}

impl Table for EthQueue {
    type Key = u64;
    type Value = HashMap<H256, EthTonConfirmationData>;

    fn dump_elements(&self) -> HashMap<Self::Key, Self::Value> {
        self.db
            .iter()
            .filter_map(|x| x.ok())
            .fold(HashMap::new(), |mut result, (k, v)| {
                let mut block_number = [0; 8];
                block_number.copy_from_slice(&k[0..8]);

                let mut item = result
                    .entry(u64::from_be_bytes(block_number))
                    .or_insert(HashMap::new());

                let _ = item
                    .entry(H256::from_slice(&k[8..40]))
                    .or_insert(bincode::deserialize(&v).expect("Shouldn't fail"));

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
    I: Iterator<Item = (sled::IVec, EthTonConfirmationData)>,
{
    type Item = (EthQueueLockEntry<'a>, EthTonConfirmationData);

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
        Ok(())
    }
}
