use relay_ton::contracts::EthEventVoteData;

use super::constants::*;
use super::Table;
use crate::models::*;
use crate::prelude::*;

const RANGE_LOWER_BOUND: [u8; 8] = [0; 8];

pub type EthVerificationQueue = VerificationQueue<EthEventVoteData>;
pub type TonVerificationQueue = VerificationQueue<SignedTonEventVoteData>;

impl EthVerificationQueue {
    pub fn new(db: &Db) -> Result<Self, Error> {
        Ok(Self {
            db: db.open_tree(ETH_QUEUE)?,
            guard: Arc::new(Default::default()),
            _marker: Default::default(),
        })
    }
}

impl TonVerificationQueue {
    pub fn new(db: &Db, configuration_id: u32) -> Result<Self, Error> {
        const COMMON_PREFIX_LEN: usize = TON_QUEUE.as_bytes().len();

        let mut prefix = [0u8; COMMON_PREFIX_LEN + std::mem::size_of::<u32>()];
        prefix[..COMMON_PREFIX_LEN].copy_from_slice(&TON_QUEUE.as_bytes());
        prefix[COMMON_PREFIX_LEN..].copy_from_slice(&configuration_id.to_le_bytes());

        Ok(Self {
            db: db.open_tree(prefix)?,
            guard: Arc::new(Default::default()),
            _marker: Default::default(),
        })
    }
}

#[derive(Clone)]
pub struct VerificationQueue<T> {
    db: Tree,
    guard: Arc<Mutex<()>>,
    _marker: std::marker::PhantomData<T>,
}

impl<T> VerificationQueue<T>
where
    T: BorshDeserialize + BorshSerialize,
{
    pub async fn range_before(
        &self,
        key: u64,
    ) -> VerificationQueueLock<'_, impl Iterator<Item = (sled::IVec, T)>, T> {
        let guard = self.guard.lock().await;
        let results = self
            .db
            .range(RANGE_LOWER_BOUND..=((key + 1).to_be_bytes()))
            .keys()
            .filter_map(|key| {
                let key = key.ok()?;
                let value: T = BorshDeserialize::deserialize(&mut &key[8..]).ok()?;
                Some((key, value))
            });

        VerificationQueueLock {
            results,
            queue: self,
            _guard: guard,
        }
    }

    pub async fn range_after(
        &self,
        key: u64,
    ) -> VerificationQueueLock<'_, impl Iterator<Item = (sled::IVec, T)>, T> {
        let guard = self.guard.lock().await;
        let results = self.db.range(key.to_be_bytes()..).keys().filter_map(|key| {
            let key = key.ok()?;
            let value: T = BorshDeserialize::deserialize(&mut &key[8..]).ok()?;
            Some((key, value))
        });

        VerificationQueueLock {
            results,
            queue: self,
            _guard: guard,
        }
    }

    pub async fn insert(&self, target_key: u64, value: &T) -> Result<(), Error> {
        let _guard = self.guard.lock().await;
        self.db.insert(make_key(target_key, value), &[])?;
        #[cfg(feature = "paranoid")]
        self.db.flush()?;
        Ok(())
    }
}

#[inline]
fn make_key<T>(target_key: u64, value: &T) -> Vec<u8>
where
    T: BorshSerialize,
{
    let value = value.try_to_vec().expect("Shouldn't fail");

    let mut key = target_key.to_be_bytes().to_vec();
    key.extend_from_slice(&value);

    key
}

impl<T> Table for VerificationQueue<T>
where
    T: BorshDeserialize + IntoView,
{
    type Key = u64;
    type Value = Vec<T::View>;

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

                let value: T = BorshDeserialize::deserialize(&mut &k[8..]).expect("Shouldn't fail");

                result
                    .entry(block_number)
                    .or_insert_with(Vec::new)
                    .push(value.into_view());
                result
            })
    }
}

pub struct VerificationQueueLock<'a, I, T> {
    results: I,
    queue: &'a VerificationQueue<T>,
    _guard: MutexGuard<'a, ()>,
}

impl<'a, I, T> Iterator for VerificationQueueLock<'a, I, T>
where
    I: Iterator<Item = (sled::IVec, T)>,
    T: BorshSerialize + BorshDeserialize,
{
    type Item = (VerificationQueueLockEntry<'a, T>, T);

    fn next(&mut self) -> Option<Self::Item> {
        self.results.next().map(|(key, value)| {
            (
                VerificationQueueLockEntry {
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

pub struct VerificationQueueLockEntry<'a, T> {
    key: sled::IVec,
    queue: &'a VerificationQueue<T>,
}

impl<'a, T> VerificationQueueLockEntry<'a, T> {
    pub fn remove(self) -> Result<(), Error> {
        self.queue.db.remove(self.key)?;
        #[cfg(feature = "paranoid")]
        self.queue.db.flush()?;
        Ok(())
    }
}
