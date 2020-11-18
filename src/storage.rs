use std::path::Path;

use anyhow::Error;
use sled::{Db, IVec};

#[derive(Clone, Debug)]
pub struct PersistentStateManager {
    inner: Db,
}

impl PersistentStateManager {
    pub fn new<T>(p: T) -> Result<Self, Error>
    where
        T: AsRef<Path>,
    {
        Ok(Self {
            inner: sled::open(p)?,
        })
    }
    pub fn flush_state(&self) -> Result<usize, Error> {
        Ok(self.inner.flush()?)
    }
    pub fn update_state<K, V>(&self, key: K, value: V) -> Result<Option<IVec>, Error>
    where
        K: AsRef<[u8]>,
        V: Into<IVec>,
    {
        Ok(self.inner.insert(key, value)?)
    }
    pub fn get<K: AsRef<[u8]>>(&self, key: K) -> Result<Option<IVec>, Error> {
        Ok(self.inner.get(&key)?)
    }
    pub fn remove<K: AsRef<[u8]>>(&self, key: K) -> Result<Option<IVec>, Error> {
        Ok(self.inner.remove(&key)?)
    }
}
