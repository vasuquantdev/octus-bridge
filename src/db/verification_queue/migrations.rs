use crate::db::{constants::*, Migration, Version, VersionIterator};
use crate::prelude::*;

use super::{EthVerificationQueue, TonVerificationQueue};

impl Migration for EthVerificationQueue {
    const NAME: &'static str = "EthVerificationQueue";

    fn get_breaking_versions() -> VersionIterator {
        [(1, 0, 1)].into()
    }

    fn update(db: &Db, _: Version, new: Version) -> Result<(), Error> {
        let queue = Self::new(db)?;

        match new {
            (1, 0, 1) => {
                let mut batch = sled::Batch::default();

                for item in queue.db.iter() {
                    let (key, _) = item?;
                    let mut new_key = Vec::with_capacity(key.len() + 1);
                    new_key[0..8].copy_from_slice(&key[0..8]);
                    new_key[9] = 1; // mark all known transactions in queue as external on migration
                    new_key[9..].copy_from_slice(&key[8..]);
                    batch.remove(key);
                    batch.insert(new_key, &[]);
                }

                queue.db.apply_batch(batch)?;
                Ok(())
            }
            _ => Ok(()),
        }
    }
}

impl Migration for TonVerificationQueue {
    const NAME: &'static str = "TonVerificationQueue";

    fn get_breaking_versions() -> VersionIterator {
        [(1, 0, 1)].into()
    }

    fn update(db: &Db, _: Version, new: Version) -> Result<(), Error> {
        let tree = db.open_tree(TON_QUEUE)?;

        match new {
            (1, 0, 1) => {
                let mut batch = sled::Batch::default();

                for item in tree.iter() {
                    let (key, _) = item?;
                    let mut new_key = Vec::with_capacity(key.len() + 1);
                    new_key[0..12].copy_from_slice(&key[0..12]);
                    new_key[13] = 1; // mark all known transactions in queue as external on migration
                    new_key[13..].copy_from_slice(&key[12..]);
                    batch.remove(key);
                    batch.insert(new_key, &[]);
                }

                tree.apply_batch(batch)?;
                Ok(())
            }
            _ => Ok(()),
        }
    }
}
