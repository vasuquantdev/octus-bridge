use anyhow::Error;
use semver::Version;
use sled::{Db, IVec, Tree};

use super::{constants::SYSTEM_DATA, stats_db, verification_queue, votes_queues};

const VERSION_FIELD: &str = "version";

pub struct Migrator {
    tree: Tree,
    db: Db,
    previous_version: Version,
}

fn get_previous_version(tree: &Tree) -> Result<Version, Error> {
    let version = String::from_utf8(
        tree.get(VERSION_FIELD.as_bytes())?
            .unwrap_or_else(|| IVec::from(clap::crate_version!()))
            .into_vec(),
    )?;
    Ok(Version::parse(&version)?)
}

impl Migrator {
    pub fn init(db: &Db) -> Result<Self, Error> {
        let tree = db.open_tree(SYSTEM_DATA)?;
        Ok(Self {
            previous_version: get_previous_version(&tree)?,
            tree,
            db: db.clone(),
        })
    }
    pub fn run_migrations(&self) -> Result<(), Error> {
        const APP_VERSION: &str = clap::crate_version!();
        let version = Version::parse(&APP_VERSION)?;
        //todo             verification_queue::TonVerificationQueue::new(&db)
        let db = &self.db;
        stats_db::TonVotingStats::new(&db)?;
        // stats_db::EthVotingStats::new(&db),
        // verification_queue::EthVerificationQueue::new(&db)?,
        // votes_queues::EthEventVotesQueue::new(&db?),
        // votes_queues::TonEventVotesQueue::new(&db)?,
        todo!()
    }
}

/// Updates data in database in case of schema update.
pub trait Migration {
    fn update(&self, version: Version) -> Result<(), Error>;
}
