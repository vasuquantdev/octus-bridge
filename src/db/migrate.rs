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
            .to_vec(),
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
    ///update from `version1` to `version2`
    fn update(&self, version1: &Version, version2: &Version) -> Result<(), Error>;
}

pub struct VersionIterator {
    versions: Vec<Version>,
}

impl VersionIterator {
    pub fn new(versions: &[&str]) -> Result<Self, Error> {
        let versions = versions
            .iter()
            .map(|x| Version::parse(*x))
            .collect::<Result<Vec<Version>, semver::SemVerError>>()?;
        Ok(Self { versions })
    }

    pub fn bulk_upgrade(
        &mut self,
        start_version: &Version,
        migrator: &dyn Migration,
    ) -> Result<(), Error> {
        self.versions
            .windows(2)
            .skip_while(|x| &x[0] < start_version)
            .map(|x| migrator.update(&x[0], &x[1]))
            .collect()
    }
}
