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
        let previous_version = get_previous_version(&tree)?;
        let current_version = Version::parse(clap::crate_version!())?;
        if current_version < previous_version {
            return Err(anyhow::anyhow!("You are running relay with build version lower, than recorded in db. Versions are incompatible. Upgrade relay to {} or higher.",previous_version));
        }
        Ok(Self {
            previous_version: get_previous_version(&tree)?,
            tree,
            db: db.clone(),
        })
    }

    pub fn run_migrations(&self) -> Result<(), Error> {
        let version = &self.previous_version;
        let db = &self.db;
        let ton_voting_stats = stats_db::TonVotingStats::new(&db)?;
        let mut ton_breaking_versions = stats_db::TonVotingStats::get_breaking_versions();
        ton_breaking_versions.bulk_upgrade(&version, &ton_voting_stats as &dyn Migration)?;
        // stats_db::EthVotingStats::new(&db),
        // verification_queue::EthVerificationQueue::new(&db)?,
        // votes_queues::EthEventVotesQueue::new(&db?),
        // votes_queues::TonEventVotesQueue::new(&db)?,
        // todo             verification_queue::TonVerificationQueue::new(&db)
        self.update_version()
    }

    fn update_version(&self) -> Result<(), Error> {
        self.tree
            .insert(VERSION_FIELD.as_bytes(), clap::crate_version!())?;
        Ok(())
    }
}

/// Updates data in database in case of schema update.
pub trait Migration {
    fn get_breaking_versions() -> VersionIterator
    where
        Self: Sized;
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
            .try_for_each(|x| migrator.update(&x[0], &x[1]))
    }
}
