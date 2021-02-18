use anyhow::{anyhow, Error};
use sled::{Db, Tree};

use super::{constants::SYSTEM_DATA, verification_queue};

const VERSION_FIELD: &str = "version";

pub struct Migrator {
    versions: Tree,
    db: Db,
}

impl Migrator {
    pub fn init(db: &Db) -> Result<Self, Error> {
        Ok(Self {
            versions: db.open_tree(&format!("{}{}", SYSTEM_DATA, VERSION_FIELD))?,
            db: db.clone(),
        })
    }

    pub fn run_migrations(&self) -> Result<(), Error> {
        log::warn!("Applying migrations");
        self.run_migration::<verification_queue::EthVerificationQueue>()?;
        self.run_migration::<verification_queue::TonVerificationQueue>()?;
        Ok(())
    }

    fn run_migration<T>(&self) -> Result<(), Error>
    where
        T: Migration,
    {
        let previous_version = semver::Version::from_tuple(self.get_previous_version(T::NAME)?);
        let current_version = semver::Version::parse(clap::crate_version!())?;

        if current_version < previous_version {
            return Err(anyhow!("You are running relay with build version lower, than recorded in db. Versions are incompatible. Upgrade relay to {} or higher.", previous_version));
        }

        let versions = T::get_breaking_versions().0;

        match versions.last() {
            Some(last) if last != &previous_version => match versions.len() {
                1 => {
                    migrate::<T>(&self.db, &previous_version, last)?;
                    self.update_version(T::NAME, last)?;
                }
                _ => {
                    for x in versions.windows(2).skip_while(|x| x[0] < previous_version) {
                        migrate::<T>(&self.db, &x[0], &x[1])?;
                        self.update_version(T::NAME, &x[1])?;
                    }
                }
            },
            _ => {}
        }
        Ok(())
    }

    fn get_previous_version(&self, entity: &str) -> Result<Version, Error> {
        let value = match self.versions.get(&entity)? {
            Some(value) if value.len() == 24 => value,
            Some(_) => return Err(anyhow!("Invalid version stored")),
            None => return Ok(Version::default()),
        };
        let mut buffer = [0; 8];

        buffer.copy_from_slice(&value[0..8]);
        let major = u64::from_le_bytes(buffer);

        buffer.copy_from_slice(&value[8..16]);
        let minor = u64::from_le_bytes(buffer);

        buffer.copy_from_slice(&value[16..24]);
        let patch = u64::from_le_bytes(buffer);

        Ok((major, minor, patch))
    }

    fn update_version(&self, entity: &str, version: &semver::Version) -> Result<(), Error> {
        let mut value = [0; 24];
        value[0..8].copy_from_slice(&version.major.to_le_bytes());
        value[8..16].copy_from_slice(&version.minor.to_le_bytes());
        value[16..24].copy_from_slice(&version.patch.to_le_bytes());
        self.versions.insert(entity.as_bytes(), &value)?;
        Ok(())
    }
}

pub type Version = (u64, u64, u64);

trait VersionExt: Sized {
    fn from_tuple(version: Version) -> Self;
    fn as_tuple(&self) -> Version;
}

impl VersionExt for semver::Version {
    fn from_tuple((major, minor, patch): Version) -> Self {
        Self::new(major, minor, patch)
    }

    fn as_tuple(&self) -> (u64, u64, u64) {
        (self.major, self.minor, self.patch)
    }
}

/// Updates data in database in case of schema update.
pub trait Migration: Sized {
    const NAME: &'static str;

    fn get_breaking_versions() -> VersionIterator;

    /// update from `old` to `version2`
    fn update(db: &Db, old: Version, new: Version) -> Result<(), Error>;
}

pub struct VersionIterator(Vec<semver::Version>);

impl<T> From<T> for VersionIterator
where
    T: AsRef<[(u64, u64, u64)]>,
{
    fn from(versions: T) -> Self {
        Self(
            versions
                .as_ref()
                .iter()
                .map(|&(major, minor, patch)| semver::Version::new(major, minor, patch))
                .collect(),
        )
    }
}

fn migrate<T: Migration>(
    db: &Db,
    old: &semver::Version,
    new: &semver::Version,
) -> Result<(), Error> {
    assert_ne!(old, new);

    log::warn!("Applying migration for {} from {} to {}", T::NAME, old, new);

    match T::update(db, old.as_tuple(), new.as_tuple()) {
        Ok(_) => {
            log::warn!("Successfully migrated {} from {} to {}", T::NAME, old, new);
            Ok(())
        }
        Err(e) => {
            log::warn!("Error while migrating {} from {} to {}", T::NAME, old, new);
            Err(e)
        }
    }
}
