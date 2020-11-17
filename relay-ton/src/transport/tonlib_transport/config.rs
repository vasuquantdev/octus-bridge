use tokio::time::Duration;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone)]
pub struct Config {
    pub network_config: String,
    pub network_name: String,
    pub verbosity: u8,
    pub keystore: KeystoreType,
    pub last_block_threshold_sec: u64,
}

impl From<Config> for tonlib::Config {
    fn from(c: Config) -> Self {
        Self {
            network_config: c.network_config,
            network_name: c.network_name,
            verbosity: c.verbosity,
            keystore: c.keystore.into(),
            last_block_threshold: Duration::from_secs(c.last_block_threshold_sec),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub enum KeystoreType {
    InMemory,
    FileSystem { root_dir: String },
}

impl From<KeystoreType> for tonlib::KeystoreType {
    fn from(t: KeystoreType) -> Self {
        match t {
            KeystoreType::InMemory => Self::InMemory,
            KeystoreType::FileSystem(root_dir) => Self::FileSystem(root_dir),
        }
    }
}
