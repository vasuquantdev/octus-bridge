use std::fs::File;
use std::path::{Path, PathBuf};

use anyhow::Result;
use nekoton_utils::*;
use secstr::SecUtf8;
use serde::{Deserialize, Serialize};

pub use self::eth_config::*;
pub use self::stored_keys::*;

mod eth_config;
mod stored_keys;

#[derive(Serialize, Deserialize)]
pub struct AppConfig {
    pub master_password: SecUtf8,
    pub relay_settings: RelayConfig,
    pub node_settings: ton_indexer::NodeConfig,
    #[serde(default = "default_logger_settings")]
    pub logger_settings: serde_yaml::Value,
}

#[derive(Serialize, Deserialize)]
pub struct BriefAppConfig {
    #[serde(default)]
    pub master_password: Option<SecUtf8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RelayConfig {
    pub keys_path: PathBuf,
    #[serde(with = "serde_address")]
    pub bridge_address: ton_block::MsgAddressInt,
    pub networks: Vec<EthConfig>,
}

impl ConfigExt for AppConfig {
    fn from_file<P>(path: &P) -> Result<Self>
    where
        P: AsRef<Path>,
    {
        let file = File::open(path)?;
        let reader = std::io::BufReader::new(file);
        let config = serde_yaml::from_reader(reader)?;
        Ok(config)
    }
}

impl ConfigExt for ton_indexer::GlobalConfig {
    fn from_file<P>(path: &P) -> Result<Self>
    where
        P: AsRef<Path>,
    {
        let file = std::fs::File::open(path)?;
        let reader = std::io::BufReader::new(file);
        let config = serde_json::from_reader(reader)?;
        Ok(config)
    }
}

pub trait ConfigExt: Sized {
    fn from_file<P>(path: &P) -> Result<Self>
    where
        P: AsRef<Path>;
}

fn default_logger_settings() -> serde_yaml::Value {
    const DEFAULT_LOG4RS_SETTINGS: &str = r##"
    appenders:
      stdout:
        kind: console
        encoder:
          pattern: "{d(%Y-%m-%d %H:%M:%S %Z)(utc)} - {h({l})} {M} = {m} {n}"
    root:
      level: error
      appenders:
        - stdout
    loggers:
      relay:
        level: info
        appenders:
          - stdout
        additive: false
    "##;
    serde_yaml::from_str(DEFAULT_LOG4RS_SETTINGS).unwrap()
}
