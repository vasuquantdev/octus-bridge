use std::fs::File;
use std::path::Path;

use anyhow::Result;
use nekoton_utils::*;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct RelayConfig {
    #[serde(with = "serde_address")]
    pub relay_address: ton_block::MsgAddressInt,

    pub indexer: ton_indexer::NodeConfig,

    #[serde(default = "default_logger_settings")]
    pub logger_settings: serde_yaml::Value,
}

impl ConfigExt for RelayConfig {
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
