use std::net::{Ipv4Addr, SocketAddrV4};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use nekoton_utils::*;
use secstr::SecUtf8;
use serde::{Deserialize, Serialize};

pub use self::eth_config::*;
pub use self::stored_keys::*;
use self::temp_keys::*;

mod eth_config;
mod stored_keys;
mod temp_keys;

#[derive(Serialize, Deserialize)]
pub struct AppConfig {
    pub master_password: SecUtf8,
    #[serde(with = "serde_address")]
    pub staker_address: ton_block::MsgAddressInt,
    pub relay_settings: RelayConfig,
    #[serde(default)]
    pub node_settings: NodeConfig,
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

#[derive(Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct NodeConfig {
    pub adnl_public_ip: Option<Ipv4Addr>,
    pub adnl_port: u16,
    pub db_path: PathBuf,
    pub temp_keys_path: PathBuf,
    pub max_db_memory_usage: usize,
}

impl NodeConfig {
    pub async fn build_indexer_config(self) -> Result<ton_indexer::NodeConfig> {
        let ip_address = match self.adnl_public_ip {
            Some(address) => address,
            None => public_ip::addr_v4()
                .await
                .ok_or(ConfigError::PublicIpNotFound)?,
        };
        log::info!("Using public ip: {}", ip_address);

        // TODO: add param to generate new temp keys
        let temp_keys =
            TempKeys::load(self.temp_keys_path, false).context("Failed to load temp keys")?;

        std::fs::create_dir_all(&self.db_path)?;

        Ok(ton_indexer::NodeConfig {
            ip_address: SocketAddrV4::new(ip_address, self.adnl_port),
            adnl_keys: temp_keys.into(),
            rocks_db_path: self.db_path.join("rocksdb"),
            file_db_path: self.db_path.join("files"),
            old_blocks_policy: Default::default(),
            shard_state_cache_enabled: false,
            max_db_memory_usage: self.max_db_memory_usage,
            adnl_options: Default::default(),
            rldp_options: Default::default(),
            dht_options: Default::default(),
            neighbours_options: Default::default(),
            overlay_shard_options: Default::default(),
        })
    }
}

impl Default for NodeConfig {
    fn default() -> Self {
        Self {
            adnl_public_ip: None,
            adnl_port: 30303,
            db_path: "db".into(),
            temp_keys_path: "adnl-keys.json".into(),
            max_db_memory_usage: ton_indexer::default_max_db_memory_usage(),
        }
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

#[derive(thiserror::Error, Debug)]
enum ConfigError {
    #[error("Failed to find public ip")]
    PublicIpNotFound,
}
