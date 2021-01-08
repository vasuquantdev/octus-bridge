use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::Error;
use clap::Clap;
use config::{Config, File, FileFormat};
use sha3::Digest;

use relay_eth::ws::{Address as EthAddr, H256};
#[cfg(feature = "graphql-transport")]
use relay_ton::transport::graphql_transport::Config as TonGraphQLConfig;
#[cfg(feature = "tonlib-transport")]
use relay_ton::transport::tonlib_transport::Config as TonTonlibConfig;

use crate::prelude::*;

mod serde_seconds {
    use super::*;

    pub fn serialize<S>(data: &Duration, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_u64(data.as_secs())
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Duration, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let seconds = u64::deserialize(deserializer)?;
        Ok(Duration::from_secs(seconds))
    }
}

#[derive(Deserialize, Serialize, Clone, Debug, Default, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub struct EthAddress(String);

#[derive(Deserialize, Serialize, Clone, Debug, Default, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub struct TonAddress(pub String);

impl EthAddress {
    pub fn to_eth_addr(&self) -> Result<EthAddr, Error> {
        let bytes = hex::decode(&self.0)?;
        let hash = sha3::Keccak256::digest(&*bytes);
        Ok(EthAddr::from_slice(&*hash))
    }
}

#[derive(Deserialize, Serialize, Clone, Debug, Default, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub struct Method(String);

impl Method {
    pub fn to_topic_hash(&self) -> Result<H256, Error> {
        let bytes = hex::decode(&self.0)?;
        let hash = sha3::Keccak256::digest(&*bytes);
        Ok(H256::from_slice(&*hash))
    }
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct RelayConfig {
    /// Path to json, where ton and eth private keys will be stored in encrypted way.
    pub keys_path: PathBuf,
    /// Listen address of relay. Used by the client to perform all maintenance actions.
    pub listen_address: SocketAddr,
    /// Path to Sled database.
    pub storage_path: PathBuf,
    /// Logger settings
    pub logger_settings: serde_yaml::Value,

    /// Address of ethereum node. Only http is supported right now
    pub eth_node_address: String,

    /// Custom TON key derivation path, used for testing
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ton_derivation_path: Option<String>,
    /// Address of bridge contract
    pub ton_contract_address: TonAddress,
    /// Config for the ton part.
    pub ton_transport: TonConfig,
    /// Config for respawning strategy in ton
    pub ton_settings: TonTimeoutParams,
    /// Number of concurrent tcp connection to ethereum node
    pub number_of_ethereum_tcp_connections: usize,
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct TonTimeoutParams {
    /// Interval between attempts to get event configuration details
    #[serde(with = "serde_seconds")]
    pub event_configuration_details_retry_interval: Duration,
    /// Amount of unsuccessful attempts
    pub event_configuration_details_retry_count: u64,

    /// Interval between attempts to get event details
    #[serde(with = "serde_seconds")]
    pub event_details_retry_interval: Duration,
    /// Amount of unsuccessful attempts
    pub event_details_retry_count: u64,

    /// Interval between attempts to get send message
    #[serde(with = "serde_seconds")]
    pub message_retry_interval: Duration,
    /// Amount of unsuccessful attempts
    pub message_retry_count: i64,
    /// Coefficient, on which every interval will be multiplied
    pub message_retry_interval_multiplier: f64,
}

impl Default for TonTimeoutParams {
    fn default() -> Self {
        Self {
            event_configuration_details_retry_count: 100,
            event_configuration_details_retry_interval: Duration::from_secs(5),
            event_details_retry_interval: Default::default(),
            event_details_retry_count: 100,
            message_retry_interval: Duration::from_secs(60),
            message_retry_count: 10,
            message_retry_interval_multiplier: 1.5,
        }
    }
}

impl Default for RelayConfig {
    fn default() -> Self {
        Self {
            keys_path: PathBuf::from("/var/lib/relay/keys.json"),
            listen_address: "127.0.0.1:12345".parse().unwrap(),
            storage_path: PathBuf::from("/var/lib/relay/persistent_storage"),
            logger_settings: default_logger_settings(),
            eth_node_address: "http://localhost:1234".into(),
            ton_derivation_path: None,
            ton_contract_address: TonAddress("".into()),
            ton_transport: TonConfig::default(),
            ton_settings: TonTimeoutParams::default(),
            number_of_ethereum_tcp_connections: 100,
        }
    }
}

fn default_logger_settings() -> serde_yaml::Value {
    const DEFAULT_LOG4RS_SETTINGS: &str = r##"
    appenders:
      stdout:
        kind: console
        encoder:
          pattern: "{d(%Y-%m-%d %H:%M:%S %Z)(utc)} - {h({l})} {M} {f}:{L} = {m} {n}"
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
      relay_eth:
        level: info
        appenders:
          - stdout
        additive: false
      relay_ton:
        level: info
        appenders:
          - stdout
        additive: false
    "##;
    serde_yaml::from_str(DEFAULT_LOG4RS_SETTINGS).unwrap()
}

#[derive(Deserialize, Serialize, Clone, Debug)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum TonConfig {
    #[cfg(feature = "tonlib-transport")]
    Tonlib(TonTonlibConfig),
    #[cfg(feature = "graphql-transport")]
    GraphQL(TonGraphQLConfig),
}

#[cfg(any(feature = "tonlib-transport", feature = "graphql-transport"))]
impl Default for TonConfig {
    fn default() -> Self {
        #[cfg(feature = "tonlib-transport")]
        return Self::Tonlib(TonTonlibConfig::default());

        #[cfg(all(feature = "graphql-transport", not(feature = "tonlib-transport")))]
        Self::GraphQL(TonGraphQLConfig::default())
    }
}

pub fn read_config(path: PathBuf) -> Result<RelayConfig, Error> {
    let mut config = Config::new();
    config.merge(File::from(path).format(FileFormat::Yaml))?;
    let config: RelayConfig = config.try_into()?;
    dbg!(&config);
    Ok(config)
}

#[derive(Deserialize, Serialize, Clone, Debug, Clap)]
pub struct Arguments {
    /// Path to config
    #[clap(short, long, conflicts_with = "gen-config")]
    pub config: Option<PathBuf>,

    /// Generate default config
    #[clap(long)]
    pub gen_config: Option<PathBuf>,
}

pub fn generate_config<T>(path: T) -> Result<(), Error>
where
    T: AsRef<Path>,
{
    let mut file = std::fs::File::create(path)?;
    let config = RelayConfig::default();
    file.write_all(serde_yaml::to_string(&config)?.as_bytes())?;
    Ok(())
}

pub fn parse_args() -> Arguments {
    dbg!(Arguments::parse())
}
