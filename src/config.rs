use std::io::Write;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::Error;
use clap::Clap;
use config::{Config, File, FileFormat};
use serde::{Deserialize, Serialize};
use sha3::Digest;

use relay_eth::ws::{Address as EthAddr, H256};
#[cfg(feature = "graphql-transport")]
use relay_ton::transport::graphql_transport::Config as TonGraphQLConfig;
#[cfg(feature = "tonlib-transport")]
use relay_ton::transport::tonlib_transport::Config as TonTonlibConfig;

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
    /// path to json, where ton and eth private keys will be stored in encrypted way.
    pub encrypted_data: PathBuf,
    /// Address of ethereum node. Only http is supported right now
    pub eth_node_address: String,
    /// Addresss of bridge contract
    pub ton_contract_address: TonAddress,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ton_derivation_path: Option<String>,
    pub storage_path: PathBuf,
    /// Listen address of relay. Used by the client to perform all maintains actions.
    pub listen_address: SocketAddr,
    /// Config for the ton part.
    pub ton_config: TonConfig,
    /// Config for respawning strategy in ton
    pub ton_operation_timeouts: TonOperationRetryParams,
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct TonOperationRetryParams {
    /// Times to try polling configuration contract
    pub configuration_contract_try_poll_times: u64,
    pub get_event_details_retry_times: u64,
    /// 1 sec ~= time before next block in masterchain.
    /// Should be greater than account polling interval
    #[serde(with = "serde_seconds")]
    pub get_event_details_poll_interval_secs: Duration,
    /// Initial timeout between unsuccessful
    /// broadcast in ton
    #[serde(with = "serde_seconds")]
    pub broadcast_in_ton_interval_secs: Duration,
    /// Times to try broadcasting transaction in ton.
    pub broadcast_in_ton_times: i64,
    /// Coefficient, on which every delay 'll be multiplied
    pub broadcast_in_ton_interval_multiplier: f64,
}
impl Default for TonOperationRetryParams {
    fn default() -> Self {
        Self {
            configuration_contract_try_poll_times: 100,
            get_event_details_retry_times: 100,
            get_event_details_poll_interval_secs: Duration::from_secs(5),
            broadcast_in_ton_interval_secs: Duration::from_secs(60),
            broadcast_in_ton_times: 3,
            broadcast_in_ton_interval_multiplier: 1.5,
        }
    }
}

impl Default for RelayConfig {
    fn default() -> Self {
        Self {
            encrypted_data: PathBuf::from("./cryptodata.json"),
            storage_path: PathBuf::from("./persistent_storage"),
            eth_node_address: "http://localhost:12345".into(),
            ton_contract_address: TonAddress("".into()),
            ton_derivation_path: None,
            listen_address: "127.0.0.1:12345".parse().unwrap(),
            ton_config: TonConfig::default(),
            ton_operation_timeouts: TonOperationRetryParams::default(),
        }
    }
}

#[derive(Deserialize, Serialize, Clone, Debug)]
#[serde(tag = "type")]
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

pub fn read_config(path: &str) -> Result<RelayConfig, Error> {
    let mut config = Config::new();
    config.merge(File::new(path, FileFormat::Json))?;
    let config: RelayConfig = config.try_into()?;
    dbg!(&config);
    Ok(config)
}

#[derive(Deserialize, Serialize, Clone, Debug, Clap)]
pub struct Arguments {
    #[clap(
        short,
        long,
        default_value = "config.json",
        conflicts_with = "gen-config"
    )]
    pub config: String,
    ///It will generate default config
    #[clap(long, requires = "crypto-store-path")]
    pub gen_config: bool,
    #[clap(long)]
    /// Path for encrypted data storage
    pub crypto_store_path: Option<PathBuf>,
    ///Path for generated config
    #[clap(long, default_value = "default_config.json")]
    pub generated_config_path: PathBuf,
}

pub fn generate_config<T>(path: T, pem_path: PathBuf) -> Result<(), Error>
where
    T: AsRef<Path>,
{
    let mut file = std::fs::File::create(path)?;
    let mut config = RelayConfig::default();
    config = RelayConfig {
        encrypted_data: pem_path,
        ..config
    };
    file.write_all(serde_json::to_vec_pretty(&config)?.as_slice())?;
    Ok(())
}

pub fn parse_args() -> Arguments {
    dbg!(Arguments::parse())
}
