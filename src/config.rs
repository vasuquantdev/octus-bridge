use anyhow::Error;
use config::{Config, File, FileFormat};
use relay_eth::ws::{Address as EthAddr, H256};
use serde::{Deserialize, Serialize};
use sha3::Digest;
use std::collections::HashMap;
use std::io::Write;
use std::path::{Path, PathBuf};
use structopt::StructOpt;
use url::Url;

#[derive(Deserialize, Serialize, Clone, Debug, Default, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub struct Address(String);

impl Address {
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

#[derive(Deserialize, Serialize, Clone, Debug, Default)]
pub struct RelayConfig {
    pub pub_key_location: PathBuf,
    pub private_key_location: PathBuf,
    pub eth_node_address: String,
    pub contract_mapping: HashMap<Address, Method>,
}

pub fn read_config(path: &str) -> Result<RelayConfig, Error> {
    let mut config = Config::new();
    config.merge(File::new(path, FileFormat::Json))?;
    let config: RelayConfig = config.try_into()?;
    Ok(config)
}

#[derive(Deserialize, Serialize, Clone, Debug, StructOpt)]
pub struct Arguments {
    #[structopt(required_if("gen_config", "false"))]
    pub config: Option<String>,
    #[structopt(long("--gen_config"))]
    pub gen_config: bool,
}

pub fn generate_config<T>(path: T) -> Result<(), Error>
where
    T: AsRef<Path>,
{
    let mut file = std::fs::File::create(path)?;
    file.write_all(serde_json::to_vec_pretty(&RelayConfig::default())?.as_slice())?;
    Ok(())
}

pub fn parse_args() -> Arguments {
    Arguments::from_args()
}
