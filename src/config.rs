use std::io::Write;
use std::path::{Path, PathBuf};

use anyhow::Error;
use config::{Config, File, FileFormat};
use serde::{Deserialize, Serialize};
use sha3::Digest;
use structopt::StructOpt;

use relay_eth::ws::{Address as EthAddr, H256};

#[derive(Deserialize, Serialize, Clone, Debug, Default, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub struct EthAddress(String);

#[derive(Deserialize, Serialize, Clone, Debug, Default, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub struct TonAddress(String);

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

#[derive(Deserialize, Serialize, Clone, Debug, Default)]
pub struct RelayConfig {
    pub pem_location: PathBuf,
    pub eth_node_address: String,
    pub ton_contract_address: TonAddress,
    pub storage_path: PathBuf,
    // pub contract_mapping: HashMap<Address, Method>,
}

pub fn read_config(path: &str) -> Result<RelayConfig, Error> {
    let mut config = Config::new();
    config.merge(File::new(path, FileFormat::Json))?;
    let config: RelayConfig = config.try_into()?;
    Ok(config)
}

#[derive(Deserialize, Serialize, Clone, Debug, StructOpt)]
pub struct Arguments {
    #[structopt(short, long, default_value = "config.json")]
    pub config: String,
    #[structopt(long, help = "It will generate default config.")]
    pub gen_config: bool,
    #[structopt(
        long,
        short,
        requires_if("gen_config", "true"),
        help = "Path for encrypted data storage"
    )]
    pub crypto_store_path: Option<PathBuf>,
}

pub fn generate_config<T>(path: T, pem_path: PathBuf) -> Result<(), Error>
where
    T: AsRef<Path>,
{
    let mut file = std::fs::File::create(path)?;
    let mut config = RelayConfig::default();
    config = RelayConfig {
        pem_location: pem_path,
        ..config
    };
    file.write_all(serde_json::to_vec_pretty(&config)?.as_slice())?;
    Ok(())
}

pub fn parse_args() -> Arguments {
    dbg!(Arguments::from_args())
}
