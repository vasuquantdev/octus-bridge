use anyhow::Error;
use config::{Config, File, FileFormat};
use serde::{Deserialize, Serialize};
use std::io::Write;
use std::path::{Path, PathBuf};
use structopt::StructOpt;
use url::Url;

#[derive(Deserialize, Serialize, Clone, Debug, Default)]
pub struct RelayConfig {
    pub pub_key_location: PathBuf,
    pub private_key_location: PathBuf,
    pub eth_node_address: String,
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
