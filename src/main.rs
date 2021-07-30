use std::path::PathBuf;

use anyhow::Result;
use relay::config::*;
use relay::engine::*;
use serde::Deserialize;

#[tokio::main]
async fn main() -> Result<()> {
    run().await
}

async fn run() -> Result<()> {
    let config = ApplicationConfig::from_env()?;

    let relay_config = RelayConfig::from_file(&config.relay_config)?;
    let global_config = ton_indexer::GlobalConfig::from_file(&config.global_config)?;

    init_logger(&relay_config.logger_settings)?;

    log::info!("Initializing relay...");

    let engine = Engine::new(relay_config, global_config).await?;
    engine.start().await?;

    log::info!("Initialized relay");

    futures::future::pending().await
}

#[derive(Deserialize)]
struct ApplicationConfig {
    relay_config: PathBuf,
    global_config: PathBuf,
}

impl ApplicationConfig {
    fn from_env() -> Result<Self> {
        let mut config = config::Config::new();
        config.merge(config::Environment::new())?;
        let config: Self = config.try_into()?;
        Ok(config)
    }
}

fn init_logger(config: &serde_yaml::Value) -> Result<()> {
    let config = serde_yaml::from_value(config.clone())?;
    log4rs::config::init_raw_config(config)?;
    Ok(())
}
