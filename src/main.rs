use anyhow::Error;
use clap::IntoApp;

use anyhow::Result;
use relay::config::*;
use relay::engine;

#[cfg(feature = "dockered")]
async fn run() -> Result<()> {
    let config = read_env()?;
    init_logger(&config.logger_settings)?;
    log::info!("Relay ready.");
    engine::run(config).await
}

#[cfg(not(feature = "dockered"))]
async fn run() -> Result<()> {
    let args = parse_args();
    match (args.config, args.gen_config) {
        (_, Some(new_config_path)) => generate_config(new_config_path)?,
        (Some(config), None) => {
            let config = read_config(config)?;

            init_logger(&config.logger_settings)?;
            log::info!("Relay ready.");

            engine::run(config).await?;
        }
        _ => Arguments::into_app().print_help().map_err(Error::from)?,
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    run().await
}

fn init_logger(config: &serde_yaml::Value) -> Result<(), Error> {
    let config = serde_yaml::from_value(config.clone())?;
    log4rs::config::init_raw_config(config)?;
    Ok(())
}
