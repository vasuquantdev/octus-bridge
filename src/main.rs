use anyhow::Error;
use clap::IntoApp;

use relay::config::*;
use relay::engine;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let args = parse_args();

    match (args.config, args.gen_config) {
        (_, Some(new_config_path)) => generate_config(new_config_path),
        (Some(config), None) => {
            let config = read_config(config)?;
            init_logger(&config.logger_settings)?;
            log::info!("Relay ready.");

            engine::run(config).await
        }
        _ => Arguments::into_app().print_help().map_err(Error::from),
    }
}

fn init_logger(config: &serde_yaml::Value) -> Result<(), Error> {
    let config = serde_yaml::from_value(config.clone())?;
    log4rs::config::init_raw_config(config)?;
    Ok(())
}
