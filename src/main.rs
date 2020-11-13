use anyhow::Error;
use daemonize::Daemonize;
use dialoguer::Password;
use dialoguer::theme::ColorfulTheme;
use log::{error, info};
use secstr::SecStr;

use relay::{crypto::key_managment::KeyData, engine, storage};
use relay::config::{generate_config, parse_args, read_config};

fn main() -> Result<(), Error> {
    env_logger::init();
    let args = parse_args();

    if args.gen_config {
        generate_config(args.config, args.crypto_store_path.unwrap())?;
        return Ok(());
    }

    let config = read_config(&args.config)?;

    info!("Relay ready.");

    let mut executor = tokio::runtime::Runtime::new().unwrap();

    // let res = Daemonize::new().start();
    // match res {
    //     Ok(_) => {
    info!("Really started");
    let err = executor.block_on(engine::run(config))?;

// },
    // Err(e)=>error!("Eror daemonizing app: {}",e)
    // };
    //
    Ok(())
}
