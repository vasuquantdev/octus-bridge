use anyhow::Error;

use relay::config::*;
use relay::engine;

fn main() -> Result<(), Error> {
    env_logger::init();
    let args = parse_args();

    if args.gen_config {
        generate_config(args.generated_config_path, args.crypto_store_path.unwrap())?;
        return Ok(());
    }

    let config = read_config(&args.config)?;
    log::info!("Relay ready.");

    #[cfg(feature = "daemonizeable")]
    {
        match daemonize::Daemonize::new().start() {
            Ok(_) => run()?,
            Err(e) => error!("Error daemonizing app: {}", e),
        };
    }
    #[cfg(not(feature = "daemonizeable"))]
    run()?;

    Ok(())
}

fn run() -> Result<(), Error> {
    let mut executor = tokio::runtime::Runtime::new().unwrap();

    log::info!("Relay started");
    let _err = executor.block_on(engine::run(config))?;
}
