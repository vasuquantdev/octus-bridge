use anyhow::Error;

use relay::config::*;
use relay::engine;

fn init_logger(log_config_path: &str) {
    if let Err(err) = log4rs::init_file(log_config_path, Default::default()) {
        println!("Error while initializing log by {}: {}", err, err);
    } else {
        return;
    }

    let level = log::LevelFilter::Trace;
    let stdout = log4rs::append::console::ConsoleAppender::builder()
        .target(log4rs::append::console::Target::Stdout)
        .build();

    let config = log4rs::config::Config::builder()
        .appender(
            log4rs::config::Appender::builder()
                .filter(Box::new(log4rs::filter::threshold::ThresholdFilter::new(
                    level,
                )))
                .build("stdout", Box::new(stdout)),
        )
        .build(
            log4rs::config::Root::builder()
                .appender("stdout")
                .build(log::LevelFilter::Trace),
        )
        .unwrap();

    let result = log4rs::init_config(config);
    if let Err(e) = result {
        println!("Error init log: {}", e);
    }
}

fn main() -> Result<(), Error> {
    init_logger("./log4rs.yaml");
    let args = parse_args();
    if args.gen_config {
        generate_config(args.generated_config_path, args.crypto_store_path.unwrap())?;
        return Ok(());
    }

    let config = read_config(&args.config)?;
    log::info!("Relay ready.");

    run(config)?;

    Ok(())
}

fn run(config: RelayConfig) -> Result<(), Error> {
    let mut executor = tokio::runtime::Runtime::new()?;

    log::info!("Relay started");
    let _err = executor.block_on(engine::run(config))?;

    Ok(())
}
