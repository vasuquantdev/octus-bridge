use anyhow::Error;
use relay::config::{read_config, parse_args, generate_config};
#[tokio::main]
async fn main() -> Result<(),Error> {
    env_logger::init();
    let args = parse_args();
    if args.gen_config{
        generate_config("default_config.json")?;
        return Ok(())
    }
    let config = read_config(&args.config.unwrap())?;



    log::info!("Relay ready.");
    Ok(())
}
