use anyhow::Error;
use relay::config::{read_config, parse_args, generate_config};
use relay::{engine, key_managment::KeyData};
use daemonize::Daemonize;
use log::{error,info};
use dialoguer::Password;
use dialoguer::theme::ColorfulTheme;
use secstr::SecStr;

fn main() -> Result<(),Error> {
    env_logger::init();
    let args = parse_args();
    let password:SecStr = Password::with_theme(&ColorfulTheme::default())
        .with_prompt("Password")
        .with_confirmation("Repeat password", "Error: the passwords don't match.")
        .interact()
        .unwrap().into();
    if args.gen_config.is_some(){
        KeyData::init(&args.config, password, Vec::from("loll"))?;
        generate_config("default_config.json", args.gen_config.unwrap())?;
        return Ok(())
    }

    let config = read_config(&args.config)?;
    info!("Relay ready.");

    let mut executor =  tokio::runtime::Builder::new()
        .threaded_scheduler()
        .build()?;
    let res = Daemonize::new().start();
    match res {
        Ok(_) => {
            info!("Realy started");
            executor.block_on(engine::run(config))? },
        Err(e)=>error!("Eror daemonizing app: {}",e)
    };

    Ok(())
}
