use std::borrow::Cow;

use anyhow::{Context, Result};
use argh::FromArgs;
use dialoguer::{Confirm, Input, Password};
use relay::config::*;
use relay::engine::*;
use serde::Serialize;

#[tokio::main]
async fn main() -> Result<()> {
    run(argh::from_env()).await
}

async fn run(app: App) -> Result<()> {
    let mut config = config::Config::new();
    config.merge(read_config(app.config)?)?;
    config.merge(config::Environment::new())?;

    match app.command {
        Subcommand::Run(run) => run.execute(config.try_into()?).await,
        Subcommand::Generate(generate) => generate.execute(config.try_into()?),
        Subcommand::Export(export) => export.execute(config.try_into()?),
    }
}

#[derive(Debug, PartialEq, FromArgs)]
#[argh(description = "")]
struct App {
    #[argh(subcommand)]
    command: Subcommand,

    /// path to config file ('config.yaml' by default)
    #[argh(option, short = 'c', default = "String::from(\"config.yaml\")")]
    config: String,
}

#[derive(Debug, PartialEq, FromArgs)]
#[argh(subcommand)]
enum Subcommand {
    Run(CmdRun),
    Generate(CmdGenerate),
    Export(CmdExport),
}

#[derive(Debug, PartialEq, FromArgs)]
/// Starts relay node
#[argh(subcommand, name = "run")]
struct CmdRun {
    /// path to global config file
    #[argh(option, short = 'g')]
    global_config: String,
}

impl CmdRun {
    async fn execute(self, config: AppConfig) -> Result<()> {
        let global_config = ton_indexer::GlobalConfig::from_file(&self.global_config)
            .context("Failed to open global config")?;

        init_logger(&config.logger_settings).context("Failed to init logger")?;

        log::info!("Initializing relay...");

        let engine = Engine::new(config, global_config)
            .await
            .context("Failed to create engine")?;
        engine.start().await.context("Failed to start engine")?;

        log::info!("Initialized relay");

        futures::future::pending().await
    }
}

#[derive(Debug, PartialEq, FromArgs)]
/// Creates encrypted file
#[argh(subcommand, name = "generate")]
struct CmdGenerate {
    /// the path where the encrypted file will be written
    #[argh(positional)]
    output: String,

    /// import from existing phrases
    #[argh(switch, short = 'i')]
    import: bool,
}

impl CmdGenerate {
    fn execute(self, config: BriefAppConfig) -> Result<()> {
        let path = std::path::Path::new(&self.output);
        if path.exists()
            && !Confirm::new()
                .with_prompt("The key file already exists. Overwrite?")
                .interact()?
        {
            return Ok(());
        }

        let (eth, ton) = match self.import {
            true => {
                let eth_phrase: String = Input::new()
                    .with_prompt("ETH seed phrase")
                    .interact_text()?;
                let eth_path: String = Input::new()
                    .with_prompt("ETH derivation path")
                    .with_initial_text(UnencryptedEthData::DEFAULT_PATH)
                    .interact_text()?;
                let eth = UnencryptedEthData::from_phrase(eth_phrase.into(), eth_path.into())?;

                let ton_phrase: String = Input::new()
                    .with_prompt("TON seed phrase")
                    .interact_text()?;
                let ton_path: String = Input::new()
                    .with_prompt("TON derivation path")
                    .with_initial_text(UnencryptedTonData::DEFAULT_PATH)
                    .interact_text()?;
                let ton = UnencryptedTonData::from_phrase(ton_phrase.into(), ton_path.into())?;

                (eth, ton)
            }
            false => (
                UnencryptedEthData::generate()?,
                UnencryptedTonData::generate()?,
            ),
        };

        let password = config.ask_password(true)?;
        StoredKeysData::new(password.unsecure(), eth, ton)?.save(path)?;
        Ok(())
    }
}

#[derive(Debug, PartialEq, FromArgs)]
/// Prints phrases, ETH address and TON public key
#[argh(subcommand, name = "export")]
struct CmdExport {
    /// the path where the encrypted file is stored
    #[argh(positional)]
    from: String,
}

impl CmdExport {
    fn execute(self, config: BriefAppConfig) -> Result<()> {
        let data = StoredKeysData::load(&self.from)?;
        let password = config.ask_password(false)?;

        let (eth, ton) = data.decrypt(password.unsecure())?;

        #[derive(Serialize)]
        struct ExportedKeysData {
            eth: serde_json::Value,
            ton: serde_json::Value,
        }

        let exported = serde_json::to_string_pretty(&ExportedKeysData {
            eth: eth.as_printable(),
            ton: ton.as_printable(),
        })?;

        println!("{}", exported);
        Ok(())
    }
}

trait BriefAppConfigExt {
    fn ask_password(&self, with_confirmation: bool) -> Result<Cow<secstr::SecUtf8>>;
}

impl BriefAppConfigExt for BriefAppConfig {
    fn ask_password(&self, with_confirmation: bool) -> Result<Cow<secstr::SecUtf8>> {
        Ok(match self.master_password.as_ref() {
            Some(password) => Cow::Borrowed(password),
            None => {
                let mut password = Password::new();
                password.with_prompt("Enter password");
                if with_confirmation {
                    password.with_confirmation("Confirm password", "Passwords mismatching");
                }
                Cow::Owned(password.interact()?.into())
            }
        })
    }
}

fn read_config<P>(path: P) -> Result<config::File<config::FileSourceString>>
where
    P: AsRef<std::path::Path>,
{
    let data = std::fs::read_to_string(path)?;
    let re = regex::Regex::new(r"\$\{([a-zA-Z_][0-9a-zA-Z_]*)\}").unwrap();
    let result = re.replace_all(&data, |caps: &regex::Captures| {
        match std::env::var(&caps[1]) {
            Ok(value) => value,
            Err(_) => (&caps[0]).to_string(),
        }
    });

    Ok(config::File::from_str(
        result.as_ref(),
        config::FileFormat::Yaml,
    ))
}

fn init_logger(config: &serde_yaml::Value) -> Result<()> {
    let config = serde_yaml::from_value(config.clone())?;
    log4rs::config::init_raw_config(config)?;
    Ok(())
}
