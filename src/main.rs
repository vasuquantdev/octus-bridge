use std::borrow::Cow;

use anyhow::{Context, Result};
use argh::FromArgs;
use dialoguer::{Confirm, Input, Password};
use futures::future::Future;
use relay::config::*;
use relay::engine::*;
use serde::{Deserialize, Serialize};
use tokio::signal::unix;
use tokio::sync::mpsc;

#[tokio::main]
async fn main() -> Result<()> {
    run(argh::from_env()).await
}

async fn run(app: App) -> Result<()> {
    match app.command {
        Subcommand::Run(run) => run.execute().await,
        Subcommand::Generate(generate) => generate.execute(),
        Subcommand::Export(export) => export.execute(),
    }
}

#[derive(Debug, PartialEq, FromArgs)]
#[argh(description = "")]
struct App {
    #[argh(subcommand)]
    command: Subcommand,
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
    /// path to config file ('config.yaml' by default)
    #[argh(option, short = 'c', default = "String::from(\"config.yaml\")")]
    config: String,

    /// path to global config file
    #[argh(option, short = 'g')]
    global_config: String,
}

impl CmdRun {
    async fn execute(self) -> Result<()> {
        let config: AppConfig = read_config(&self.config)?;
        let global_config = ton_indexer::GlobalConfig::from_file(&self.global_config)
            .context("Failed to open global config")?;

        init_logger(&config.logger_settings, move || {
            futures::future::ready(
                read_config(self.config.clone()).map(|config: AppConfig| config.logger_settings),
            )
        })
        .context("Failed to init logger")?;

        log::info!("Initializing relay...");

        let (shutdown_requests_tx, mut shutdown_requests_rx) = mpsc::unbounded_channel();

        let engine = Engine::new(config, global_config, shutdown_requests_tx)
            .await
            .context("Failed to create engine")?;
        engine.start().await.context("Failed to start engine")?;

        log::info!("Initialized relay");

        shutdown_requests_rx.recv().await;
        Ok(())
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

    /// use empty password
    #[argh(switch)]
    empty_password: bool,

    /// path to config file ('config.yaml' by default)
    #[argh(option, short = 'c', default = "String::from(\"config.yaml\")")]
    config: String,
}

impl CmdGenerate {
    fn execute(self) -> Result<()> {
        let config: BriefAppConfig = read_config(self.config)?;

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
                let ton_phrase: String = Input::new()
                    .with_prompt("TON seed phrase")
                    .interact_text()?;
                let ton_path: String = Input::new()
                    .with_prompt("TON derivation path")
                    .with_initial_text(UnencryptedTonData::DEFAULT_PATH)
                    .interact_text()?;
                let ton = UnencryptedTonData::from_phrase(ton_phrase.into(), ton_path.into())?;

                let eth_phrase: String = Input::new()
                    .with_prompt("ETH seed phrase")
                    .interact_text()?;
                let eth_path: String = Input::new()
                    .with_prompt("ETH derivation path")
                    .with_initial_text(UnencryptedEthData::DEFAULT_PATH)
                    .interact_text()?;
                let eth = UnencryptedEthData::from_phrase(eth_phrase.into(), eth_path.into())?;

                (eth, ton)
            }
            false => (
                UnencryptedEthData::generate()?,
                UnencryptedTonData::generate()?,
            ),
        };

        println!("Generated TON data: {:#}", ton.as_printable());
        println!("Generated ETH data: {:#}", eth.as_printable());

        let password = if self.empty_password {
            make_empty_password()
        } else {
            config.ask_password(true)?
        };
        StoredKeysData::new(password.unsecure(), eth, ton)
            .context("Failed to generate encrypted keys data")?
            .save(path)
            .context("Failed to save encrypted keys data")?;

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

    /// use empty password
    #[argh(switch)]
    empty_password: bool,

    /// path to config file ('config.yaml' by default)
    #[argh(option, short = 'c', default = "String::from(\"config.yaml\")")]
    config: String,
}

impl CmdExport {
    fn execute(self) -> Result<()> {
        let config: BriefAppConfig = read_config(self.config)?;

        let data =
            StoredKeysData::load(&self.from).context("Failed to load encrypted keys data")?;
        let password = if self.empty_password {
            make_empty_password()
        } else {
            config.ask_password(false)?
        };

        let (eth, ton) = data
            .decrypt(password.unsecure())
            .context("Failed to decrypt keys data")?;

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
            Some(password) if !password.unsecure().is_empty() => Cow::Borrowed(password),
            _ => {
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

fn make_empty_password<'a>() -> Cow<'a, secstr::SecUtf8> {
    Cow::Owned("".into())
}

fn read_config<P, T>(path: P) -> Result<T>
where
    P: AsRef<std::path::Path>,
    for<'de> T: Deserialize<'de>,
{
    let data = std::fs::read_to_string(path).context("Failed to read config")?;
    let re = regex::Regex::new(r"\$\{([a-zA-Z_][0-9a-zA-Z_]*)\}").unwrap();
    let result = re.replace_all(&data, |caps: &regex::Captures| {
        match std::env::var(&caps[1]) {
            Ok(value) => value,
            Err(_) => {
                eprintln!("WARN: Environment variable {} was not set", &caps[1]);
                String::default()
            }
        }
    });

    let mut config = config::Config::new();
    config.merge(config::File::from_str(
        result.as_ref(),
        config::FileFormat::Yaml,
    ))?;

    config.try_into().context("Failed to parse config")
}

fn init_logger<F, R>(initial_value: &serde_yaml::Value, mut on_reload: F) -> Result<()>
where
    F: FnMut() -> R + Send + Sync + 'static,
    R: Future<Output = Result<serde_yaml::Value>> + Send + Sync + 'static,
{
    fn parse_config(value: serde_yaml::Value) -> Result<log4rs::Config> {
        let config = serde_yaml::from_value::<log4rs::config::RawConfig>(value)?;

        let (appenders, errors) = config.appenders_lossy(&log4rs::config::Deserializers::default());
        if !errors.is_empty() {
            return Err(InitError::Deserializing).with_context(|| format!("{:#?}", errors));
        }

        let config = log4rs::Config::builder()
            .appenders(appenders)
            .loggers(config.loggers())
            .build(config.root())?;
        Ok(config)
    }

    let handle = log4rs::config::init_config(parse_config(initial_value.clone())?)?;
    let mut signals_stream =
        unix::signal(unix::SignalKind::hangup()).context("Failed to subscribe to SIGHUP signal")?;

    tokio::spawn(async move {
        while let Some(()) = signals_stream.recv().await {
            match on_reload().await.and_then(parse_config) {
                Ok(config) => {
                    handle.set_config(config);
                }
                Err(e) => {
                    log::error!("Failed to reload config: {:?}", e);
                }
            };
        }
    });

    Ok(())
}

#[derive(thiserror::Error, Debug)]
enum InitError {
    #[error("Errors found when deserializing the logger config")]
    Deserializing,
}
