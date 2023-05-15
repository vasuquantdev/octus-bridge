use std::borrow::Cow;
use std::sync::Arc;

use anyhow::{Context, Result};
use argh::FromArgs;
use dialoguer::{Confirm, Input, Password};
use pkey_mprotect::*;
use relay::config::*;
use relay::engine::*;
use serde::Serialize;
use tokio::signal::unix;
use tokio::sync::mpsc;

#[global_allocator]
static GLOBAL: broxus_util::alloc::Allocator = broxus_util::alloc::allocator();

fn main() -> Result<()> {
    if atty::is(atty::Stream::Stdout) {
        tracing_subscriber::fmt::init();
    } else {
        tracing_subscriber::fmt::fmt().without_time().init();
    }

    let ArgsOrVersion::<App>(app) = argh::from_env();
    match app.command {
        Subcommand::Run(run) => run.execute(),
        Subcommand::Generate(generate) => generate.execute(),
        Subcommand::Export(export) => export.execute(),
    }
}

#[derive(Debug, FromArgs)]
#[argh(description = "")]
struct App {
    #[argh(subcommand)]
    command: Subcommand,
}

#[derive(Debug, FromArgs)]
#[argh(subcommand)]
enum Subcommand {
    Run(CmdRun),
    Generate(CmdGenerate),
    Export(CmdExport),
}

#[derive(Debug, FromArgs)]
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
    fn execute(self) -> Result<()> {
        let (relay, config) = create_relay(&self.config)?;

        let global_config = ton_indexer::GlobalConfig::from_file(&self.global_config)
            .context("Failed to open global config")?;

        // NOTE: protection keys must be called in the main thread
        let protection_keys = ProtectionKeys::new(config.require_protected_keystore)
            .context("Failed to create protection keys")?;

        // Create engine future
        let engine = async move {
            // Spawn SIGHUP signal listener
            relay.start_listening_reloads()?;

            tracing::info!("initializing relay...");
            let mut shutdown_requests_rx =
                relay.init(config, global_config, protection_keys).await?;
            tracing::info!("initialized relay");

            shutdown_requests_rx.recv().await;
            Ok(())
        };

        // Create main future
        let future = async move {
            let any_signal = any_signal([
                unix::SignalKind::interrupt(),
                unix::SignalKind::terminate(),
                unix::SignalKind::quit(),
                unix::SignalKind::from_raw(6),  // SIGABRT/SIGIOT
                unix::SignalKind::from_raw(20), // SIGTSTP
            ]);

            tokio::select! {
                res = engine => res,
                signal = any_signal => {
                    if let Ok(signal) = signal {
                        tracing::warn!(?signal, "received termination signal, flushing state");
                    }
                    // NOTE: engine future is safely dropped here so rocksdb method
                    // `rocksdb_close` is called in DB object destructor
                    Ok(())
                }
            }
        };

        // Manually create an executor and run the main future
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("Failed building the Runtime")
            .block_on(future)
    }
}

#[derive(Debug, FromArgs)]
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
        let config: BriefAppConfig = broxus_util::read_config(self.config)?;

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

#[derive(Debug, FromArgs)]
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
        let config: BriefAppConfig = broxus_util::read_config(self.config)?;

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

        println!("{exported}");
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

fn create_relay<P>(config_path: P) -> Result<(Arc<Relay>, AppConfig)>
where
    P: AsRef<std::path::Path>,
{
    let config: AppConfig = broxus_util::read_config(&config_path)?;
    let state = Arc::new(Relay {
        config_path: config_path.as_ref().into(),
        engine: Default::default(),
    });

    Ok((state, config))
}

struct Relay {
    config_path: std::path::PathBuf,
    engine: tokio::sync::Mutex<Option<Arc<Engine>>>,
}

impl Relay {
    async fn init(
        &self,
        config: AppConfig,
        global_config: ton_indexer::GlobalConfig,
        protection_keys: Arc<ProtectionKeys>,
    ) -> Result<ShutdownRequestsRx> {
        let (shutdown_requests_tx, shutdown_requests_rx) = mpsc::unbounded_channel();

        let engine = Engine::new(config, global_config, protection_keys, shutdown_requests_tx)
            .await
            .context("Failed to create engine")?;
        *self.engine.lock().await = Some(engine.clone());

        engine.start().await.context("Failed to start engine")?;

        Ok(shutdown_requests_rx)
    }

    fn start_listening_reloads(self: &Arc<Self>) -> Result<()> {
        let mut signals_stream = unix::signal(unix::SignalKind::hangup())
            .context("Failed to subscribe to SIGHUP signal")?;

        let state = self.clone();
        tokio::spawn(async move {
            while let Some(()) = signals_stream.recv().await {
                if let Err(e) = state.reload().await {
                    tracing::error!("failed to reload config: {e:?}");
                };
            }
        });

        Ok(())
    }

    async fn reload(&self) -> Result<()> {
        let config: AppConfig = broxus_util::read_config(&self.config_path)?;

        if let Some(engine) = &*self.engine.lock().await {
            engine
                .update_metrics_config(config.metrics_settings)
                .await?;
        }

        tracing::info!("updated config");
        Ok(())
    }
}

fn any_signal<I>(signals: I) -> tokio::sync::oneshot::Receiver<unix::SignalKind>
where
    I: IntoIterator<Item = unix::SignalKind>,
{
    let (tx, rx) = tokio::sync::oneshot::channel();

    let any_signal = futures_util::future::select_all(signals.into_iter().map(|signal| {
        Box::pin(async move {
            unix::signal(signal)
                .expect("Failed subscribing on unix signals")
                .recv()
                .await;
            signal
        })
    }));

    tokio::spawn(async move {
        let signal = any_signal.await.0;
        tx.send(signal).ok();
    });

    rx
}

fn make_empty_password<'a>() -> Cow<'a, secstr::SecUtf8> {
    Cow::Owned("".into())
}

struct ArgsOrVersion<T: argh::FromArgs>(T);

impl<T: argh::FromArgs> argh::TopLevelCommand for ArgsOrVersion<T> {}

impl<T: argh::FromArgs> argh::FromArgs for ArgsOrVersion<T> {
    fn from_args(command_name: &[&str], args: &[&str]) -> Result<Self, argh::EarlyExit> {
        /// Also use argh for catching `--version`-only invocations
        #[derive(argh::FromArgs)]
        struct Version {
            /// print version information and exit
            #[argh(switch, short = 'v')]
            pub version: bool,
        }

        match Version::from_args(command_name, args) {
            Ok(v) if v.version => Err(argh::EarlyExit {
                output: format!("{} {VERSION}", command_name.first().unwrap_or(&""),),
                status: Ok(()),
            }),
            Err(exit) if exit.status.is_ok() => {
                let help = match T::from_args(command_name, &["--help"]) {
                    Ok(_) => unreachable!(),
                    Err(exit) => exit.output,
                };
                Err(argh::EarlyExit {
                    output: format!("{help}  -v, --version     print version information and exit"),
                    status: Ok(()),
                })
            }
            _ => T::from_args(command_name, args).map(Self),
        }
    }
}

const VERSION: &str = env!("CARGO_PKG_VERSION");
