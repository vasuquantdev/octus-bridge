use anyhow::Context;
use tokio::signal::ctrl_c;
use tokio::sync::oneshot;

use models::*;

use super::db::migrate::Migrator;
use crate::config::RelayConfig;
use crate::engine::handle_panic::setup_panic_handler;
use crate::prelude::*;

mod api;
pub mod bridge;
mod exporter;
mod handle_panic;
pub mod models;

pub async fn run(config: RelayConfig) -> Result<(), Error> {
    let db = sled::open(&config.storage_path).map_err(|e| {
        let context = format!(
            "Failed opening db. Db path: {}",
            &config.storage_path.to_string_lossy()
        );
        Error::new(e).context(context)
    })?;

    Migrator::init(&db)
        .context("Failed initializing migrator")?
        .run_migrations()
        .context("Failed running migrations")?;

    setup_panic_handler(db.clone());
    let crypto_data_metadata = std::fs::File::open(&config.keys_path);

    let file_size = match crypto_data_metadata {
        Err(e) => {
            log::warn!("Error opening file with encrypted config: {}", e);
            0
        }
        Ok(a) => a.metadata()?.len(),
    };

    let bridge_state = match file_size {
        0 => {
            log::info!("started in uninitialized state");
            BridgeState::Uninitialized
        }
        _ => {
            log::info!("stared in locked state");
            BridgeState::Locked
        }
    };

    let mut shutdown_notifier = ShutdownNotifier::new();
    let api_shutdown_signal = shutdown_notifier.subscribe();
    let exporter_shutdown_signal = shutdown_notifier.subscribe();

    {
        let db = db.clone();
        tokio::spawn(async move {
            ctrl_c().await.expect("Failed subscribing on unix signals");

            log::info!("Received ctrl-c event.");
            shutdown_notifier.notify();

            log::info!("Flushing db...");
            match db.flush() {
                Ok(a) => log::info!("Flushed db before stop... Bytes written: {:?}", a),
                Err(e) => log::error!("Failed flushing db before panic: {}", e),
            }

            log::info!("Waiting for graceful shutdown...");
            tokio::time::delay_for(tokio::time::Duration::from_secs(2)).await;
            std::process::exit(0);
        })
    };

    let state = Arc::new(RwLock::new(State {
        state_manager: db.clone(),
        bridge_state,
    }));

    tokio::spawn(api::serve(
        config.clone(),
        state.clone(),
        api_shutdown_signal,
    ));
    tokio::spawn(exporter::serve(config, state, exporter_shutdown_signal));

    future::pending().await
}

struct ShutdownNotifier {
    tx: Vec<oneshot::Sender<()>>,
}

impl ShutdownNotifier {
    pub fn new() -> Self {
        Self {
            tx: Default::default(),
        }
    }

    pub fn subscribe(&mut self) -> oneshot::Receiver<()> {
        let (tx, rx) = oneshot::channel();
        self.tx.push(tx);
        rx
    }

    pub fn notify(self) {
        for tx in self.tx {
            let _ = tx.send(());
        }
    }
}
