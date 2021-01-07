use crate::prelude::*;
use tokio::signal::ctrl_c;

use models::*;

use crate::config::RelayConfig;
use crate::engine::handle_panic::setup_panic_handler;

pub mod bridge;
mod handle_panic;
pub mod models;
mod routes;

pub async fn run(config: RelayConfig) -> Result<(), Error> {
    let state_manager = sled::open(&config.storage_path)?;
    setup_panic_handler(state_manager.clone());
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

    let (tx, rx) = tokio::sync::oneshot::channel();
    {
        let db = state_manager.clone();
        tokio::spawn(async move {
            ctrl_c().await.expect("Failed subscribing on unix signals");
            log::info!("Received ctrl-c event.");
            tx.send(()).expect("Failed sending notification");
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

    routes::serve(
        config,
        Arc::new(RwLock::new(State {
            state_manager: state_manager.clone(),
            bridge_state,
        })),
        rx,
    )
    .await;

    Ok(())
}
