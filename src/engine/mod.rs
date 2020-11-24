mod bridge;
mod models;
mod routes;


use std::sync::Arc;
use anyhow::Error;
use tokio::sync::RwLock;
use crate::config::RelayConfig;
use models::*;


pub async fn run(config: RelayConfig) -> Result<(), Error> {
    let state_manager = sled::open(&config.storage_path)?;

    let crypto_data_metadata = std::fs::File::open(&config.encrypted_data);
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

    routes::serve(
        config,
        Arc::new(RwLock::new(State {
            state_manager,
            bridge_state,
        })),
    )
        .await;
    Ok(())
}
