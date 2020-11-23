use std::sync::Arc;

use anyhow::Error;
use serde::Deserialize;
use sled::Db;

use crate::config::RelayConfig;
use crate::crypto::key_managment::KeyData;
use crate::engine::bridge::Bridge;
use crate::engine::routes::create_bridge;

impl State {
    pub async fn finalize(&mut self, config: RelayConfig, key_data: KeyData) -> Result<(), Error> {
        let bridge = create_bridge(self.state_manager.clone(), config, key_data).await?;

        log::info!("Successfully initialized");

        let spawned_bridge = bridge.clone();
        tokio::spawn(async move { spawned_bridge.run().await });

        self.bridge_state = BridgeState::Running(bridge);
        Ok(())
    }
}

pub struct State {
    pub state_manager: Db,
    pub bridge_state: BridgeState,
}

pub enum BridgeState {
    Uninitialized,
    Locked,
    Running(Arc<Bridge>),
}

#[derive(Deserialize, Debug)]
pub struct InitData {
    pub ton_seed: String,
    pub eth_seed: String,
    pub password: String,
    pub language: String,
}

#[derive(Deserialize, Debug)]
pub struct Password {
    pub password: String,
}

#[derive(Deserialize, Debug)]
pub struct RescanEthData {
    pub block: u64,
}
