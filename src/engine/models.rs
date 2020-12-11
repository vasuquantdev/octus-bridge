use std::convert::TryFrom;
use std::str::FromStr;
use std::sync::Arc;

use anyhow::{anyhow, Error};
use num_bigint::BigUint;
use num_traits::ToPrimitive;
use serde::{Deserialize, Serialize};
use sled::Db;
use ton_block::MsgAddressInt;

use relay_ton::contracts;

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

// TODO: move into separate lib and share with client
#[derive(Deserialize, Serialize)]
pub struct NewEventConfiguration {
    pub ethereum_event_abi: String,
    pub ethereum_event_address: String,
    pub event_proxy_address: String,
    pub ethereum_event_blocks_to_confirm: u64,
    pub required_confirmations: u64,
    pub required_rejections: u64,
}

impl TryFrom<NewEventConfiguration> for contracts::models::NewEventConfiguration {
    type Error = anyhow::Error;

    fn try_from(value: NewEventConfiguration) -> Result<Self, Self::Error> {
        let ethereum_event_abi: serde_json::Value =
            serde_json::from_str(&value.ethereum_event_abi)?;

        let ethereum_event_address =
            ethereum_types::Address::from_str(&value.ethereum_event_address)?;
        let event_proxy_address = MsgAddressInt::from_str(&value.event_proxy_address)
            .map_err(|e| anyhow::anyhow!("{}", e.to_string()))?;
        let ethereum_event_blocks_to_confirm = value.ethereum_event_blocks_to_confirm.into();
        let required_confirmations = value.required_confirmations.into();
        let required_rejections = value.required_confirmations.into();

        Ok(Self {
            ethereum_event_abi: serde_json::to_string(&ethereum_event_abi)?,
            ethereum_event_address,
            event_proxy_address,
            ethereum_event_blocks_to_confirm,
            required_confirmations,
            required_rejections,
        })
    }
}

// TODO: move into separate lib and share with client
#[derive(Deserialize, Serialize)]
pub struct EventConfiguration {
    pub address: String,

    pub ethereum_event_abi: String,
    pub ethereum_event_address: String,
    pub event_proxy_address: String,
    pub ethereum_event_blocks_to_confirm: u64,
    pub required_confirmations: u64,
    pub required_rejections: u64,

    pub confirm_keys: Vec<String>,
    pub reject_keys: Vec<String>,
    pub active: bool,
}

impl From<(MsgAddressInt, contracts::models::EthereumEventConfiguration)> for EventConfiguration {
    fn from((address, c): (MsgAddressInt, contracts::models::EthereumEventConfiguration)) -> Self {
        Self {
            address: address.to_string(),
            ethereum_event_abi: c.ethereum_event_abi,
            ethereum_event_address: hex::encode(c.ethereum_event_address.to_fixed_bytes()),
            event_proxy_address: c.event_proxy_address.to_string(),
            ethereum_event_blocks_to_confirm: c
                .ethereum_event_blocks_to_confirm
                .to_u64()
                .unwrap_or(u64::max_value()),
            required_confirmations: c
                .required_confirmations
                .to_u64()
                .unwrap_or(u64::max_value()),
            required_rejections: c.required_rejections.to_u64().unwrap_or(u64::max_value()),
            confirm_keys: c
                .confirm_keys
                .iter()
                .map(|key| key.to_hex_string())
                .collect(),
            reject_keys: c
                .reject_keys
                .iter()
                .map(|key| key.to_hex_string())
                .collect(),
            active: c.active,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VotingAddress {
    pub address: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase", tag = "vote", content = "address")]
pub enum Voting {
    Confirm(String),
    Reject(String),
}

impl TryFrom<Voting> for (MsgAddressInt, contracts::models::Voting) {
    type Error = anyhow::Error;

    fn try_from(value: Voting) -> Result<Self, Self::Error> {
        let (address, voting) = match value {
            Voting::Confirm(address) => (address, contracts::models::Voting::Confirm),
            Voting::Reject(address) => (address, contracts::models::Voting::Reject),
        };
        let address =
            MsgAddressInt::from_str(&address).map_err(|e| anyhow!("{}", e.to_string()))?;
        Ok((address, voting))
    }
}
