use relay_models::models::{
    EventConfiguration, EventConfigurationType, NewEventConfiguration, Voting,
};
use relay_ton::contracts;

use crate::config::RelayConfig;
use crate::crypto::key_managment::KeyData;
use crate::engine::bridge::*;
use crate::prelude::*;

impl State {
    pub async fn finalize(&mut self, config: RelayConfig, key_data: KeyData) -> Result<(), Error> {
        let bridge = make_bridge(self.state_manager.clone(), config, key_data).await?;

        log::info!("Successfully initialized");

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

pub trait FromRequest<T>: Sized {
    fn try_from(request: T) -> Result<Self, Error>;
}

impl FromRequest<Voting> for (u32, contracts::models::Voting) {
    fn try_from(value: Voting) -> Result<Self, Error> {
        let (configuration_id, voting) = match value {
            Voting::Confirm(configuration_id) => {
                (configuration_id, contracts::models::Voting::Confirm)
            }
            Voting::Reject(configuration_id) => {
                (configuration_id, contracts::models::Voting::Reject)
            }
        };
        Ok((configuration_id, voting))
    }
}

impl FromRequest<NewEventConfiguration> for (u32, MsgAddressInt, contracts::models::EventType) {
    fn try_from(request: NewEventConfiguration) -> Result<Self, Error> {
        let event_address =
            MsgAddressInt::from_str(&request.address).map_err(|e| anyhow!("{}", e.to_string()))?;

        let event_type = match request.configuration_type {
            EventConfigurationType::Eth => contracts::models::EventType::ETH,
            EventConfigurationType::Ton => contracts::models::EventType::TON,
        };

        Ok((request.configuration_id, event_address, event_type))
    }
}

pub trait FromContractModels<T>: Sized {
    fn from(model: T) -> Self;
}

impl FromContractModels<(u32, contracts::models::EthEventConfiguration)> for EventConfiguration {
    fn from((configuration_id, c): (u32, contracts::models::EthEventConfiguration)) -> Self {
        Self {
            configuration_id,
            ethereum_event_abi: c.common.event_abi,
            ethereum_event_address: hex::encode(c.event_address.as_bytes()),
            event_proxy_address: c.proxy_address.to_string(),
            ethereum_event_blocks_to_confirm: c.event_blocks_to_confirm,
            event_required_confirmations: c.common.event_required_confirmations,
            event_required_rejects: c.common.event_required_rejects,
            event_initial_balance: c.common.event_initial_balance.to_u64().unwrap_or(u64::MAX),
            bridge_address: c.common.bridge_address.to_string(),
            event_code: relay_ton::prelude::serialize_toc(&c.common.event_code)
                .map(hex::encode)
                .unwrap_or_default(),
        }
    }
}
