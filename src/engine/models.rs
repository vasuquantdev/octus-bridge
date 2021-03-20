use relay_models::models::{
    CommonEventConfigurationParamsView, EthEventConfigurationView, EventConfigurationType,
    EventConfigurationView, NewEventConfiguration, TonEventConfigurationView, Voting,
};
use relay_ton::contracts;

use crate::config::RelayConfig;
use crate::crypto::key_managment::KeyData;
use crate::engine::bridge::*;
use crate::prelude::*;

impl State {
    pub async fn finalize(&mut self, config: RelayConfig, key_data: KeyData) -> Result<(), Error> {
        log::info!("ETH address: 0x{}", hex::encode(&key_data.eth.address()));
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

impl FromContractModels<&contracts::models::CommonEventConfigurationParams>
    for CommonEventConfigurationParamsView
{
    fn from(c: &contracts::models::CommonEventConfigurationParams) -> Self {
        Self {
            event_abi: c.event_abi.clone(),
            event_required_confirmations: c.event_required_confirmations,
            event_required_rejects: c.event_required_rejects,
            event_code: relay_ton::prelude::serialize_toc(&c.event_code)
                .map(hex::encode)
                .unwrap_or_default(),
            bridge_address: c.bridge_address.to_string(),
            event_initial_balance: c.event_initial_balance.to_u64().unwrap_or(u64::MAX),
            meta: relay_ton::prelude::serialize_toc(&c.meta)
                .map(hex::encode)
                .unwrap_or_default(),
        }
    }
}

impl FromContractModels<&contracts::models::EthEventConfiguration> for EthEventConfigurationView {
    fn from(c: &contracts::models::EthEventConfiguration) -> Self {
        Self {
            common: <CommonEventConfigurationParamsView as FromContractModels<_>>::from(&c.common),
            event_address: hex::encode(&c.event_address),
            event_blocks_to_confirm: c.event_blocks_to_confirm,
            proxy_address: c.proxy_address.to_string(),
            start_block_number: c.start_block_number,
        }
    }
}

impl
    FromContractModels<(
        u32,
        &MsgAddressInt,
        &contracts::models::EthEventConfiguration,
    )> for EventConfigurationView
{
    fn from(
        (id, address, c): (
            u32,
            &MsgAddressInt,
            &contracts::models::EthEventConfiguration,
        ),
    ) -> Self {
        Self::Eth {
            id,
            address: address.to_string(),
            data: <EthEventConfigurationView as FromContractModels<_>>::from(c),
        }
    }
}

impl FromContractModels<&contracts::models::TonEventConfiguration> for TonEventConfigurationView {
    fn from(c: &contracts::models::TonEventConfiguration) -> Self {
        Self {
            common: <CommonEventConfigurationParamsView as FromContractModels<_>>::from(&c.common),
            event_address: c.event_address.to_string(),
            proxy_address: hex::encode(c.proxy_address.as_bytes()),
            start_timestamp: c.start_timestamp,
        }
    }
}

impl
    FromContractModels<(
        u32,
        &MsgAddressInt,
        &contracts::models::TonEventConfiguration,
    )> for EventConfigurationView
{
    fn from(
        (id, address, c): (
            u32,
            &MsgAddressInt,
            &contracts::models::TonEventConfiguration,
        ),
    ) -> Self {
        Self::Ton {
            id,
            address: address.to_string(),
            data: <TonEventConfigurationView as FromContractModels<_>>::from(c),
        }
    }
}
