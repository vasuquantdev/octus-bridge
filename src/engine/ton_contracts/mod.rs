use anyhow::Result;
use nekoton_abi::*;
use ton_types::UInt256;

pub use self::models::*;
use crate::utils::*;

pub mod base_event_configuration_contract;
pub mod bridge_contract;
pub mod connector_contract;
pub mod eth_event_configuration_contract;
pub mod eth_event_contract;
pub mod staking_contract;
pub mod ton_event_configuration_contract;
pub mod ton_event_contract;

mod models;

pub struct EthEventContract<'a>(pub &'a ExistingContract);

impl EthEventContract<'_> {
    pub fn get_details(&self) -> Result<TonEventDetails> {
        let function = eth_event_contract::get_details();
        let result = self.0.run_local(function, &[answer_id()])?.unpack()?;
        Ok(result)
    }
}

pub struct TonEventContract<'a>(pub &'a ExistingContract);

impl TonEventContract<'_> {
    pub fn get_details(&self) -> Result<TonEventDetails> {
        let function = ton_event_contract::get_details();
        let result = self.0.run_local(function, &[answer_id()])?.unpack()?;
        Ok(result)
    }
}

pub struct EventConfigurationBaseContract<'a>(pub &'a ExistingContract);

impl EventConfigurationBaseContract<'_> {
    pub fn get_type(&self) -> Result<EventType> {
        let function = base_event_configuration_contract::get_type();
        let event_type = self.0.run_local(function, &[answer_id()])?.unpack_first()?;
        Ok(event_type)
    }
}

pub struct EthEventConfigurationContract<'a>(pub &'a ExistingContract);

impl EthEventConfigurationContract<'_> {
    pub fn get_details(&self) -> Result<EthEventConfigurationDetails> {
        let function = eth_event_configuration_contract::get_details();
        let details = self.0.run_local(function, &[answer_id()])?.unpack()?;
        Ok(details)
    }

    pub fn derive_event_address(&self, vote_data: EthEventVoteData) -> Result<UInt256> {
        let function = eth_event_configuration_contract::derive_event_address();
        let input = [answer_id(), vote_data.token_value().named("vote_data")];
        let ton_block::MsgAddrStd { address, .. } =
            self.0.run_local(function, &input)?.unpack_first()?;

        Ok(UInt256::from_be_bytes(&address.get_bytestring(0)))
    }
}

pub struct TonEventConfigurationContract<'a>(pub &'a ExistingContract);

impl TonEventConfigurationContract<'_> {
    pub fn get_details(&self) -> Result<TonEventConfigurationDetails> {
        let function = ton_event_configuration_contract::get_details();
        let details = self.0.run_local(function, &[answer_id()])?.unpack()?;
        Ok(details)
    }

    pub fn derive_event_address(&self, vote_data: TonEventVoteData) -> Result<UInt256> {
        let function = ton_event_configuration_contract::derive_event_address();
        let input = [answer_id(), vote_data.token_value().named("vote_data")];
        let ton_block::MsgAddrStd { address, .. } =
            self.0.run_local(function, &input)?.unpack_first()?;

        Ok(UInt256::from_be_bytes(&address.get_bytestring(0)))
    }
}

pub struct BridgeContract<'a>(pub &'a ExistingContract);

impl BridgeContract<'_> {
    pub fn bridge_configuration(&self) -> Result<BridgeConfiguration> {
        let function = bridge_contract::bridge_configuration();
        let bridge_configuration: BridgeConfiguration = self.0.run_local(function, &[])?.unpack_first()?;
        Ok(bridge_configuration)
    }

    pub fn derive_connector_address(&self, id: u64) -> Result<UInt256> {
        let function = bridge_contract::derive_connector_address();
        let input = [id.token_value().named("id")];
        let ton_block::MsgAddrStd { address, .. } =
            self.0.run_local(function, &input)?.unpack_first()?;

        Ok(UInt256::from_be_bytes(&address.get_bytestring(0)))
    }
}

pub struct ConnectorContract<'a>(pub &'a ExistingContract);

impl ConnectorContract<'_> {
    pub fn get_details(&self) -> Result<ConnectorDetails> {
        let function = connector_contract::get_details();
        let details = self.0.run_local(function, &[])?.unpack()?;
        Ok(details)
    }
}

pub struct StakingContract<'a>(pub &'a ExistingContract);

impl StakingContract<'_> {
    pub fn current_relay_round(&self) -> Result<u128> {
        let function = staking_contract::current_relay_round();
        let current_relay_round: u128 = self.0.run_local(function, &[])?.unpack_first()?;
        Ok(current_relay_round)
    }

    pub fn get_relay_round_address(&self, round_num: u128) -> Result<UInt256> {
        let function = staking_contract::get_relay_round_address();
        let input = [round_num.token_value().named("round_num")];
        let relay_round_address: ton_block::MsgAddrStd =
            self.0.run_local(function, &input)?.unpack_first()?;

        Ok(UInt256::from_be_bytes(
            &relay_round_address.address.get_bytestring(0),
        ))
    }

    pub fn get_relay_round_address_from_timestamp(&self, time: u128) -> Result<UInt256> {
        let function = staking_contract::get_relay_round_address_from_timestamp();
        let input = [time.token_value().named("time")];
        let relay_round_address_from_timestamp: ton_block::MsgAddrStd =
            self.0.run_local(function, &input)?.unpack_first()?;

        Ok(UInt256::from_be_bytes(
            &relay_round_address_from_timestamp.address.get_bytestring(0),
        ))
    }

    pub fn prev_relay_round_end_time(&self) -> Result<u128> {
        let function = staking_contract::prev_relay_round_end_time();
        let prev_relay_round_end_time: u128 = self.0.run_local(function, &[])?.unpack_first()?;
        Ok(prev_relay_round_end_time)
    }
}
