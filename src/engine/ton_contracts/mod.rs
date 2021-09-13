use anyhow::Result;
use nekoton_abi::*;
use ton_types::UInt256;

pub use self::models::*;
use crate::utils::*;

pub mod base_event_configuration_contract;
pub mod base_event_contract;
pub mod bridge_contract;
pub mod connector_contract;
pub mod elections_contract;
pub mod eth_event_configuration_contract;
pub mod eth_event_contract;
pub mod relay_round_contract;
pub mod staking_contract;
pub mod ton_event_configuration_contract;
pub mod ton_event_contract;
pub mod user_data_contract;

mod models;

pub struct EventBaseContract<'a>(pub &'a ExistingContract);

impl EventBaseContract<'_> {
    pub fn status(&self) -> Result<EventStatus> {
        let function = base_event_contract::status();
        let result = self.0.run_local(function, &[])?.unpack_first()?;
        Ok(result)
    }

    pub fn get_voters(&self, vote: EventVote) -> Result<Vec<UInt256>> {
        let function = base_event_contract::get_voters();
        let inputs = [answer_id(), vote.token_value().named("vote")];
        let RelayKeys { items } = self.0.run_local(function, &inputs)?.unpack()?;
        Ok(items)
    }
}

pub struct EthEventContract<'a>(pub &'a ExistingContract);

impl EthEventContract<'_> {
    pub fn event_init_data(&self) -> Result<EthEventInitData> {
        let function = eth_event_contract::get_event_init_data();
        let inputs = [answer_id()];
        let event_init_data = self.0.run_local(function, &inputs)?.unpack_first()?;
        Ok(event_init_data)
    }
}

pub struct TonEventContract<'a>(pub &'a ExistingContract);

impl TonEventContract<'_> {
    pub fn event_init_data(&self) -> Result<TonEventInitData> {
        let function = ton_event_contract::get_event_init_data();
        let inputs = [answer_id()];
        let event_init_data = self.0.run_local(function, &inputs)?.unpack_first()?;
        Ok(event_init_data)
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
}

pub struct TonEventConfigurationContract<'a>(pub &'a ExistingContract);

impl TonEventConfigurationContract<'_> {
    pub fn get_details(&self) -> Result<TonEventConfigurationDetails> {
        let function = ton_event_configuration_contract::get_details();
        let details = self.0.run_local(function, &[answer_id()])?.unpack()?;
        Ok(details)
    }
}

pub struct BridgeContract<'a>(pub &'a ExistingContract);

impl BridgeContract<'_> {
    pub fn connector_counter(&self) -> Result<u64> {
        let function = bridge_contract::connector_counter();
        let counter = self.0.run_local(function, &[])?.unpack_first()?;
        Ok(counter)
    }

    pub fn get_details(&self) -> Result<BridgeDetails> {
        let function = bridge_contract::get_details();
        let input = [answer_id()];
        let configuration = self.0.run_local(function, &input)?.unpack()?;
        Ok(configuration)
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
    pub fn get_details(&self) -> Result<StakingDetails> {
        let function = staking_contract::get_details();
        let input = [answer_id()];
        let details = self.0.run_local(function, &input)?.unpack_first()?;
        Ok(details)
    }

    pub fn get_relay_rounds_details(&self) -> Result<RelayRoundsDetails> {
        let function = staking_contract::get_relay_rounds_details();
        let input = [answer_id()];
        let details = self.0.run_local(function, &input)?.unpack_first()?;
        Ok(details)
    }

    pub fn get_relay_config(&self) -> Result<RelayConfigDetails> {
        let function = staking_contract::get_relay_config();
        let input = [answer_id()];
        let details = self.0.run_local(function, &input)?.unpack_first()?;
        Ok(details)
    }

    pub fn get_relay_round_address(&self, round_num: u32) -> Result<UInt256> {
        let function = staking_contract::get_relay_round_address();
        let input = [answer_id(), round_num.token_value().named("round_num")];
        let ton_block::MsgAddrStd { address, .. } =
            self.0.run_local(function, &input)?.unpack_first()?;

        Ok(UInt256::from_be_bytes(&address.get_bytestring(0)))
    }

    pub fn get_election_address(&self, round_num: u32) -> Result<UInt256> {
        let function = staking_contract::get_election_address();
        let input = [answer_id(), round_num.token_value().named("round_num")];
        let ton_block::MsgAddrStd { address, .. } =
            self.0.run_local(function, &input)?.unpack_first()?;

        Ok(UInt256::from_be_bytes(&address.get_bytestring(0)))
    }

    pub fn get_user_data_address(&self, user: &UInt256) -> Result<UInt256> {
        let function = staking_contract::get_user_data_address();
        let input = [
            answer_id(),
            ton_block::MsgAddrStd::with_address(None, 0, user.into())
                .token_value()
                .named("user"),
        ];
        let ton_block::MsgAddrStd { address, .. } =
            self.0.run_local(function, &input)?.unpack_first()?;

        Ok(UInt256::from_be_bytes(&address.get_bytestring(0)))
    }
}

pub struct ElectionsContract<'a>(pub &'a ExistingContract);

impl ElectionsContract<'_> {
    pub fn staker_addrs(&self) -> Result<Vec<UInt256>> {
        let function = elections_contract::staker_addrs();
        let StakerAddresses { items } = self.0.run_local(function, &[])?.unpack()?;
        Ok(items)
    }
}

pub struct RelayRoundContract<'a>(pub &'a ExistingContract);

impl RelayRoundContract<'_> {
    pub fn relay_keys(&self) -> Result<Vec<UInt256>> {
        let function = relay_round_contract::relay_keys();
        let RelayKeys { items } = self.0.run_local(function, &[])?.unpack()?;
        Ok(items)
    }
}

pub struct UserDataContract<'a>(pub &'a ExistingContract);

impl UserDataContract<'_> {
    pub fn get_details(&self) -> Result<UserDataDetails> {
        let function = user_data_contract::get_details();
        Ok(self.0.run_local(function, &[])?.unpack_first()?)
    }
}
