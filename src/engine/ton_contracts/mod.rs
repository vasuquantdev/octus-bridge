use anyhow::Result;
use nekoton_abi::*;
#[cfg(feature = "ton")]
use ton_block::Deserializable;
use ton_types::UInt256;

pub use self::models::*;
use crate::utils::*;

pub mod base_event_configuration_contract;
pub mod base_event_contract;
pub mod bridge_contract;
pub mod connector_contract;
#[cfg(not(feature = "disable-staking"))]
pub mod elections_contract;
pub mod eth_ton_event_configuration_contract;
pub mod eth_ton_event_contract;
#[cfg(not(feature = "disable-staking"))]
pub mod relay_round_contract;
pub mod sol_ton_event_configuration_contract;
pub mod sol_ton_event_contract;
#[cfg(not(feature = "disable-staking"))]
pub mod staking_contract;
pub mod ton_eth_event_configuration_contract;
pub mod ton_eth_event_contract;
pub mod ton_sol_event_configuration_contract;
pub mod ton_sol_event_contract;
#[cfg(not(feature = "disable-staking"))]
pub mod user_data_contract;

mod models;

pub struct EventBaseContract<'a>(pub &'a ExistingContract);

impl EventBaseContract<'_> {
    pub fn status(&self) -> Result<EventStatus> {
        let function = base_event_contract::status();
        let result = self.0.run_local(function, &[])?.unpack_first()?;
        Ok(result)
    }

    pub fn round_number(&self) -> Result<u32> {
        let function = base_event_contract::round_number();
        let result = self.0.run_local(function, &[])?.unpack_first()?;
        Ok(result)
    }

    pub fn created_at(&self) -> Result<u32> {
        let function = base_event_contract::created_at();
        let result = self.0.run_local(function, &[])?.unpack_first()?;
        Ok(result)
    }

    pub fn get_voters(&self, vote: EventVote) -> Result<Vec<UInt256>> {
        let function = base_event_contract::get_voters();
        let inputs = [answer_id(), vote.token_value().named("vote")];
        let RelayKeys { items } = self.0.run_local(function, &inputs)?.unpack()?;
        Ok(items)
    }

    pub fn get_api_version(&self) -> Result<u32> {
        let function = base_event_contract::get_api_version();
        let inputs = [answer_id()];
        let version = self.0.run_local(function, &inputs)?.unpack_first()?;
        Ok(version)
    }
}

pub struct EthTonEventContract<'a>(pub &'a ExistingContract);

impl EthTonEventContract<'_> {
    pub fn event_init_data(&self) -> Result<EthTonEventInitData> {
        let function = eth_ton_event_contract::get_event_init_data();
        let event_init_data = self
            .0
            .run_local_responsible(function, &[answer_id()])?
            .unpack_first()?;
        Ok(event_init_data)
    }

    #[cfg(feature = "ton")]
    pub fn event_decoded_data(&self) -> Result<EthTonEventDecodedData> {
        let function = eth_ton_event_contract::get_decoded_data();
        let event_decoded_data = self
            .0
            .run_local_responsible(function, &[answer_id()])?
            .unpack()?;
        Ok(event_decoded_data)
    }
}

pub struct TonEthEventContract<'a>(pub &'a ExistingContract);

impl TonEthEventContract<'_> {
    pub fn event_init_data(&self) -> Result<TonEthEventInitData> {
        let function = ton_eth_event_contract::get_event_init_data();
        let event_init_data = self
            .0
            .run_local_responsible(function, &[answer_id()])?
            .unpack_first()?;
        Ok(event_init_data)
    }

    #[cfg(feature = "ton")]
    pub fn event_decoded_data(&self) -> Result<TonEthEventDecodedData> {
        let function = ton_eth_event_contract::get_decoded_data();
        let event_decoded_data = self
            .0
            .run_local_responsible(function, &[answer_id()])?
            .unpack()?;
        Ok(event_decoded_data)
    }
}

pub struct SolTonEventContract<'a>(pub &'a ExistingContract);

impl SolTonEventContract<'_> {
    pub fn event_init_data(&self) -> Result<SolTonEventInitData> {
        let function = sol_ton_event_contract::get_event_init_data();
        let event_init_data = self
            .0
            .run_local_responsible(function, &[answer_id()])?
            .unpack_first()?;
        Ok(event_init_data)
    }
}

pub struct TonSolEventContract<'a>(pub &'a ExistingContract);

impl TonSolEventContract<'_> {
    pub fn event_init_data(&self) -> Result<TonSolEventInitData> {
        let function = ton_sol_event_contract::get_event_init_data();
        let event_init_data = self
            .0
            .run_local_responsible(function, &[answer_id()])?
            .unpack_first()?;
        Ok(event_init_data)
    }
}

pub struct EventConfigurationBaseContract<'a>(pub &'a ExistingContract);

impl EventConfigurationBaseContract<'_> {
    pub fn get_type(&self) -> Result<EventType> {
        let function = base_event_configuration_contract::get_type();
        let event_type = self
            .0
            .run_local_responsible(function, &[answer_id()])?
            .unpack_first()?;
        Ok(event_type)
    }

    pub fn get_flags(&self) -> Result<Option<u64>> {
        let function = base_event_configuration_contract::get_flags();
        let output = function.run_local_responsible(
            &nekoton_utils::SimpleClock,
            self.0.account.clone(),
            &[answer_id()],
        )?;
        if output.result_code == 60 {
            Ok(None)
        } else if let Some(tokens) = output.tokens {
            Ok(Some(tokens.unpack_first()?))
        } else {
            Err(ExistingContractError::NonZeroResultCode(output.result_code).into())
        }
    }
}

pub struct EthTonEventConfigurationContract<'a>(pub &'a ExistingContract);

impl EthTonEventConfigurationContract<'_> {
    pub fn get_details(&self) -> Result<EthTonEventConfigurationDetails> {
        let function = eth_ton_event_configuration_contract::get_details();
        let details = self
            .0
            .run_local_responsible(function, &[answer_id()])?
            .unpack()?;
        Ok(details)
    }
}

pub struct TonEthEventConfigurationContract<'a>(pub &'a ExistingContract);

impl TonEthEventConfigurationContract<'_> {
    pub fn get_details(&self) -> Result<TonEthEventConfigurationDetails> {
        let function = ton_eth_event_configuration_contract::get_details();
        let details = self
            .0
            .run_local_responsible(function, &[answer_id()])?
            .unpack()?;
        Ok(details)
    }
}

pub struct SolTonEventConfigurationContract<'a>(pub &'a ExistingContract);

impl SolTonEventConfigurationContract<'_> {
    pub fn get_details(&self) -> Result<SolTonEventConfigurationDetails> {
        let function = sol_ton_event_configuration_contract::get_details();
        let details = self
            .0
            .run_local_responsible(function, &[answer_id()])?
            .unpack()?;
        Ok(details)
    }
}

pub struct TonSolEventConfigurationContract<'a>(pub &'a ExistingContract);

impl TonSolEventConfigurationContract<'_> {
    pub fn get_details(&self) -> Result<TonSolEventConfigurationDetails> {
        let function = ton_sol_event_configuration_contract::get_details();
        let details = self
            .0
            .run_local_responsible(function, &[answer_id()])?
            .unpack()?;
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

    #[cfg(not(feature = "disable-staking"))]
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

#[cfg(not(feature = "disable-staking"))]
pub struct StakingContract<'a>(pub &'a ExistingContract);

#[cfg(not(feature = "disable-staking"))]
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

#[cfg(not(feature = "disable-staking"))]
pub struct ElectionsContract<'a>(pub &'a ExistingContract);

#[cfg(not(feature = "disable-staking"))]
impl ElectionsContract<'_> {
    pub fn staker_addrs(&self) -> Result<Vec<UInt256>> {
        let function = elections_contract::staker_addrs();
        let StakerAddresses { items } = self.0.run_local(function, &[])?.unpack()?;
        Ok(items)
    }
}

#[cfg(not(feature = "disable-staking"))]
pub struct RelayRoundContract<'a>(pub &'a ExistingContract);

#[cfg(not(feature = "disable-staking"))]
impl RelayRoundContract<'_> {
    pub fn get_details(&self) -> Result<RelayRoundDetails> {
        let function = relay_round_contract::get_details();
        let inputs = [answer_id()];
        let details = self.0.run_local(function, &inputs)?.unpack_first()?;
        Ok(details)
    }

    pub fn has_unclaimed_reward(&self, staker_addr: UInt256) -> Result<bool> {
        let function = relay_round_contract::has_unclaimed_reward();
        let inputs = [
            answer_id(),
            ton_block::MsgAddrStd::with_address(None, 0, staker_addr.into())
                .token_value()
                .named("staker_addr"),
        ];
        let has_reward = self.0.run_local(function, &inputs)?.unpack_first()?;
        Ok(has_reward)
    }

    pub fn end_time(&self) -> Result<u32> {
        let function = relay_round_contract::end_time();
        let end_time = self.0.run_local(function, &[])?.unpack_first()?;
        Ok(end_time)
    }
}

#[cfg(not(feature = "disable-staking"))]
pub struct UserDataContract<'a>(pub &'a ExistingContract);

#[cfg(not(feature = "disable-staking"))]
impl UserDataContract<'_> {
    pub fn get_details(&self) -> Result<UserDataDetails> {
        let function = user_data_contract::get_details();
        Ok(self.0.run_local(function, &[answer_id()])?.unpack_first()?)
    }
}

#[cfg(feature = "ton")]
pub struct JettonWalletContract<'a>(pub &'a ExistingContract);

#[cfg(feature = "ton")]
impl JettonWalletContract<'_> {
    pub fn get_jetton_minter(&self) -> Result<ton_block::MsgAddressInt> {
        let data = self.get_wallet_data()?;

        const JETTON_MINTER_ITEM_POS: usize = 2;
        let StackItem::Cell(jetton_minter) = &data.stack[JETTON_MINTER_ITEM_POS] else {
            return Err(
                ExistingContractError::UnexpectedStackItemType(JETTON_MINTER_ITEM_POS).into(),
            );
        };

        let jetton_minter_address =
            ton_block::MsgAddressInt::construct_from_cell(jetton_minter.clone())?;

        Ok(jetton_minter_address)
    }

    pub fn get_wallet_data(&self) -> Result<VmGetterOutput> {
        let context = ExecutionContext {
            clock: &nekoton_utils::SimpleClock,
            account_stuff: &self.0.account,
        };
        let data = context.run_getter("get_wallet_data", &[])?;

        if !data.is_ok || data.exit_code != 0 {
            return Err(ExistingContractError::NonZeroResultCode(data.exit_code).into());
        }

        const EXPECTED_STACK_LEN: usize = 4;
        if !data.stack.len() == EXPECTED_STACK_LEN {
            return Err(ExistingContractError::ItemsStackLenMismatch {
                expected: EXPECTED_STACK_LEN,
                actual: data.stack.len(),
            }
            .into());
        }

        Ok(data)
    }
}
