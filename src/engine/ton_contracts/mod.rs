use anyhow::Result;
use nekoton_abi::*;
use ton_types::UInt256;

use crate::utils::*;

pub struct EthEventContract<'a>(pub &'a ExistingContract);

impl EthEventContract<'_> {
    pub fn get_details(&self) -> Result<TonEventDetails> {
        let mut function = FunctionBuilder::new("getDetails")
            .default_headers()
            .out_arg("event_init_data", EthEventInitData::make_params_tuple())
            .out_arg("status", ton_abi::ParamType::Uint(8))
            .out_arg(
                "confirms",
                ton_abi::ParamType::Array(Box::new(ton_abi::ParamType::Uint(256))),
            )
            .out_arg(
                "rejects",
                ton_abi::ParamType::Array(Box::new(ton_abi::ParamType::Uint(256))),
            )
            .out_arg(
                "empty",
                ton_abi::ParamType::Array(Box::new(ton_abi::ParamType::Uint(256))),
            )
            .out_arg("balance", ton_abi::ParamType::Uint(128))
            .out_arg("initializer", ton_abi::ParamType::Address)
            .out_arg("meta", ton_abi::ParamType::Cell)
            .out_arg("required_votes", ton_abi::ParamType::Uint(32));
        function.make_responsible();

        let result = self
            .0
            .run_local(&function.build(), &[answer_id()])?
            .unpack()?;
        Ok(result)
    }
}

#[derive(Debug, PackAbiPlain, UnpackAbiPlain, Clone)]
pub struct EthEventDetails {
    #[abi]
    pub event_init_data: EthEventInitData,
    #[abi]
    pub status: EventStatus,
    #[abi(with = "array_uint256_bytes")]
    pub confirms: Vec<UInt256>,
    #[abi(with = "array_uint256_bytes")]
    pub rejects: Vec<UInt256>,
    #[abi(with = "array_uint256_bytes")]
    pub empty: Vec<UInt256>,
    #[abi(uint128)]
    pub balance: u128,
    #[abi(address)]
    pub initializer: ton_block::MsgAddressInt,
    #[abi(cell)]
    pub meta: ton_types::Cell,
    #[abi(uint32)]
    pub required_votes: u32,
}

#[derive(Debug, PackAbi, UnpackAbi, Clone)]
pub struct EthEventInitData {
    #[abi]
    pub vote_data: EthEventVoteData,
    #[abi(with = "address_only_hash")]
    pub configuration: UInt256,
    #[abi(with = "address_only_hash")]
    pub staking: UInt256,
    #[abi(uint32)]
    pub chain_id: u32,
}

impl EthEventInitData {
    pub fn make_params_tuple() -> ton_abi::ParamType {
        TupleBuilder::new()
            .arg("vote_data", EthEventVoteData::make_params_tuple())
            .arg("configuration", ton_abi::ParamType::Address)
            .arg("staking", ton_abi::ParamType::Address)
            .arg("chain_id", ton_abi::ParamType::Uint(32))
            .build()
    }
}

#[derive(Debug, PackAbi, UnpackAbi, Clone)]
pub struct EthEventVoteData {
    #[abi(with = "uint256_bytes")]
    pub event_transaction: UInt256,
    #[abi(uint32)]
    pub event_index: u32,
    #[abi(cell)]
    pub event_data: ton_types::Cell,
    #[abi(uint32)]
    pub event_block_number: u32,
    #[abi(with = "uint256_bytes")]
    pub event_block: UInt256,
}

impl EthEventVoteData {
    pub fn make_params_tuple() -> ton_abi::ParamType {
        TupleBuilder::new()
            .arg("event_transaction", ton_abi::ParamType::Uint(256))
            .arg("event_index", ton_abi::ParamType::Uint(32))
            .arg("event_data", ton_abi::ParamType::Cell)
            .arg("event_block_number", ton_abi::ParamType::Uint(32))
            .arg("event_block", ton_abi::ParamType::Uint(256))
            .build()
    }
}

pub struct TonEventContract<'a>(pub &'a ExistingContract);

impl TonEventContract<'_> {
    pub fn get_details(&self) -> Result<TonEventDetails> {
        let mut function = FunctionBuilder::new("getDetails")
            .default_headers()
            .out_arg("event_init_data", TonEventInitData::make_params_tuple())
            .out_arg("status", ton_abi::ParamType::Uint(8))
            .out_arg(
                "confirms",
                ton_abi::ParamType::Array(Box::new(ton_abi::ParamType::Uint(256))),
            )
            .out_arg(
                "rejects",
                ton_abi::ParamType::Array(Box::new(ton_abi::ParamType::Uint(256))),
            )
            .out_arg(
                "empty",
                ton_abi::ParamType::Array(Box::new(ton_abi::ParamType::Uint(256))),
            )
            .out_arg(
                "signatures",
                ton_abi::ParamType::Array(Box::new(ton_abi::ParamType::Bytes)),
            )
            .out_arg("balance", ton_abi::ParamType::Uint(128))
            .out_arg("initializer", ton_abi::ParamType::Address)
            .out_arg("meta", ton_abi::ParamType::Cell)
            .out_arg("required_votes", ton_abi::ParamType::Uint(32));
        function.make_responsible();

        let result = self
            .0
            .run_local(&function.build(), &[answer_id()])?
            .unpack()?;
        Ok(result)
    }
}

#[derive(Debug, PackAbiPlain, UnpackAbiPlain, Clone)]
pub struct TonEventDetails {
    #[abi]
    pub event_init_data: TonEventInitData,
    #[abi]
    pub status: EventStatus,
    #[abi(with = "array_uint256_bytes")]
    pub confirms: Vec<UInt256>,
    #[abi(with = "array_uint256_bytes")]
    pub rejects: Vec<UInt256>,
    #[abi(with = "array_uint256_bytes")]
    pub empty: Vec<UInt256>,
    #[abi]
    pub signatures: Vec<Vec<u8>>,
    #[abi(uint128)]
    pub balance: u128,
    #[abi(address)]
    pub initializer: ton_block::MsgAddressInt,
    #[abi(cell)]
    pub meta: ton_types::Cell,
    #[abi(uint32)]
    pub required_votes: u32,
}

#[derive(Debug, PackAbi, UnpackAbi, Clone)]
pub struct TonEventInitData {
    #[abi]
    pub vote_data: TonEventVoteData,
    #[abi(with = "address_only_hash")]
    pub configuration: UInt256,
    #[abi(with = "address_only_hash")]
    pub staking: UInt256,
    #[abi(uint32)]
    pub chain_id: u32,
}

impl TonEventInitData {
    pub fn make_params_tuple() -> ton_abi::ParamType {
        TupleBuilder::new()
            .arg("vote_data", TonEventVoteData::make_params_tuple())
            .arg("configuration", ton_abi::ParamType::Address)
            .arg("staking", ton_abi::ParamType::Address)
            .arg("chain_id", ton_abi::ParamType::Uint(32))
            .build()
    }
}

#[derive(Debug, PackAbi, UnpackAbi, Clone)]
pub struct TonEventVoteData {
    #[abi(with = "uint256_bytes")]
    pub event_transaction: UInt256,
    #[abi(uint64)]
    pub event_transaction_lt: u64,
    #[abi(uint32)]
    pub event_timestamp: u32,
    #[abi(uint32)]
    pub event_index: u32,
    #[abi(cell)]
    pub event_data: ton_types::Cell,
}

impl TonEventVoteData {
    pub fn make_params_tuple() -> ton_abi::ParamType {
        TupleBuilder::new()
            .arg("event_transaction", ton_abi::ParamType::Uint(256))
            .arg("event_transaction_lt", ton_abi::ParamType::Uint(64))
            .arg("event_timestamp", ton_abi::ParamType::Uint(32))
            .arg("event_index", ton_abi::ParamType::Uint(32))
            .arg("event_data", ton_abi::ParamType::Cell)
            .build()
    }
}

#[derive(Debug, PackAbi, UnpackAbi, Copy, Clone, Eq, PartialEq)]
pub enum EventStatus {
    Pending = 0,
    Confirmed = 1,
    Rejected = 2,
}

pub struct EventConfigurationBaseContract<'a>(pub &'a ExistingContract);

impl EventConfigurationBaseContract<'_> {
    pub fn get_type(&self) -> Result<EventType> {
        let mut function = FunctionBuilder::new("getType")
            .time_header()
            .out_arg("_type", ton_abi::ParamType::Uint(8));
        function.make_responsible();

        let inputs = [answer_id()];

        // TODO: rewrite after https://gitlab.dexpa.io/ethereum-freeton-bridge/bridge-contracts/-/issues/51

        let event_type = match self
            .0
            .run_local(&function.clone().build(), &inputs)
            .and_then(|tokens| {
                tokens
                    .unpack_first::<EventType>()
                    .map_err(anyhow::Error::from)
            }) {
            Ok(event_type) => event_type,
            Err(_) => {
                function = function.expire_header();
                self.0
                    .run_local(&function.build(), &inputs)?
                    .unpack_first()?
            }
        };

        Ok(event_type)
    }
}

pub struct EthEventConfigurationContract<'a>(pub &'a ExistingContract);

impl EthEventConfigurationContract<'_> {
    pub fn get_details(&self) -> Result<EthEventConfigurationDetails> {
        let mut function = FunctionBuilder::new("getDetails")
            .time_header()
            .out_arg(
                "basic_configuration",
                BasicConfiguration::make_params_tuple(),
            )
            .out_arg(
                "network_configuration",
                EthEventConfiguration::make_params_tuple(),
            );
        function.make_responsible();

        let details = self
            .0
            .run_local(&function.build(), &[answer_id()])?
            .unpack()?;
        Ok(details)
    }
}

pub struct TonEventConfigurationContract<'a>(pub &'a ExistingContract);

impl TonEventConfigurationContract<'_> {
    pub fn get_details(&self) -> Result<TonEventConfigurationDetails> {
        let mut function = FunctionBuilder::new("getDetails")
            .time_header()
            .expire_header()
            .out_arg(
                "basic_configuration",
                BasicConfiguration::make_params_tuple(),
            )
            .out_arg(
                "network_configuration",
                TonEventConfiguration::make_params_tuple(),
            );
        function.make_responsible();

        let details = self
            .0
            .run_local(&function.build(), &[answer_id()])?
            .unpack()?;
        Ok(details)
    }
}

#[derive(Debug, PackAbiPlain, UnpackAbiPlain, Clone)]
pub struct EthEventConfigurationDetails {
    #[abi]
    pub basic_configuration: BasicConfiguration,
    #[abi]
    pub network_configuration: EthEventConfiguration,
}

#[derive(Debug, PackAbiPlain, UnpackAbiPlain, Clone)]
pub struct TonEventConfigurationDetails {
    #[abi]
    pub basic_configuration: BasicConfiguration,
    #[abi]
    pub network_configuration: TonEventConfiguration,
}

#[derive(Debug, PackAbi, UnpackAbi, Clone)]
pub struct BasicConfiguration {
    #[abi]
    pub event_abi: Vec<u8>,
    #[abi(with = "address_only_hash")]
    pub staking: UInt256,
    #[abi]
    pub event_initial_balance: u128,
    #[abi(cell)]
    pub event_code: ton_types::Cell,
    #[abi(cell)]
    pub meta: ton_types::Cell,
    #[abi(uint32)]
    pub chain_id: u32,
}

impl BasicConfiguration {
    fn make_params_tuple() -> ton_abi::ParamType {
        TupleBuilder::new()
            .arg("event_abi", ton_abi::ParamType::Bytes)
            .arg("staking", ton_abi::ParamType::Address)
            .arg("event_initial_balance", ton_abi::ParamType::Uint(128))
            .arg("event_code", ton_abi::ParamType::Cell)
            .arg("meta", ton_abi::ParamType::Cell)
            .arg("chain_id", ton_abi::ParamType::Uint(32))
            .build()
    }
}

#[derive(Debug, PackAbi, UnpackAbi, Clone)]
pub struct EthEventConfiguration {
    #[abi(with = "uint160_bytes")]
    pub event_emitter: [u8; 20],
    #[abi(uint16)]
    pub event_blocks_to_confirm: u16,
    #[abi(with = "address_only_hash")]
    pub proxy: UInt256,
    #[abi(uint32)]
    pub start_block_number: u32,
}

impl EthEventConfiguration {
    fn make_params_tuple() -> ton_abi::ParamType {
        TupleBuilder::new()
            .arg("event_emitter", ton_abi::ParamType::Uint(160))
            .arg("event_blocks_to_confirm", ton_abi::ParamType::Uint(16))
            .arg("proxy", ton_abi::ParamType::Address)
            .arg("start_block_number", ton_abi::ParamType::Uint(32))
            .build()
    }
}

#[derive(Debug, PackAbi, UnpackAbi, Clone)]
pub struct TonEventConfiguration {
    #[abi(with = "address_only_hash")]
    pub event_emitter: UInt256,
    #[abi(with = "uint160_bytes")]
    pub proxy: [u8; 20],
    #[abi(uint32)]
    pub start_timestamp: u32,
}

impl TonEventConfiguration {
    fn make_params_tuple() -> ton_abi::ParamType {
        TupleBuilder::new()
            .arg("event_emitter", ton_abi::ParamType::Address)
            .arg("proxy", ton_abi::ParamType::Uint(160))
            .arg("start_timestamp", ton_abi::ParamType::Uint(32))
            .build()
    }
}

#[derive(Debug, PackAbi, UnpackAbi, Copy, Clone, Eq, PartialEq)]
pub enum EventType {
    Eth = 0,
    Ton = 1,
}

pub struct BridgeContract<'a>(pub &'a ExistingContract);

impl BridgeContract<'_> {
    pub fn derive_connector_address(&self, id: u64) -> Result<UInt256> {
        let function = FunctionBuilder::new("deriveConnectorAddress")
            .default_headers()
            .in_arg("id", ton_abi::ParamType::Uint(128))
            .out_arg("connector", ton_abi::ParamType::Address);

        let address: ton_block::MsgAddrStd = self
            .0
            .run_local(&function.build(), &[(id as u128).token_value().named("id")])?
            .unpack_first()?;
        Ok(UInt256::from_be_bytes(&address.address.get_bytestring(0)))
    }
}

pub struct ConnectorContract<'a>(pub &'a ExistingContract);

impl ConnectorContract<'_> {
    pub fn get_details(&self) -> Result<ConnectorDetails> {
        let function = FunctionBuilder::new("getDetails")
            .header("time", ton_abi::ParamType::Time)
            .out_arg("id", ton_abi::ParamType::Uint(128))
            .out_arg("eventConfiguration", ton_abi::ParamType::Address)
            .out_arg("enabled", ton_abi::ParamType::Bool);

        let mut result = self.0.run_local(&function.build(), &[])?.into_unpacker();
        let _id: u128 = result.unpack_next()?;
        let event_configuration: ton_block::MsgAddrStd = result.unpack_next()?;
        let enabled: bool = result.unpack_next()?;

        Ok(ConnectorDetails {
            event_configuration: UInt256::from_be_bytes(
                &event_configuration.address.get_bytestring(0),
            ),
            enabled,
        })
    }
}

#[derive(Debug, Copy, Clone)]
pub struct ConnectorDetails {
    pub event_configuration: UInt256,
    pub enabled: bool,
}
