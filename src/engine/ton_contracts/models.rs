use nekoton_abi::*;
use ton_types::UInt256;

#[derive(Debug, Clone, PackAbiPlain, UnpackAbiPlain)]
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

#[derive(Debug, Clone, PackAbi, UnpackAbi)]
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

#[derive(Debug, Clone, PackAbi, UnpackAbi)]
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

#[derive(Debug, Clone, PackAbiPlain, UnpackAbiPlain)]
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
    #[abi(array, bytes)]
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

#[derive(Debug, Clone, PackAbi, UnpackAbi)]
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

#[derive(Debug, Clone, PackAbi, UnpackAbi)]
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

#[derive(Debug, Copy, Clone, Eq, PartialEq, PackAbi, UnpackAbi)]
pub enum EventStatus {
    Pending = 0,
    Confirmed = 1,
    Rejected = 2,
}

#[derive(Debug, Clone, PackAbiPlain, UnpackAbiPlain)]
pub struct EthEventConfigurationDetails {
    #[abi]
    pub basic_configuration: BasicConfiguration,
    #[abi]
    pub network_configuration: EthEventConfiguration,
}

#[derive(Debug, Clone, PackAbiPlain, UnpackAbiPlain)]
pub struct TonEventConfigurationDetails {
    #[abi]
    pub basic_configuration: BasicConfiguration,
    #[abi]
    pub network_configuration: TonEventConfiguration,
}

#[derive(Debug, Clone, PackAbi, UnpackAbi)]
pub struct BasicConfiguration {
    #[abi]
    pub event_abi: Vec<u8>,
    #[abi(with = "address_only_hash")]
    pub staking: UInt256,
    #[abi]
    pub event_initial_balance: u64,
    #[abi(cell)]
    pub event_code: ton_types::Cell,
    #[abi(cell)]
    pub meta: ton_types::Cell,
    #[abi(uint32)]
    pub chain_id: u32,
}

impl BasicConfiguration {
    pub fn make_params_tuple() -> ton_abi::ParamType {
        TupleBuilder::new()
            .arg("event_abi", ton_abi::ParamType::Bytes)
            .arg("staking", ton_abi::ParamType::Address)
            .arg("event_initial_balance", ton_abi::ParamType::Uint(64))
            .arg("event_code", ton_abi::ParamType::Cell)
            .arg("meta", ton_abi::ParamType::Cell)
            .arg("chain_id", ton_abi::ParamType::Uint(32))
            .build()
    }
}

#[derive(Debug, Clone, PackAbi, UnpackAbi)]
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
    pub fn make_params_tuple() -> ton_abi::ParamType {
        TupleBuilder::new()
            .arg("event_emitter", ton_abi::ParamType::Uint(160))
            .arg("event_blocks_to_confirm", ton_abi::ParamType::Uint(16))
            .arg("proxy", ton_abi::ParamType::Address)
            .arg("start_block_number", ton_abi::ParamType::Uint(32))
            .build()
    }
}

#[derive(Debug, Clone, PackAbi, UnpackAbi)]
pub struct TonEventConfiguration {
    #[abi(with = "address_only_hash")]
    pub event_emitter: UInt256,
    #[abi(with = "uint160_bytes")]
    pub proxy: [u8; 20],
    #[abi(uint32)]
    pub start_timestamp: u32,
}

impl TonEventConfiguration {
    pub fn make_params_tuple() -> ton_abi::ParamType {
        TupleBuilder::new()
            .arg("event_emitter", ton_abi::ParamType::Address)
            .arg("proxy", ton_abi::ParamType::Uint(160))
            .arg("start_timestamp", ton_abi::ParamType::Uint(32))
            .build()
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, PackAbi, UnpackAbi)]
pub enum EventType {
    Eth = 0,
    Ton = 1,
}

#[derive(Debug, Copy, Clone, PackAbiPlain, UnpackAbiPlain)]
pub struct ConnectorDetails {
    #[abi(uint64)]
    pub id: u64,
    #[abi(with = "address_only_hash")]
    pub event_configuration: UInt256,
    #[abi(bool)]
    pub enabled: bool,
}

#[derive(Debug, Clone, PackAbiPlain, UnpackAbiPlain)]
pub struct ConnectorDeployedEvent {
    #[abi(uint64)]
    pub id: u64,
    #[abi(with = "address_only_hash")]
    pub connector: UInt256,
    #[abi(with = "address_only_hash")]
    pub event_configuration: UInt256,
}

#[derive(Debug, Clone, PackAbiPlain, UnpackAbiPlain)]
pub struct RelayRoundInitializedEvent {
    #[abi(uint128)]
    pub round_num: u128,
    #[abi(uint128)]
    pub round_start_time: u128,
    #[abi(address)]
    pub round_addr: ton_block::MsgAddressInt,
    #[abi(uint128)]
    pub relays_count: u128,
    #[abi(bool)]
    pub duplicate: bool,
}
