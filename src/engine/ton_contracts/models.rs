use nekoton_abi::*;
use ton_types::UInt256;

#[derive(Debug, Clone, PackAbiPlain, UnpackAbiPlain, KnownParamTypePlain)]
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

#[derive(Debug, Clone, PackAbi, UnpackAbi, KnownParamType)]
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

#[derive(Debug, Clone, PackAbi, UnpackAbi, KnownParamType)]
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

#[derive(Debug, Clone, PackAbiPlain, UnpackAbiPlain, KnownParamTypePlain)]
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

#[derive(Debug, Clone, PackAbi, UnpackAbi, KnownParamType)]
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

#[derive(Debug, Clone, PackAbi, UnpackAbi, KnownParamType)]
pub struct TonEventVoteData {
    #[abi(uint64)]
    pub event_transaction_lt: u64,
    #[abi(uint32)]
    pub event_timestamp: u32,
    #[abi(uint32)]
    pub event_index: u32,
    #[abi(cell)]
    pub event_data: ton_types::Cell,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, PackAbi, UnpackAbi, KnownParamType)]
pub enum EventStatus {
    Initializing = 0,
    Pending = 1,
    Confirmed = 2,
    Rejected = 3,
}

#[derive(Debug, Clone, PackAbiPlain, UnpackAbiPlain, KnownParamTypePlain)]
pub struct EthEventConfigurationDetails {
    #[abi]
    pub basic_configuration: BasicConfiguration,
    #[abi]
    pub network_configuration: EthEventConfiguration,
}

#[derive(Debug, Clone, PackAbiPlain, UnpackAbiPlain, KnownParamTypePlain)]
pub struct TonEventConfigurationDetails {
    #[abi]
    pub basic_configuration: BasicConfiguration,
    #[abi]
    pub network_configuration: TonEventConfiguration,
}

#[derive(Debug, Clone, PackAbi, UnpackAbi, KnownParamType)]
pub struct BasicConfiguration {
    #[abi(string)]
    pub event_abi: String,
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

#[derive(Debug, Clone, PackAbi, UnpackAbi, KnownParamType)]
pub struct EthEventConfiguration {
    #[abi(with = "uint160_bytes")]
    pub event_emitter: [u8; 20],
    #[abi(uint16)]
    pub event_blocks_to_confirm: u16,
    #[abi(with = "address_only_hash")]
    pub proxy: UInt256,
    #[abi(uint32)]
    pub start_block_number: u32,
    #[abi(uint32)]
    pub end_block_number: u32,
}

#[derive(Debug, Clone, PackAbi, UnpackAbi, KnownParamType)]
pub struct TonEventConfiguration {
    #[abi(with = "address_only_hash")]
    pub event_emitter: UInt256,
    #[abi(with = "uint160_bytes")]
    pub proxy: [u8; 20],
    #[abi(uint32)]
    pub start_timestamp: u32,
    #[abi(uint32)]
    pub end_timestamp: u32,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, PackAbi, UnpackAbi, KnownParamType)]
pub enum EventType {
    Eth = 0,
    Ton = 1,
}

#[derive(Debug, Clone, PackAbi, UnpackAbi, KnownParamType)]
pub struct BridgeConfiguration {
    #[abi(with = "address_only_hash")]
    pub staking: UInt256,
    #[abi(bool)]
    pub active: bool,
    #[abi(cell)]
    pub connector_code: ton_types::Cell,
    #[abi(uint64)]
    pub connector_deploy_value: u64,
}

#[derive(Debug, Copy, Clone, PackAbiPlain, UnpackAbiPlain, KnownParamTypePlain)]
pub struct ConnectorDetails {
    #[abi(uint64)]
    pub id: u64,
    #[abi(with = "address_only_hash")]
    pub event_configuration: UInt256,
    #[abi(bool)]
    pub enabled: bool,
}

#[derive(Debug, Clone, PackAbiPlain, UnpackAbiPlain, KnownParamTypePlain)]
pub struct ConnectorDeployedEvent {
    #[abi(uint64)]
    pub id: u64,
    #[abi(with = "address_only_hash")]
    pub connector: UInt256,
    #[abi(with = "address_only_hash")]
    pub event_configuration: UInt256,
}

#[derive(Debug, Clone, PackAbiPlain, UnpackAbiPlain, KnownParamTypePlain)]
pub struct RelayRoundInitializedEvent {
    #[abi(uint32)]
    pub round_num: u32,
    #[abi(uint32)]
    pub round_start_time: u32,
    #[abi(with = "address_only_hash")]
    pub round_addr: UInt256,
    #[abi(uint32)]
    pub relays_count: u32,
    #[abi(bool)]
    pub duplicate: bool,
}

#[derive(Debug, Clone, PackAbiPlain, UnpackAbiPlain, KnownParamTypePlain)]
pub struct RelayKeys {
    #[abi(with = "array_uint256_bytes")]
    pub items: Vec<UInt256>,
}
