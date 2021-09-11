use nekoton_abi::*;
use ton_types::UInt256;

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

#[derive(Debug, Clone, PackAbi, UnpackAbi, KnownParamType)]
pub struct TonEventInitData {
    #[abi]
    pub vote_data: TonEventVoteData,
    #[abi(with = "address_only_hash")]
    pub configuration: UInt256,
    #[abi(with = "address_only_hash")]
    pub staking: UInt256,
}

#[derive(Debug, Clone, PackAbi, UnpackAbi, KnownParamType)]
pub struct TonEventVoteData {
    #[abi(uint64)]
    pub event_transaction_lt: u64,
    #[abi(uint32)]
    pub event_timestamp: u32,
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

#[derive(Debug, Copy, Clone, Eq, PartialEq, PackAbi, UnpackAbi, KnownParamType)]
pub enum EventVote {
    Reserved = 0,
    Empty = 1,
    Confirm = 2,
    Reject = 3,
}

#[derive(Debug, Clone, PackAbiPlain, UnpackAbiPlain, KnownParamTypePlain)]
pub struct EthEventConfigurationDetails {
    #[abi]
    pub basic_configuration: BasicConfiguration,
    #[abi]
    pub network_configuration: EthEventConfiguration,
    #[abi(cell)]
    pub meta: ton_types::Cell,
}

#[derive(Debug, Clone, PackAbiPlain, UnpackAbiPlain, KnownParamTypePlain)]
pub struct TonEventConfigurationDetails {
    #[abi]
    pub basic_configuration: BasicConfiguration,
    #[abi]
    pub network_configuration: TonEventConfiguration,
    #[abi(cell)]
    pub meta: ton_types::Cell,
}

#[derive(Debug, Clone, PackAbi, UnpackAbi, KnownParamType)]
pub struct BasicConfiguration {
    #[abi(string)]
    pub event_abi: String,
    #[abi(with = "address_only_hash")]
    pub staking: UInt256,
    #[abi(uint64)]
    pub event_initial_balance: u64,
    #[abi(cell)]
    pub event_code: ton_types::Cell,
}

#[derive(Debug, Clone, PackAbi, UnpackAbi, KnownParamType)]
pub struct EthEventConfiguration {
    #[abi(uint32)]
    pub chain_id: u32,
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

impl std::fmt::Display for EventType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Eth => f.write_str("ETH"),
            Self::Ton => f.write_str("TON"),
        }
    }
}

#[derive(Debug, Clone, PackAbiPlain, UnpackAbiPlain, KnownParamTypePlain)]
pub struct BridgeDetails {
    #[abi(cell)]
    pub connector_code: ton_types::Cell,
    #[abi(uint64)]
    pub connector_deploy_value: u64,
    #[abi(uint64)]
    pub connector_counter: u64,
    #[abi(with = "address_only_hash")]
    pub staking: UInt256,
    #[abi(bool)]
    pub active: bool,
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
pub struct StakerAddresses {
    #[abi(with = "array_address_only_hash")]
    pub items: Vec<UInt256>,
}

#[derive(Debug, Clone, PackAbiPlain, UnpackAbiPlain, KnownParamTypePlain)]
pub struct RelayKeys {
    #[abi(with = "array_uint256_bytes")]
    pub items: Vec<UInt256>,
}

#[derive(Debug, Clone, PackAbi, UnpackAbi, KnownParamType)]
pub struct UserDataDetails {
    #[abi(uint128)]
    pub token_balance: u128,
    #[abi(uint32)]
    pub relay_lock_until: u32,
    #[abi(array)]
    pub reward_rounds: Vec<UserDataRewardRound>,
    #[abi(with = "uint160_bytes")]
    pub relay_eth_address: [u8; 20],
    #[abi(bool)]
    pub eth_address_confirmed: bool,
    #[abi(with = "uint256_bytes")]
    pub relay_ton_pubkey: UInt256,
    #[abi(bool)]
    pub ton_pubkey_confirmed: bool,
}

#[derive(Debug, Clone, PackAbi, UnpackAbi, KnownParamType)]
pub struct UserDataRewardRound {
    #[abi(uint128)]
    pub reward_balance: u128,
    #[abi(uint128)]
    pub reward_debt: u128,
}

#[derive(Debug, Clone, PackAbi, UnpackAbi, KnownParamType)]
pub struct StakingDetails {
    #[abi(with = "address_only_hash")]
    pub dao_root: UInt256,
    #[abi(with = "address_only_hash")]
    pub bridge_event_config_eth_ton: UInt256,
    #[abi(with = "address_only_hash")]
    pub bridge_event_config_ton_eth: UInt256,
    #[abi(with = "address_only_hash")]
    pub token_root: UInt256,
    #[abi(with = "address_only_hash")]
    pub token_wallet: UInt256,
    #[abi(with = "address_only_hash")]
    pub admin: UInt256,
    #[abi(with = "address_only_hash")]
    pub rewarder: UInt256,
    #[abi(uint128)]
    pub token_balance: u128,
    #[abi(uint128)]
    pub reward_token_balance: u128,
    #[abi(uint128)]
    pub reward_per_second: u128,
    #[abi(uint32)]
    pub last_reward_time: u32,
    #[abi(array)]
    pub reward_rounds: Vec<RewardRound>,
}

#[derive(Debug, Clone, PackAbi, UnpackAbi, KnownParamType)]
pub struct RelayRoundsDetails {
    #[abi(bool)]
    pub origin_relay_round_initialized: bool,
    #[abi(uint32)]
    pub current_relay_round: u32,
    #[abi(uint32)]
    pub current_relay_round_start_time: u32,
    #[abi(uint32)]
    pub current_election_start_time: u32,
    #[abi(uint32)]
    pub prev_relay_round_end_time: u32,
    #[abi(uint32)]
    pub pending_relay_round: u32,
}

#[derive(Debug, Clone, PackAbi, UnpackAbi, KnownParamType)]
pub struct RewardRound {
    #[abi(with = "uint256_bytes")]
    pub account_reward_per_share: UInt256,
    #[abi(uint128)]
    pub reward_tokens: u128,
    #[abi(uint128)]
    pub total_reward: u128,
    #[abi(uint32)]
    pub start_time: u32,
}

#[derive(Debug, Clone, PackAbiPlain, UnpackAbiPlain, KnownParamTypePlain)]
pub struct ElectionStartedEvent {
    #[abi(uint32)]
    pub round_num: u32,
    #[abi(uint32)]
    pub election_start_time: u32,
    #[abi(with = "address_only_hash")]
    pub election_addr: UInt256,
}

#[derive(Debug, Clone, PackAbiPlain, UnpackAbiPlain, KnownParamTypePlain)]
pub struct ElectionEndedEvent {
    #[abi(uint32)]
    pub round_num: u32,
    #[abi(uint32)]
    pub relay_requests: u32,
    #[abi(bool)]
    pub min_relays_ok: bool,
}

#[derive(Debug, Clone, PackAbiPlain, UnpackAbiPlain, KnownParamTypePlain)]
pub struct RelayMembershipRequestedEvent {
    #[abi(uint32)]
    pub round_num: u32,
    #[abi(uint128)]
    pub tokens: u128,
    #[abi(with = "uint256_bytes")]
    pub ton_pubkey: UInt256,
    #[abi(with = "uint160_bytes")]
    pub eth_address: [u8; 20],
    #[abi(uint32)]
    pub lock_until: u32,
}
