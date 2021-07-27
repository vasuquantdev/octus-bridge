use std::hash::Hash;
use std::io::Write;

use borsh::{BorshDeserialize, BorshSerialize};
use nekoton_parser::abi::{
    IntoUnpacker, PackAbi, StandaloneToken, TokenValueExt, UnpackAbi, UnpackToken,
};
use primitive_types::H256;
use ton_abi::Token;
use ton_block::{Deserializable, Serializable};

use crate::contracts::errors::*;
use crate::contracts::prelude::*;
use crate::models::*;
use crate::prelude::*;

// Events

crate::define_event!(BridgeContractEvent, BridgeContractEventKind, {
    EventConfigurationCreationVote { id: u32, relay: MsgAddrStd, vote: Voting },
    EventConfigurationCreationEnd {
        id: u32,
        active: bool,
        address: MsgAddressInt,
        event_type: EventType
    },

    EventConfigurationUpdateVote { id: u32, relay: MsgAddrStd, vote: Voting },
    EventConfigurationUpdateEnd {
        id: u32,
        active: bool,
        address: MsgAddressInt,
        event_type: EventType
    },

    BridgeConfigurationUpdateVote {
        bridge_configuration: BridgeConfiguration,
        relay: MsgAddrStd,
        vote: VoteData,
    },
    BridgeConfigurationUpdateEnd {
        bridge_configuration: BridgeConfiguration,
        status: bool,
    },

    BridgeRelaysUpdateVote {
        target: RelayUpdate,
        relay: MsgAddrStd,
        vote: VoteData
    },
    BridgeRelaysUpdateEnd {
        target: RelayUpdate,
        active: bool
    },

    OwnershipGranted { relay: MsgAddrStd },
    OwnershipRemoved { relay: MsgAddrStd },
});

impl TryFrom<(BridgeContractEventKind, Vec<Token>)> for BridgeContractEvent {
    type Error = ContractError;

    fn try_from((kind, tokens): (BridgeContractEventKind, Vec<Token>)) -> ContractResult<Self> {
        let mut tokens = tokens.into_iter();
        Ok(match kind {
            BridgeContractEventKind::EventConfigurationCreationVote => {
                BridgeContractEvent::EventConfigurationCreationVote {
                    id: tokens.next().unpack()?,
                    relay: tokens.next().unpack()?,
                    vote: tokens.next().unpack()?,
                }
            }
            BridgeContractEventKind::EventConfigurationCreationEnd => {
                BridgeContractEvent::EventConfigurationCreationEnd {
                    id: tokens.next().unpack()?,
                    active: tokens.next().unpack()?,
                    address: tokens.next().unpack()?,
                    event_type: tokens.next().unpack()?,
                }
            }
            BridgeContractEventKind::EventConfigurationUpdateVote => {
                BridgeContractEvent::EventConfigurationUpdateVote {
                    id: tokens.next().unpack()?,
                    relay: tokens.next().unpack()?,
                    vote: tokens.next().unpack()?,
                }
            }
            BridgeContractEventKind::EventConfigurationUpdateEnd => {
                BridgeContractEvent::EventConfigurationUpdateEnd {
                    id: tokens.next().unpack()?,
                    active: tokens.next().unpack()?,
                    address: tokens.next().unpack()?,
                    event_type: tokens.next().unpack()?,
                }
            }
            BridgeContractEventKind::BridgeConfigurationUpdateVote => {
                BridgeContractEvent::BridgeConfigurationUpdateVote {
                    bridge_configuration: tokens.next().unpack()?,
                    relay: tokens.next().unpack()?,
                    vote: tokens.next().unpack()?,
                }
            }
            BridgeContractEventKind::BridgeConfigurationUpdateEnd => {
                BridgeContractEvent::BridgeConfigurationUpdateEnd {
                    bridge_configuration: tokens.next().unpack()?,
                    status: tokens.next().unpack()?,
                }
            }
            BridgeContractEventKind::BridgeRelaysUpdateVote => {
                BridgeContractEvent::BridgeRelaysUpdateVote {
                    target: tokens.next().unpack()?,
                    relay: tokens.next().unpack()?,
                    vote: tokens.next().unpack()?,
                }
            }
            BridgeContractEventKind::BridgeRelaysUpdateEnd => {
                BridgeContractEvent::BridgeRelaysUpdateEnd {
                    target: tokens.next().unpack()?,
                    active: tokens.next().unpack()?,
                }
            }
            BridgeContractEventKind::OwnershipGranted => BridgeContractEvent::OwnershipGranted {
                relay: tokens.next().unpack()?,
            },
            BridgeContractEventKind::OwnershipRemoved => BridgeContractEvent::OwnershipRemoved {
                relay: tokens.next().unpack()?,
            },
        })
    }
}

crate::define_event!(
    TonEventConfigurationContractEvent,
    TonEventConfigurationContractEventKind,
    {
        EventConfirmation {
            address: MsgAddrStd,
            relay: MsgAddrStd,
        },
        EventReject {
            address: MsgAddrStd,
            relay: MsgAddrStd,
        }
    }
);

impl TryFrom<(TonEventConfigurationContractEventKind, Vec<Token>)>
    for TonEventConfigurationContractEvent
{
    type Error = ContractError;

    fn try_from(
        (kind, tokens): (TonEventConfigurationContractEventKind, Vec<Token>),
    ) -> Result<Self, Self::Error> {
        let mut tokens = tokens.into_iter();

        Ok(match kind {
            TonEventConfigurationContractEventKind::EventConfirmation => {
                TonEventConfigurationContractEvent::EventConfirmation {
                    address: tokens.next().unpack()?,
                    relay: tokens.next().unpack()?,
                }
            }
            TonEventConfigurationContractEventKind::EventReject => {
                TonEventConfigurationContractEvent::EventReject {
                    address: tokens.next().unpack()?,
                    relay: tokens.next().unpack()?,
                }
            }
        })
    }
}

crate::define_event!(
    EthEventConfigurationContractEvent,
    EthEventConfigurationContractEventKind,
    {
        EventConfirmation {
            address: MsgAddrStd,
            relay: MsgAddrStd,
        },
        EventReject {
            address: MsgAddrStd,
            relay: MsgAddrStd,
        }
    }
);

impl TryFrom<(EthEventConfigurationContractEventKind, Vec<Token>)>
    for EthEventConfigurationContractEvent
{
    type Error = ContractError;

    fn try_from(
        (kind, tokens): (EthEventConfigurationContractEventKind, Vec<Token>),
    ) -> Result<Self, Self::Error> {
        let mut tokens = tokens.into_iter();

        Ok(match kind {
            EthEventConfigurationContractEventKind::EventConfirmation => {
                EthEventConfigurationContractEvent::EventConfirmation {
                    address: tokens.next().unpack()?,
                    relay: tokens.next().unpack()?,
                }
            }
            EthEventConfigurationContractEventKind::EventReject => {
                EthEventConfigurationContractEvent::EventReject {
                    address: tokens.next().unpack()?,
                    relay: tokens.next().unpack()?,
                }
            }
        })
    }
}

// Models

#[derive(UnpackAbi)]
pub struct EthereumAccount {
    #[abi(name = "ethereumAccount", unpack_with = "unpack_h160")]
    ethereum_account: primitive_types::H160,
}

impl StandaloneToken for EthereumAccount {}

#[derive(PackAbi, UnpackAbi, Debug, Clone, Eq, PartialEq)]
pub struct BridgeConfiguration {
    #[abi(uint16)]
    pub nonce: u16,
    #[abi(uint16, name = "bridgeUpdateRequiredConfirmations")]
    pub bridge_update_required_confirmations: u16,
    #[abi(uint16, name = "bridgeUpdateRequiredRejects")]
    pub bridge_update_required_rejects: u16,
    #[abi(bool)]
    pub active: bool,
}

impl StandaloneToken for BridgeConfiguration {}

#[derive(Debug, Clone)]
pub struct ActiveEventConfiguration {
    pub id: u32,
    pub address: MsgAddressInt,
    pub event_type: EventType,
}

impl TryFrom<ContractOutput> for Vec<ActiveEventConfiguration> {
    type Error = ContractError;

    fn try_from(value: ContractOutput) -> Result<Self, Self::Error> {
        let mut tokens = value.tokens.into_unpacker();
        let ids: Vec<u32> = tokens.unpack_next()?;
        let addrs: Vec<MsgAddressInt> = tokens.unpack_next()?;
        let types: Vec<EventType> = tokens.unpack_next()?;
        if ids.len() != addrs.len() || ids.len() != types.len() {
            return Err(ContractError::InvalidAbi);
        }

        Ok(ids
            .into_iter()
            .zip(addrs.into_iter())
            .zip(types.into_iter())
            .map(|((id, address), event_type)| ActiveEventConfiguration {
                id,
                address,
                event_type,
            })
            .collect())
    }
}

#[derive(PackAbi, UnpackAbi, Debug, Clone)]
pub struct RelayUpdate {
    #[abi(uint16)]
    pub nonce: u16,
    #[abi(int8)]
    pub wid: i8,
    #[abi(with = "nekoton_parser::abi::uint256_bytes")]
    pub addr: UInt256,
    #[abi(name = "ethereumAccount", with = "nekoton_parser::abi::uint256_bytes")]
    pub ethereum_account: UInt256,
    #[abi]
    pub action: RelayUpdateAction,
}

#[derive(PackAbi, UnpackAbi, Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub enum RelayUpdateAction {
    Remove = 0,
    Add = 1,
}

impl StandaloneToken for RelayUpdateAction {}

#[derive(PackAbi, UnpackAbi, Debug, Clone)]
pub struct VoteData {
    #[abi]
    pub signature: Vec<u8>,
}

impl VoteData {
    pub fn is_empty(&self) -> bool {
        self.signature.is_empty()
    }
}

impl StandaloneToken for VoteData {}

#[derive(UnpackAbi, Debug, Clone, Default)]
pub struct CommonEventConfigurationParams {
    #[abi(string, name = "eventABI")]
    pub event_abi: String,
    #[abi(uint16, name = "eventRequiredConfirmations")]
    pub event_required_confirmations: u16,
    #[abi(uint16, name = "eventRequiredRejects")]
    pub event_required_rejects: u16,
    #[abi(cell, name = "eventCode")]
    pub event_code: Cell,
    #[abi(address, name = "bridgeAddress")]
    pub bridge_address: MsgAddressInt,
    #[abi(
        name = "eventInitialBalance",
        with = "nekoton_parser::abi::uint128_number"
    )]
    pub event_initial_balance: BigUint,
    #[abi(cell)]
    pub meta: Cell,
}

#[derive(UnpackAbi, Debug, Clone)]
pub struct EthEventConfiguration {
    pub common: CommonEventConfigurationParams,
    #[abi(name = "eventAddress", unpack_with = "unpack_h160")]
    pub event_address: EthAddress,
    #[abi(uint16, name = "eventBlocksToConfirm")]
    pub event_blocks_to_confirm: u16,
    #[abi(address, name = "proxyAddress")]
    pub proxy_address: MsgAddressInt,
    #[abi(uint32, name = "startBlockNumber")]
    pub start_block_number: u32,
}

impl TryFrom<ContractOutput> for EthEventConfiguration {
    type Error = ContractError;

    fn try_from(output: ContractOutput) -> ContractResult<EthEventConfiguration> {
        let mut tuple = output.tokens.into_unpacker();

        let common: CommonEventConfigurationParams = tuple.unpack_next()?;
        let eth_event: EthEventConfiguration = tuple.unpack_next()?;

        Ok(EthEventConfiguration {
            common,
            event_address: eth_event.event_address,
            event_blocks_to_confirm: eth_event.event_blocks_to_confirm,
            proxy_address: eth_event.proxy_address,
            start_block_number: eth_event.start_block_number,
        })
    }
}

#[derive(UnpackAbi, Debug, Clone)]
pub struct TonEventConfiguration {
    pub common: CommonEventConfigurationParams,
    #[abi(address, name = "eventAddress")]
    pub event_address: MsgAddressInt,
    #[abi(name = "proxyAddress", unpack_with = "unpack_h160")]
    pub proxy_address: primitive_types::H160,
    #[abi(uint32, name = "startTimestamp")]
    pub start_timestamp: u32,
}

impl TryFrom<ContractOutput> for TonEventConfiguration {
    type Error = ContractError;

    fn try_from(output: ContractOutput) -> Result<Self, Self::Error> {
        let mut tuple = output.tokens.into_unpacker();

        let common: CommonEventConfigurationParams = tuple.unpack_next()?;
        let ton_event: TonEventConfiguration = tuple.unpack_next()?;

        Ok(TonEventConfiguration {
            common,
            event_address: ton_event.event_address,
            proxy_address: ton_event.proxy_address,
            start_timestamp: ton_event.start_timestamp,
        })
    }
}

#[derive(UnpackAbi, Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
pub enum EventStatus {
    InProcess = 0,
    Confirmed = 1,
    Rejected = 2,
}

impl StandaloneToken for EventStatus {}

#[derive(UnpackAbi, Debug, Clone)]
pub struct TonEventInitData {
    #[abi(name = "eventTransaction", with = "nekoton_parser::abi::uint256_bytes")]
    pub event_transaction: UInt256,
    #[abi(uint64, name = "eventTransactionLt")]
    pub event_transaction_lt: u64,
    #[abi(uint32, name = "eventTimestamp")]
    pub event_timestamp: u32,
    #[abi(uint32, name = "eventIndex")]
    pub event_index: u32,
    #[abi(cell, name = "eventData")]
    pub event_data: Cell,
    #[abi(address, name = "tonEventConfiguration")]
    pub ton_event_configuration: MsgAddressInt,
    #[abi(uint16, name = "requiredConfirmations")]
    pub required_confirmations: u16,
    #[abi(uint16, name = "requiredRejects")]
    pub required_rejections: u16,
    #[abi(cell, name = "configurationMeta")]
    pub configuration_meta: Cell,
}

#[derive(PackAbi, Debug, Clone)]
pub struct TonEventVoteData {
    /// Not an argument for FunctionArg!
    pub configuration_id: u32,
    #[abi(name = "eventTransaction")]
    pub event_transaction: UInt256,
    #[abi(name = "eventTransactionLt")]
    pub event_transaction_lt: u64,
    #[abi(name = "eventTimestamp")]
    pub event_timestamp: u32,
    #[abi(name = "eventIndex")]
    pub event_index: u32,
    #[abi(name = "eventData")]
    pub event_data: Cell,
}

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
struct StoredTonEventVotingData {
    pub configuration_id: u32,
    pub event_transaction: Vec<u8>,
    pub event_transaction_lt: u64,
    pub event_timestamp: u32,
    pub event_index: u32,
    pub event_data: Vec<u8>,
}

impl BorshSerialize for TonEventVoteData {
    fn serialize<W: Write>(&self, writer: &mut W) -> std::io::Result<()> {
        let event_data = self
            .event_data
            .write_to_bytes()
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;

        StoredTonEventVotingData {
            configuration_id: self.configuration_id,
            event_transaction: self.event_transaction.as_slice().to_vec(),
            event_transaction_lt: self.event_transaction_lt,
            event_timestamp: self.event_timestamp,
            event_index: self.event_index,
            event_data,
        }
        .serialize(writer)
    }
}

impl BorshDeserialize for TonEventVoteData {
    fn deserialize(buf: &mut &[u8]) -> std::io::Result<Self> {
        let stored: StoredTonEventVotingData = BorshDeserialize::deserialize(buf)?;

        let event_data = Cell::construct_from_bytes(&stored.event_data)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;

        Ok(Self {
            configuration_id: stored.configuration_id,
            event_transaction: UInt256::from_be_bytes(&stored.event_transaction),
            event_transaction_lt: stored.event_transaction_lt,
            event_timestamp: stored.event_timestamp,
            event_index: stored.event_index,
            event_data,
        })
    }
}

#[derive(Debug, Clone)]
pub struct TonEventDetails {
    pub init_data: TonEventInitData,
    pub status: EventStatus,
    pub confirm_keys: Vec<MsgAddrStd>,
    pub reject_keys: Vec<MsgAddrStd>,
    pub event_data_signatures: Vec<Vec<u8>>,
}

impl TryFrom<ContractOutput> for TonEventDetails {
    type Error = ContractError;

    fn try_from(output: ContractOutput) -> ContractResult<Self> {
        let mut tuple = output.tokens.into_unpacker();

        Ok(TonEventDetails {
            init_data: tuple.unpack_next()?,
            status: tuple.unpack_next()?,
            confirm_keys: tuple.unpack_next()?,
            reject_keys: tuple.unpack_next()?,
            event_data_signatures: tuple.unpack_next()?,
        })
    }
}

#[derive(UnpackAbi, Debug, Clone)]
pub struct EthEventInitData {
    #[abi(name = "eventTransaction", unpack_with = "unpack_h256")]
    pub event_transaction: primitive_types::H256,
    #[abi(uint32, name = "eventIndex")]
    pub event_index: u32,
    #[abi(cell, name = "eventData")]
    pub event_data: Cell,
    #[abi(uint32, name = "eventBlockNumber")]
    pub event_block_number: u32,
    #[abi(name = "eventBlock", unpack_with = "unpack_h256")]
    pub event_block: primitive_types::H256,
    #[abi(address, name = "ethereumEventConfiguration")]
    pub eth_event_configuration: MsgAddressInt,
    #[abi(uint16, name = "requiredConfirmations")]
    pub required_confirmations: u16,
    #[abi(uint16, name = "requiredRejects")]
    pub required_rejections: u16,
    #[abi(address, name = "proxyAddress")]
    pub proxy_address: MsgAddressInt,
    #[abi(cell, name = "configurationMeta")]
    pub configuration_meta: Cell,
}

#[derive(PackAbi, Debug, Clone)]
pub struct EthEventVoteData {
    /// Not serializable!
    pub configuration_id: u32,
    #[abi(name = "eventTransaction", pack_with = "pack_h256")]
    pub event_transaction: primitive_types::H256,
    #[abi(name = "eventIndex")]
    pub event_index: u32,
    #[abi(name = "eventData")]
    pub event_data: Cell,
    #[abi(name = "eventBlockNumber")]
    pub event_block_number: u32,
    #[abi(name = "eventBlock", pack_with = "pack_h256")]
    pub event_block: primitive_types::H256,
}

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub struct StoredEthEventVoteData {
    pub configuration_id: u32,
    pub event_transaction: Vec<u8>,
    pub event_index: u32,
    pub event_data: Vec<u8>,
    pub event_block_number: u32,
    pub event_block: Vec<u8>,
}

impl BorshSerialize for EthEventVoteData {
    fn serialize<W: Write>(&self, writer: &mut W) -> std::io::Result<()> {
        let event_data = self
            .event_data
            .write_to_bytes()
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;

        StoredEthEventVoteData {
            configuration_id: self.configuration_id,
            event_transaction: self.event_transaction.as_bytes().to_vec(),
            event_index: self.event_index,
            event_data,
            event_block_number: self.event_block_number,
            event_block: self.event_block.as_bytes().to_vec(),
        }
        .serialize(writer)
    }
}

impl BorshDeserialize for EthEventVoteData {
    fn deserialize(buf: &mut &[u8]) -> std::io::Result<Self> {
        let stored: StoredEthEventVoteData = BorshDeserialize::deserialize(buf)?;

        let event_data = Cell::construct_from_bytes(&stored.event_data)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;

        let event_transaction: UInt256 = stored.event_transaction.into();
        let event_block: UInt256 = stored.event_block.into();

        Ok(Self {
            configuration_id: stored.configuration_id,
            event_transaction: H256::from_slice(event_transaction.as_slice()),
            event_index: stored.event_index,
            event_data,
            event_block_number: stored.event_block_number,
            event_block: H256::from_slice(event_block.as_slice()),
        })
    }
}

#[derive(Debug, Clone)]
pub struct EthEventDetails {
    pub init_data: EthEventInitData,
    pub status: EventStatus,
    pub confirm_relays: Vec<MsgAddrStd>,
    pub reject_relays: Vec<MsgAddrStd>,
}

impl TryFrom<ContractOutput> for EthEventDetails {
    type Error = ContractError;

    fn try_from(output: ContractOutput) -> ContractResult<Self> {
        let mut tuple = output.tokens.into_unpacker();

        Ok(EthEventDetails {
            init_data: tuple.unpack_next()?,
            status: tuple.unpack_next()?,
            confirm_relays: tuple.unpack_next()?,
            reject_relays: tuple.unpack_next()?,
        })
    }
}

#[allow(clippy::upper_case_acronyms)]
#[derive(PackAbi, UnpackAbi, Debug, Clone, Copy, Eq, PartialEq, Deserialize, Serialize)]
pub enum EventType {
    ETH = 0,
    TON = 1,
}

impl StandaloneToken for EventType {}

#[derive(PackAbi, UnpackAbi, Debug, Clone, Copy, Eq, PartialEq, Deserialize, Serialize)]
pub enum Voting {
    Reject = 0,
    Confirm = 1,
}

impl StandaloneToken for Voting {}

#[derive(Debug, Clone)]
pub struct BridgeKey {
    pub ton: MsgAddrStd,
    pub eth: EthAddress,
}

impl TryFrom<ContractOutput> for Vec<BridgeKey> {
    type Error = ContractError;

    fn try_from(value: ContractOutput) -> Result<Self, Self::Error> {
        let mut tuple = value.tokens.into_unpacker();

        let keys: Vec<_> = tuple.unpack_next()?;
        let ethereum_accounts: Vec<EthereumAccount> = tuple.unpack_next()?;

        if keys.len() != ethereum_accounts.len() {
            return Err(ContractError::InvalidAbi);
        }

        Ok(keys
            .into_iter()
            .zip(ethereum_accounts.into_iter())
            .map(|(ton, eth)| BridgeKey {
                ton,
                eth: eth.ethereum_account,
            })
            .collect())
    }
}
