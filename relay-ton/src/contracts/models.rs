use std::hash::Hash;
use std::io::Write;

use borsh::{BorshDeserialize, BorshSerialize};
use num_traits::ToPrimitive;
use ton_block::{Deserializable, Serializable};

use crate::contracts::errors::*;
use crate::contracts::prelude::*;
use crate::models::*;
use crate::prelude::*;
use ethereum_types::H256;

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
                    id: tokens.next().try_parse()?,
                    relay: tokens.next().try_parse()?,
                    vote: tokens.next().try_parse()?,
                }
            }
            BridgeContractEventKind::EventConfigurationCreationEnd => {
                BridgeContractEvent::EventConfigurationCreationEnd {
                    id: tokens.next().try_parse()?,
                    active: tokens.next().try_parse()?,
                    address: tokens.next().try_parse()?,
                    event_type: tokens.next().try_parse()?,
                }
            }
            BridgeContractEventKind::EventConfigurationUpdateVote => {
                BridgeContractEvent::EventConfigurationUpdateVote {
                    id: tokens.next().try_parse()?,
                    relay: tokens.next().try_parse()?,
                    vote: tokens.next().try_parse()?,
                }
            }
            BridgeContractEventKind::EventConfigurationUpdateEnd => {
                BridgeContractEvent::EventConfigurationUpdateEnd {
                    id: tokens.next().try_parse()?,
                    active: tokens.next().try_parse()?,
                    address: tokens.next().try_parse()?,
                    event_type: tokens.next().try_parse()?,
                }
            }
            BridgeContractEventKind::BridgeConfigurationUpdateVote => {
                BridgeContractEvent::BridgeConfigurationUpdateVote {
                    bridge_configuration: tokens.next().try_parse()?,
                    relay: tokens.next().try_parse()?,
                    vote: tokens.next().try_parse()?,
                }
            }
            BridgeContractEventKind::BridgeConfigurationUpdateEnd => {
                BridgeContractEvent::BridgeConfigurationUpdateEnd {
                    bridge_configuration: tokens.next().try_parse()?,
                    status: tokens.next().try_parse()?,
                }
            }
            BridgeContractEventKind::BridgeRelaysUpdateVote => {
                BridgeContractEvent::BridgeRelaysUpdateVote {
                    target: tokens.next().try_parse()?,
                    relay: tokens.next().try_parse()?,
                    vote: tokens.next().try_parse()?,
                }
            }
            BridgeContractEventKind::BridgeRelaysUpdateEnd => {
                BridgeContractEvent::BridgeRelaysUpdateEnd {
                    target: tokens.next().try_parse()?,
                    active: tokens.next().try_parse()?,
                }
            }
            BridgeContractEventKind::OwnershipGranted => BridgeContractEvent::OwnershipGranted {
                relay: tokens.next().try_parse()?,
            },
            BridgeContractEventKind::OwnershipRemoved => BridgeContractEvent::OwnershipRemoved {
                relay: tokens.next().try_parse()?,
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
                    address: tokens.next().try_parse()?,
                    relay: tokens.next().try_parse()?,
                }
            }
            TonEventConfigurationContractEventKind::EventReject => {
                TonEventConfigurationContractEvent::EventReject {
                    address: tokens.next().try_parse()?,
                    relay: tokens.next().try_parse()?,
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
                    address: tokens.next().try_parse()?,
                    relay: tokens.next().try_parse()?,
                }
            }
            EthEventConfigurationContractEventKind::EventReject => {
                EthEventConfigurationContractEvent::EventReject {
                    address: tokens.next().try_parse()?,
                    relay: tokens.next().try_parse()?,
                }
            }
        })
    }
}

// Models

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct BridgeConfiguration {
    pub nonce: u16,
    pub bridge_update_required_confirmations: u16,
    pub bridge_update_required_rejects: u16,
    pub active: bool,
}

impl StandaloneToken for BridgeConfiguration {}

fn parse_bridge_configuration(tokens: Vec<Token>) -> ContractResult<BridgeConfiguration> {
    let mut tokens = tokens.into_iter();
    Ok(BridgeConfiguration {
        nonce: tokens.next().try_parse()?,
        bridge_update_required_confirmations: tokens.next().try_parse()?,
        bridge_update_required_rejects: tokens.next().try_parse()?,
        active: tokens.next().try_parse()?,
    })
}

impl FunctionArg for BridgeConfiguration {
    fn token_value(self) -> TokenValue {
        TokenValue::Tuple(vec![
            self.nonce.token_value().named("nonce"),
            self.bridge_update_required_confirmations
                .token_value()
                .named("bridgeUpdateRequiredConfirmations"),
            self.bridge_update_required_rejects
                .token_value()
                .named("bridgeUpdateRequiredRejects"),
            self.active.token_value().named("active"),
        ])
    }
}

impl ParseToken<BridgeConfiguration> for TokenValue {
    fn try_parse(self) -> ContractResult<BridgeConfiguration> {
        match self {
            TokenValue::Tuple(tokens) => parse_bridge_configuration(tokens),
            _ => Err(ContractError::InvalidAbi),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ActiveEventConfiguration {
    pub id: u32,
    pub address: MsgAddressInt,
    pub event_type: EventType,
}

impl TryFrom<ContractOutput> for Vec<ActiveEventConfiguration> {
    type Error = ContractError;

    fn try_from(value: ContractOutput) -> Result<Self, Self::Error> {
        let mut tokens = value.into_parser();
        let ids: Vec<u32> = tokens.parse_next()?;
        let addrs: Vec<MsgAddressInt> = tokens.parse_next()?;
        let types: Vec<EventType> = tokens.parse_next()?;
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

#[derive(Debug, Clone)]
pub struct RelayUpdate {
    pub nonce: u16,
    pub ton_account: MsgAddrStd,
    pub eth_account: ethereum_types::Address,
    pub action: RelayUpdateAction,
}

impl ParseToken<RelayUpdate> for TokenValue {
    fn try_parse(self) -> ContractResult<RelayUpdate> {
        let mut tuple = match self {
            TokenValue::Tuple(tokens) => tokens.into_iter(),
            _ => return Err(ContractError::InvalidAbi),
        };

        let nonce = tuple.next().try_parse()?;
        let workchain_id = tuple.next().try_parse()?;
        let addr: UInt256 = tuple.next().try_parse()?;
        let ton_account = MsgAddrStd::with_address(None, workchain_id, addr.into());

        Ok(RelayUpdate {
            nonce,
            ton_account,
            eth_account: tuple.next().try_parse()?,
            action: tuple.next().try_parse()?,
        })
    }
}

impl FunctionArg for RelayUpdate {
    fn token_value(self) -> TokenValue {
        TokenValue::Tuple(vec![
            self.nonce.token_value().named("nonce"),
            self.ton_account.workchain_id.token_value().named("wid"),
            UInt256::from(self.ton_account.address.get_bytestring(0))
                .token_value()
                .named("addr"),
            self.eth_account.token_value().named("ethereumAccount"),
            self.action.token_value().named("action"),
        ])
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub enum RelayUpdateAction {
    Remove,
    Add,
}

impl FunctionArg for RelayUpdateAction {
    fn token_value(self) -> TokenValue {
        match self {
            RelayUpdateAction::Remove => false.token_value(),
            RelayUpdateAction::Add => true.token_value(),
        }
    }
}

impl StandaloneToken for RelayUpdateAction {}

impl ParseToken<RelayUpdateAction> for TokenValue {
    fn try_parse(self) -> ContractResult<RelayUpdateAction> {
        match self {
            TokenValue::Bool(true) => Ok(RelayUpdateAction::Add),
            TokenValue::Bool(false) => Ok(RelayUpdateAction::Remove),
            _ => Err(ContractError::InvalidAbi),
        }
    }
}

#[derive(Debug, Clone)]
pub struct VoteData {
    pub signature: Vec<u8>,
}

impl VoteData {
    pub fn reject() -> Self {
        Self {
            signature: Vec::new(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.signature.is_empty()
    }
}

impl FunctionArg for VoteData {
    fn token_value(self) -> TokenValue {
        TokenValue::Tuple(vec![self.signature.token_value().named("signature")])
    }
}

impl StandaloneToken for VoteData {}

impl ParseToken<VoteData> for TokenValue {
    fn try_parse(self) -> ContractResult<VoteData> {
        let mut tuple = match self {
            TokenValue::Tuple(tuple) => tuple.into_iter(),
            _ => return Err(ContractError::InvalidAbi),
        };
        Ok(VoteData {
            signature: tuple.next().try_parse()?,
        })
    }
}

#[derive(Debug, Clone)]
pub struct CommonEventConfigurationParams {
    pub event_abi: String,

    pub event_required_confirmations: u16,
    pub event_required_rejects: u16,

    pub event_code: Cell,

    pub bridge_address: MsgAddressInt,

    pub event_initial_balance: BigUint,

    pub meta: Cell,
}

impl ParseToken<CommonEventConfigurationParams> for TokenValue {
    fn try_parse(self) -> ContractResult<CommonEventConfigurationParams> {
        let mut tuple = match self {
            TokenValue::Tuple(tuple) => tuple.into_iter(),
            _ => return Err(ContractError::InvalidAbi),
        };

        Ok(CommonEventConfigurationParams {
            event_abi: tuple.next().try_parse()?,
            event_required_confirmations: tuple.next().try_parse()?,
            event_required_rejects: tuple.next().try_parse()?,
            event_code: tuple.next().try_parse()?,
            bridge_address: tuple.next().try_parse()?,
            event_initial_balance: tuple.next().try_parse()?,
            meta: tuple.next().try_parse()?,
        })
    }
}

#[derive(Debug, Clone)]
pub struct EthEventConfiguration {
    pub common: CommonEventConfigurationParams,
    pub event_address: ethereum_types::Address,
    pub event_blocks_to_confirm: u16,
    pub proxy_address: MsgAddressInt,
    pub start_block_number: u32,
}

impl TryFrom<ContractOutput> for EthEventConfiguration {
    type Error = ContractError;

    fn try_from(output: ContractOutput) -> ContractResult<EthEventConfiguration> {
        let mut tuple = output.into_parser();

        let common = tuple.parse_next()?;
        let mut tuple = match tuple.parse_next()? {
            TokenValue::Tuple(tuple) => tuple.into_iter(),
            _ => return Err(ContractError::InvalidAbi),
        };

        Ok(EthEventConfiguration {
            common,
            event_address: tuple.next().try_parse()?,
            event_blocks_to_confirm: tuple.next().try_parse()?,
            proxy_address: tuple.next().try_parse()?,
            start_block_number: tuple.next().try_parse()?,
        })
    }
}

#[derive(Debug, Clone)]
pub struct TonEventConfiguration {
    pub common: CommonEventConfigurationParams,
    pub event_address: MsgAddressInt,
    pub proxy_address: ethereum_types::H160,
    pub start_timestamp: u32,
}

impl TryFrom<ContractOutput> for TonEventConfiguration {
    type Error = ContractError;

    fn try_from(output: ContractOutput) -> Result<Self, Self::Error> {
        let mut tuple = output.into_parser();

        let common = tuple.parse_next()?;
        let mut tuple = match tuple.parse_next()? {
            TokenValue::Tuple(tuple) => tuple.into_iter(),
            _ => return Err(ContractError::InvalidAbi),
        };

        Ok(TonEventConfiguration {
            common,
            event_address: tuple.next().try_parse()?,
            proxy_address: tuple.next().try_parse()?,
            start_timestamp: tuple.next().try_parse()?,
        })
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
pub enum EventStatus {
    InProcess,
    Confirmed,
    Rejected,
}

impl ParseToken<EventStatus> for TokenValue {
    fn try_parse(self) -> ContractResult<EventStatus> {
        match self {
            TokenValue::Uint(value) => match value.number.to_u8() {
                Some(0) => Ok(EventStatus::InProcess),
                Some(1) => Ok(EventStatus::Confirmed),
                Some(2) => Ok(EventStatus::Rejected),
                _ => Err(ContractError::InvalidAbi),
            },
            _ => Err(ContractError::InvalidAbi),
        }
    }
}

impl StandaloneToken for EventStatus {}

#[derive(Debug, Clone)]
pub struct TonEventInitData {
    pub event_transaction: UInt256,
    pub event_transaction_lt: u64,
    pub event_timestamp: u32,
    pub event_index: u32,
    pub event_data: Cell,

    pub ton_event_configuration: MsgAddressInt,
    pub required_confirmations: u16,
    pub required_rejections: u16,

    pub configuration_meta: Cell,
}

impl ParseToken<TonEventInitData> for TokenValue {
    fn try_parse(self) -> ContractResult<TonEventInitData> {
        let mut tuple = match self {
            TokenValue::Tuple(tuple) => tuple.into_iter(),
            _ => return Err(ContractError::InvalidAbi),
        };

        Ok(TonEventInitData {
            event_transaction: tuple.next().try_parse()?,
            event_transaction_lt: tuple.next().try_parse()?,
            event_timestamp: tuple.next().try_parse()?,
            event_index: tuple.next().try_parse()?,
            event_data: tuple.next().try_parse()?,
            ton_event_configuration: tuple.next().try_parse()?,
            required_confirmations: tuple.next().try_parse()?,
            required_rejections: tuple.next().try_parse()?,
            configuration_meta: tuple.next().try_parse()?,
        })
    }
}

#[derive(Debug, Clone)]
pub struct TonEventVoteData {
    /// Not serializable!
    pub configuration_id: u32,

    pub event_transaction: UInt256,
    pub event_transaction_lt: u64,
    pub event_timestamp: u32,
    pub event_index: u32,
    pub event_data: Cell,
}

impl FunctionArg for TonEventVoteData {
    fn token_value(self) -> TokenValue {
        TokenValue::Tuple(vec![
            self.event_transaction
                .token_value()
                .named("eventTransaction"),
            self.event_transaction_lt
                .token_value()
                .named("eventTransactionLt"),
            self.event_timestamp.token_value().named("eventTimestamp"),
            self.event_index.token_value().named("eventIndex"),
            self.event_data.token_value().named("eventData"),
        ])
    }
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
        let mut tuple = output.into_parser();

        Ok(TonEventDetails {
            init_data: tuple.parse_next()?,
            status: tuple.parse_next()?,
            confirm_keys: tuple.parse_next()?,
            reject_keys: tuple.parse_next()?,
            event_data_signatures: tuple.parse_next()?,
        })
    }
}

#[derive(Debug, Clone)]
pub struct EthEventInitData {
    pub event_transaction: ethereum_types::H256,
    pub event_index: u32,
    pub event_data: Cell,
    pub event_block_number: u32,
    pub event_block: ethereum_types::H256,

    pub eth_event_configuration: MsgAddressInt,
    pub required_confirmations: u16,
    pub required_rejections: u16,

    pub proxy_address: MsgAddressInt,

    pub configuration_meta: Cell,
}

impl ParseToken<EthEventInitData> for TokenValue {
    fn try_parse(self) -> ContractResult<EthEventInitData> {
        let mut tuples = match self {
            TokenValue::Tuple(tuple) => tuple.into_iter(),
            _ => return Err(ContractError::InvalidAbi),
        };

        Ok(EthEventInitData {
            event_transaction: tuples.next().try_parse()?,
            event_index: tuples.next().try_parse()?,
            event_data: tuples.next().try_parse()?,
            event_block_number: tuples.next().try_parse()?,
            event_block: tuples.next().try_parse()?,
            eth_event_configuration: tuples.next().try_parse()?,
            required_confirmations: tuples.next().try_parse()?,
            required_rejections: tuples.next().try_parse()?,
            proxy_address: tuples.next().try_parse()?,
            configuration_meta: tuples.next().try_parse()?,
        })
    }
}

#[derive(Debug, Clone)]
pub struct EthEventVoteData {
    /// Not serializable!
    pub configuration_id: u32,

    pub event_transaction: ethereum_types::H256,
    pub event_index: u32,
    pub event_data: Cell,
    pub event_block_number: u32,
    pub event_block: ethereum_types::H256,
}

impl FunctionArg for EthEventVoteData {
    fn token_value(self) -> TokenValue {
        TokenValue::Tuple(vec![
            self.event_transaction
                .token_value()
                .named("eventTransaction"),
            self.event_index.token_value().named("eventIndex"),
            self.event_data.token_value().named("eventData"),
            self.event_block_number
                .token_value()
                .named("eventBlockNumber"),
            self.event_block.token_value().named("eventBlock"),
        ])
    }
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
        let mut tuple = output.into_parser();

        Ok(EthEventDetails {
            init_data: tuple.parse_next()?,
            status: tuple.parse_next()?,
            confirm_relays: tuple.parse_next()?,
            reject_relays: tuple.parse_next()?,
        })
    }
}

#[allow(clippy::upper_case_acronyms)]
#[derive(Debug, Clone, Copy, Eq, PartialEq, Deserialize, Serialize)]
pub enum EventType {
    ETH,
    TON,
}

impl ParseToken<EventType> for TokenValue {
    fn try_parse(self) -> ContractResult<EventType> {
        match self {
            TokenValue::Uint(int) => match int.number.to_u8() {
                Some(0) => Ok(EventType::ETH),
                Some(1) => Ok(EventType::TON),
                _ => Err(ContractError::InvalidAbi),
            },
            _ => Err(ContractError::InvalidAbi),
        }
    }
}

impl StandaloneToken for EventType {}

impl FunctionArg for EventType {
    fn token_value(self) -> TokenValue {
        match self {
            EventType::ETH => 0u8.token_value(),
            EventType::TON => 1u8.token_value(),
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Deserialize, Serialize)]
pub enum Voting {
    Reject,
    Confirm,
}

impl FunctionArg for Voting {
    fn token_value(self) -> TokenValue {
        match self {
            Voting::Reject => false.token_value(),
            Voting::Confirm => true.token_value(),
        }
    }
}

impl StandaloneToken for Voting {}

impl ParseToken<Voting> for TokenValue {
    fn try_parse(self) -> ContractResult<Voting> {
        match self {
            TokenValue::Bool(false) => Ok(Voting::Reject),
            TokenValue::Bool(true) => Ok(Voting::Confirm),
            _ => Err(ContractError::InvalidAbi),
        }
    }
}

impl TryFrom<ContractOutput> for Vec<BridgeKey> {
    type Error = ContractError;

    fn try_from(value: ContractOutput) -> Result<Self, Self::Error> {
        let mut tuple = value.into_parser();

        let keys: Vec<_> = tuple.parse_next()?;
        let ethereum_accounts: Vec<_> = tuple.parse_next()?;

        if keys.len() != ethereum_accounts.len() {
            return Err(ContractError::InvalidAbi);
        }

        Ok(keys
            .into_iter()
            .zip(ethereum_accounts.into_iter())
            .map(|(ton, eth)| BridgeKey { ton, eth })
            .collect())
    }
}

#[derive(Debug, Clone)]
pub struct BridgeKey {
    pub ton: MsgAddrStd,
    pub eth: ethereum_types::Address,
}
