use num_traits::ToPrimitive;
use std::hash::Hash;

use crate::contracts::errors::*;
use crate::contracts::prelude::*;
use crate::models::*;
use crate::prelude::*;

// Events

crate::define_event!(BridgeContractEvent, BridgeContractEventKind, {
    EventConfigurationCreationVote { id: BigUint, relay_key: UInt256, vote: Voting },
    EventConfigurationCreationEnd {
        id: BigUint,
        active: bool,
        address: MsgAddressInt,
        event_type: EventType
    },

    EventConfigurationUpdateVote { id: BigUint, relay_key: UInt256, vote: Voting },
    EventConfigurationUpdateEnd {
        id: BigUint,
        active: bool,
    },

    BridgeConfigurationUpdateVote {
        bridge_configuration: BridgeConfiguration,
        relay_key: UInt256,
        vote: VoteData,
    },
    BridgeConfigurationUpdateEnd {
        bridge_configuration: BridgeConfiguration,
        status: bool,
    },

    BridgeRelaysUpdateVote {
        target: RelayUpdate,
        ethereum_account: ethereum_types::Address,
        relay_key: UInt256,
        vote: VoteData
    },
    BridgeRelaysUpdateEnd {
        target: RelayUpdate,
        ethereum_account: ethereum_types::Address,
        active: bool
    },

    OwnershipGranted { key: UInt128 },
    OwnershipRemoved { key: UInt128 },
});

impl TryFrom<(BridgeContractEventKind, Vec<Token>)> for BridgeContractEvent {
    type Error = ContractError;

    fn try_from((kind, tokens): (BridgeContractEventKind, Vec<Token>)) -> ContractResult<Self> {
        let mut tokens = tokens.into_iter();
        Ok(match kind {
            BridgeContractEventKind::EventConfigurationCreationVote => {
                BridgeContractEvent::EventConfigurationCreationVote {
                    id: tokens.next().try_parse()?,
                    relay_key: tokens.next().try_parse()?,
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
                    relay_key: tokens.next().try_parse()?,
                    vote: tokens.next().try_parse()?,
                }
            }
            BridgeContractEventKind::EventConfigurationUpdateEnd => {
                BridgeContractEvent::EventConfigurationUpdateEnd {
                    id: tokens.next().try_parse()?,
                    active: tokens.next().try_parse()?,
                }
            }
            BridgeContractEventKind::BridgeConfigurationUpdateVote => {
                BridgeContractEvent::BridgeConfigurationUpdateVote {
                    bridge_configuration: tokens.next().try_parse()?,
                    relay_key: tokens.next().try_parse()?,
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
                    ethereum_account: tokens.next().try_parse()?,
                    relay_key: tokens.next().try_parse()?,
                    vote: tokens.next().try_parse()?,
                }
            }
            BridgeContractEventKind::BridgeRelaysUpdateEnd => {
                BridgeContractEvent::BridgeRelaysUpdateEnd {
                    target: tokens.next().try_parse()?,
                    ethereum_account: tokens.next().try_parse()?,
                    active: tokens.next().try_parse()?,
                }
            }
            BridgeContractEventKind::OwnershipGranted => BridgeContractEvent::OwnershipGranted {
                key: tokens.next().try_parse()?,
            },
            BridgeContractEventKind::OwnershipRemoved => BridgeContractEvent::OwnershipRemoved {
                key: tokens.next().try_parse()?,
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
            relay_key: UInt256,
        },
        EventReject {
            address: MsgAddrStd,
            relay_key: UInt256,
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
                    relay_key: tokens.next().try_parse()?,
                }
            }
            TonEventConfigurationContractEventKind::EventReject => {
                TonEventConfigurationContractEvent::EventReject {
                    address: tokens.next().try_parse()?,
                    relay_key: tokens.next().try_parse()?,
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
            relay_key: UInt256,
        },
        EventReject {
            address: MsgAddrStd,
            relay_key: UInt256,
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
                    relay_key: tokens.next().try_parse()?,
                }
            }
            EthEventConfigurationContractEventKind::EventReject => {
                EthEventConfigurationContractEvent::EventReject {
                    address: tokens.next().try_parse()?,
                    relay_key: tokens.next().try_parse()?,
                }
            }
        })
    }
}

// Models

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct BridgeConfiguration {
    pub event_configuration_required_confirmations: u16,
    pub event_configuration_required_rejections: u16,

    pub bridge_configuration_update_required_confirmations: u16,
    pub bridge_configuration_update_required_rejections: u16,

    pub bridge_relay_update_required_confirmations: u16,
    pub bridge_relay_update_required_rejections: u16,

    pub active: bool,
}

impl StandaloneToken for BridgeConfiguration {}

fn parse_bridge_configuration(tokens: Vec<Token>) -> ContractResult<BridgeConfiguration> {
    let mut tokens = tokens.into_iter();
    Ok(BridgeConfiguration {
        event_configuration_required_confirmations: tokens.next().try_parse()?,
        event_configuration_required_rejections: tokens.next().try_parse()?,
        bridge_configuration_update_required_confirmations: tokens.next().try_parse()?,
        bridge_configuration_update_required_rejections: tokens.next().try_parse()?,
        bridge_relay_update_required_confirmations: tokens.next().try_parse()?,
        bridge_relay_update_required_rejections: tokens.next().try_parse()?,
        active: tokens.next().try_parse()?,
    })
}

impl FunctionArg for BridgeConfiguration {
    fn token_value(self) -> TokenValue {
        TokenValue::Tuple(vec![
            self.event_configuration_required_confirmations
                .token_value()
                .named("eventConfigurationRequiredConfirmations"),
            self.event_configuration_required_rejections
                .token_value()
                .named("eventConfigurationRequiredRejects"),
            self.bridge_configuration_update_required_confirmations
                .token_value()
                .named("bridgeConfigurationUpdateRequiredConfirmations"),
            self.bridge_configuration_update_required_rejections
                .token_value()
                .named("bridgeConfigurationUpdateRequiredRejects"),
            self.bridge_relay_update_required_confirmations
                .token_value()
                .named("bridgeRelayUpdateRequiredConfirmations"),
            self.bridge_relay_update_required_rejections
                .token_value()
                .named("bridgeRelayUpdateRequiredRejects"),
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
    pub id: BigUint,
    pub address: MsgAddressInt,
    pub event_type: EventType,
}

impl TryFrom<ContractOutput> for Vec<ActiveEventConfiguration> {
    type Error = ContractError;

    fn try_from(value: ContractOutput) -> Result<Self, Self::Error> {
        let mut tokens = value.into_parser();
        let ids: Vec<BigUint> = tokens.parse_next()?;
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventConfigurationStatus {
    #[serde(with = "serde_vec_uint256")]
    pub confirm_keys: Vec<UInt256>,
    #[serde(with = "serde_vec_uint256")]
    pub reject_keys: Vec<UInt256>,
    pub active: bool,
}

impl TryFrom<ContractOutput> for EventConfigurationStatus {
    type Error = ContractError;

    fn try_from(value: ContractOutput) -> Result<Self, Self::Error> {
        let mut tokens = value.tokens.into_iter();
        Ok(Self {
            confirm_keys: tokens.next().try_parse()?,
            reject_keys: tokens.next().try_parse()?,
            active: tokens.next().try_parse()?,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BridgeConfigurationStatus {
    #[serde(with = "serde_vec_uint256")]
    pub confirm_keys: Vec<UInt256>,
    #[serde(with = "serde_vec_uint256")]
    pub reject_keys: Vec<UInt256>,
}

impl TryFrom<ContractOutput> for BridgeConfigurationStatus {
    type Error = ContractError;

    fn try_from(value: ContractOutput) -> Result<Self, Self::Error> {
        let mut tokens = value.tokens.into_iter();
        Ok(Self {
            confirm_keys: tokens.next().try_parse()?,
            reject_keys: tokens.next().try_parse()?,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RelayUpdate {
    #[serde(with = "serde_uint256")]
    pub relay_key: UInt256,
    pub action: RelayUpdateAction,
}

fn parse_relay_update(tokens: Vec<Token>) -> ContractResult<RelayUpdate> {
    let mut tuple = tokens.into_iter();
    Ok(RelayUpdate {
        relay_key: tuple.next().try_parse()?,
        action: tuple.next().try_parse()?,
    })
}

impl ParseToken<RelayUpdate> for TokenValue {
    fn try_parse(self) -> ContractResult<RelayUpdate> {
        match self {
            TokenValue::Tuple(tokens) => parse_relay_update(tokens),
            _ => Err(ContractError::InvalidAbi),
        }
    }
}

impl FunctionArg for RelayUpdate {
    fn token_value(self) -> TokenValue {
        TokenValue::Tuple(vec![
            self.relay_key.token_value().named("key"),
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

#[derive(Debug, Clone, Serialize, Deserialize)]
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

fn parse_vote_data(tokens: Vec<Token>) -> ContractResult<VoteData> {
    let mut tokens = tokens.into_iter();
    Ok(VoteData {
        signature: tokens.next().try_parse()?,
    })
}

impl FunctionArg for VoteData {
    fn token_value(self) -> TokenValue {
        TokenValue::Tuple(vec![self.signature.token_value().named("signature")])
    }
}

impl StandaloneToken for VoteData {}

impl ParseToken<VoteData> for TokenValue {
    fn try_parse(self) -> ContractResult<VoteData> {
        match self {
            TokenValue::Tuple(tokens) => parse_vote_data(tokens),
            _ => Err(ContractError::InvalidAbi),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommonEventConfigurationParams {
    pub event_abi: String,

    pub event_required_confirmations: u16,
    pub event_required_rejects: u16,

    #[serde(with = "serde_cells")]
    pub event_code: Cell,

    #[serde(with = "serde_int_addr")]
    pub bridge_address: MsgAddressInt,

    pub event_initial_balance: BigUint,
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
        })
    }
}

#[derive(Debug, Clone)]
pub struct EthEventConfiguration {
    pub common: CommonEventConfigurationParams,
    pub event_address: ethereum_types::Address,
    pub event_blocks_to_confirm: u16,
    pub proxy_address: MsgAddressInt,
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
        })
    }
}

#[derive(Debug, Clone)]
pub struct TonEventConfiguration {
    pub common: CommonEventConfigurationParams,
    pub event_address: MsgAddressInt,
    pub proxy_address: ethereum_types::H160,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TonEventInitData {
    #[serde(with = "serde_uint256")]
    pub event_transaction: UInt256,
    pub event_transaction_lt: u64,
    pub event_index: u64,
    #[serde(with = "serde_cells")]
    pub event_data: Cell,

    #[serde(with = "serde_int_addr")]
    pub ton_event_configuration: MsgAddressInt,
    pub required_confirmations: BigUint,
    pub required_rejections: BigUint,
}

impl ParseToken<TonEventInitData> for TokenValue {
    fn try_parse(self) -> ContractResult<TonEventInitData> {
        let mut tuple = match self {
            TokenValue::Tuple(tuple) => tuple.into_iter(),
            _ => return Err(ContractError::InvalidAbi),
        };

        // TODO: update models

        let event_transaction = tuple.next().try_parse()?;
        let event_index = tuple.next().try_parse()?;
        let event_data = tuple.next().try_parse()?;
        let event_transaction_lt = tuple.next().try_parse()?; // event_block_number
        let _event_block: UInt256 = tuple.next().try_parse()?;
        let ton_event_configuration = tuple.next().try_parse()?;
        let required_confirmations = tuple.next().try_parse()?;
        let required_rejections = tuple.next().try_parse()?;

        Ok(TonEventInitData {
            event_transaction,
            event_transaction_lt,
            event_index,
            event_data,
            ton_event_configuration,
            required_confirmations,
            required_rejections,
        })
    }
}

impl StandaloneToken for TonEventInitData {}

impl FunctionArg for TonEventInitData {
    fn token_value(self) -> TokenValue {
        TokenValue::Tuple(vec![
            self.event_transaction
                .token_value()
                .named("eventTransaction"),
            BigUint256(self.event_index.into())
                .token_value()
                .named("eventIndex"),
            self.event_data.token_value().named("eventData"),
            BigUint256(self.event_transaction_lt.into())
                .token_value()
                .named("eventBlockNumber"),
            UInt256::default().token_value().named("eventBlock"),
            self.ton_event_configuration
                .token_value()
                .named("tonEventConfiguration"),
            BigUint256(self.required_confirmations)
                .token_value()
                .named("requiredConfirmations"),
            BigUint256(self.required_rejections)
                .token_value()
                .named("requiredRejects"),
        ])
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TonEventDetails {
    pub init_data: TonEventInitData,
    pub status: EventStatus,
    #[serde(with = "serde_vec_uint256")]
    pub confirm_keys: Vec<UInt256>,
    #[serde(with = "serde_vec_uint256")]
    pub reject_keys: Vec<UInt256>,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EthEventInitData {
    pub event_transaction: ethereum_types::H256,
    pub event_index: BigUint,
    #[serde(with = "serde_cells")]
    pub event_data: Cell,
    pub event_block_number: BigUint,
    pub event_block: ethereum_types::H256,

    #[serde(with = "serde_std_addr")]
    pub eth_event_configuration: MsgAddrStd,
    pub required_confirmations: BigUint,
    pub required_rejections: BigUint,

    #[serde(with = "serde_std_addr")]
    pub proxy_address: MsgAddrStd,
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
        })
    }
}

impl StandaloneToken for EthEventInitData {}

impl FunctionArg for EthEventInitData {
    fn token_value(self) -> TokenValue {
        TokenValue::Tuple(vec![
            self.event_transaction
                .token_value()
                .named("eventTransaction"),
            BigUint256(self.event_index)
                .token_value()
                .named("eventIndex"),
            self.event_data.token_value().named("eventData"),
            BigUint256(self.event_block_number)
                .token_value()
                .named("eventBlockNumber"),
            self.event_block.token_value().named("eventBlock"),
            self.eth_event_configuration
                .token_value()
                .named("ethereumEventConfiguration"),
            BigUint256(self.required_confirmations)
                .token_value()
                .named("requiredConfirmations"),
            BigUint256(self.required_rejections)
                .token_value()
                .named("requiredRejects"),
            self.proxy_address.token_value().named("proxyAddress"),
        ])
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EthEventDetails {
    pub init_data: EthEventInitData,
    pub status: EventStatus,
    #[serde(with = "serde_vec_uint256")]
    pub confirm_keys: Vec<UInt256>,
    #[serde(with = "serde_vec_uint256")]
    pub reject_keys: Vec<UInt256>,
}

impl TryFrom<ContractOutput> for EthEventDetails {
    type Error = ContractError;

    fn try_from(output: ContractOutput) -> ContractResult<Self> {
        let mut tuple = output.into_parser();

        Ok(EthEventDetails {
            init_data: tuple.parse_next()?,
            status: tuple.parse_next()?,
            confirm_keys: tuple.parse_next()?,
            reject_keys: tuple.parse_next()?,
        })
    }
}

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

#[derive(Debug, Clone)]
pub struct BridgeKeys {
    pub keys: Vec<BridgeKey>,
}

impl TryFrom<ContractOutput> for BridgeKeys {
    type Error = ContractError;

    fn try_from(value: ContractOutput) -> Result<Self, Self::Error> {
        let mut tuple = value.into_parser();

        let keys: Vec<_> = tuple.parse_next()?;
        let ethereum_accounts: Vec<_> = tuple.parse_next()?;

        if keys.len() != ethereum_accounts.len() {
            return Err(ContractError::InvalidAbi);
        }

        Ok(Self {
            keys: keys
                .into_iter()
                .zip(ethereum_accounts.into_iter())
                .map(|(ton, eth)| BridgeKey { ton, eth })
                .collect(),
        })
    }
}

#[derive(Debug, Clone)]
pub struct BridgeKey {
    pub ton: UInt256,
    pub eth: ethereum_types::Address,
}
