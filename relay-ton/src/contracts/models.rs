use crate::contracts::errors::*;
use crate::contracts::prelude::*;
use crate::models::*;
use crate::prelude::*;

crate::define_event!(BridgeContractEvent, BridgeContractEventKind, {
    VotingForUpdateConfigStarted { voting_address: MsgAddrStd },
    BridgeConfigUpdated,
    VotingForAddEventTypeStarted { voting_address: MsgAddrStd },
    EventTypeAdded { event_root_address: MsgAddrStd },
    VotingForRemoveEventTypeStarted { voting_address: MsgAddrStd },
    EventTypeRemoved { event_root_address: MsgAddrStd },
});

impl TryFrom<(BridgeContractEventKind, Vec<Token>)> for BridgeContractEvent {
    type Error = ContractError;

    fn try_from((kind, tokens): (BridgeContractEventKind, Vec<Token>)) -> ContractResult<Self> {
        Ok(match kind {
            BridgeContractEventKind::VotingForUpdateConfigStarted => {
                BridgeContractEvent::VotingForUpdateConfigStarted {
                    voting_address: tokens.into_iter().next().try_parse()?,
                }
            }
            BridgeContractEventKind::BridgeConfigUpdated => {
                BridgeContractEvent::BridgeConfigUpdated
            }
            BridgeContractEventKind::VotingForAddEventTypeStarted => {
                BridgeContractEvent::VotingForAddEventTypeStarted {
                    voting_address: tokens.into_iter().next().try_parse()?,
                }
            }
            BridgeContractEventKind::EventTypeAdded => BridgeContractEvent::EventTypeAdded {
                event_root_address: tokens.into_iter().next().try_parse()?,
            },
            BridgeContractEventKind::VotingForRemoveEventTypeStarted => {
                BridgeContractEvent::VotingForRemoveEventTypeStarted {
                    voting_address: tokens.into_iter().next().try_parse()?,
                }
            }
            BridgeContractEventKind::EventTypeRemoved => BridgeContractEvent::EventTypeRemoved {
                event_root_address: tokens.into_iter().next().try_parse()?,
            },
        })
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct VotingSet {
    pub change_nonce: BigUint,
    pub signers: Vec<MsgAddrStd>,
    pub signatures_high_parts: Vec<UInt256>,
    pub signatures_low_parts: Vec<UInt256>,
}

fn parse_voting_set<I>(tokens: &mut ContractOutputParser<I>) -> ContractResult<VotingSet>
where
    I: Iterator<Item = Token>,
{
    Ok(VotingSet {
        change_nonce: tokens.parse_next()?,
        signers: tokens.parse_next()?,
        signatures_high_parts: tokens.parse_next()?,
        signatures_low_parts: tokens.parse_next()?,
    })
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct BridgeConfiguration {
    pub add_event_type_required_confirmations_percent: u8,
    pub remove_event_type_required_confirmations_percent: u8,
    pub add_relay_required_confirmations_percent: u8,
    pub remove_relay_required_confirmations_percent: u8,
    pub update_config_required_confirmations_percent: u8,
    pub event_root_code: Cell,
    pub ton_to_eth_event_code: Cell,
    pub eth_to_ton_event_code: Cell,
}

impl StandaloneToken for BridgeConfiguration {}

impl TryFrom<ContractOutput> for BridgeConfiguration {
    type Error = ContractError;

    fn try_from(output: ContractOutput) -> ContractResult<Self> {
        let mut tokens = output.into_parser();
        parse_bridge_configuration(&mut tokens)
    }
}

fn parse_bridge_configuration<I>(
    tokens: &mut ContractOutputParser<I>,
) -> ContractResult<BridgeConfiguration>
where
    I: Iterator<Item = Token>,
{
    Ok(BridgeConfiguration {
        add_event_type_required_confirmations_percent: tokens.parse_next()?,
        remove_event_type_required_confirmations_percent: tokens.parse_next()?,
        add_relay_required_confirmations_percent: tokens.parse_next()?,
        remove_relay_required_confirmations_percent: tokens.parse_next()?,
        update_config_required_confirmations_percent: tokens.parse_next()?,
        event_root_code: tokens.parse_next()?,
        ton_to_eth_event_code: tokens.parse_next()?,
        eth_to_ton_event_code: tokens.parse_next()?,
    })
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct EthereumEventsConfiguration {
    pub ethereum_event_abi: String,
    pub ethereum_address: Vec<u8>,
    pub event_proxy_address: MsgAddrStd,
    pub confirmations: BigUint,
    pub confirmed: bool,
}

impl StandaloneToken for EthereumEventsConfiguration {}

impl ParseToken<EthereumEventsConfiguration> for TokenValue {
    fn try_parse(self) -> ContractResult<EthereumEventsConfiguration> {
        let mut tuple = match self {
            TokenValue::Tuple(tuple) => tuple,
            _ => return Err(ContractError::InvalidAbi),
        }
        .into_iter();

        Ok(EthereumEventsConfiguration {
            ethereum_event_abi: String::from_utf8(tuple.next().try_parse()?)
                .map_err(|_| ContractError::InvalidString)?,
            ethereum_address: tuple.next().try_parse()?,
            event_proxy_address: tuple.next().try_parse()?,
            confirmations: tuple.next().try_parse()?,
            confirmed: tuple.next().try_parse()?,
        })
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct TonEventConfiguration {
    pub eth_address: Vec<u8>,
    pub eth_event_abi: Vec<u8>,
    pub event_proxy_address: Vec<u8>,
    pub min_signs: u8,
    pub min_signs_percent: u8,
    pub ton_to_eth_rate: BigUint,
    pub eth_to_ton_rate: BigUint,
}

impl TryFrom<ContractOutput> for (MsgAddrStd, TonEventConfiguration) {
    type Error = ContractError;

    fn try_from(output: ContractOutput) -> ContractResult<Self> {
        let mut tokens = output.into_parser();

        let ton_address = tokens.parse_next()?;
        let config = parse_ton_event_configuration(&mut tokens)?;

        Ok((ton_address, config))
    }
}

fn parse_ton_event_configuration<I>(
    tokens: &mut ContractOutputParser<I>,
) -> ContractResult<TonEventConfiguration>
where
    I: Iterator<Item = Token>,
{
    Ok(TonEventConfiguration {
        eth_address: tokens.parse_next()?,
        eth_event_abi: tokens.parse_next()?,
        event_proxy_address: tokens.parse_next()?,
        min_signs: tokens.parse_next()?,
        min_signs_percent: tokens.parse_next()?,
        ton_to_eth_rate: tokens.parse_next()?,
        eth_to_ton_rate: tokens.parse_next()?,
    })
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct VotingForAddEventTypeDetails {
    config: TonEventConfiguration,
    votes: VotingSet,
}

impl TryFrom<ContractOutput> for VotingForAddEventTypeDetails {
    type Error = ContractError;

    fn try_from(output: ContractOutput) -> ContractResult<Self> {
        let mut tokens = output.into_parser();

        let config = parse_ton_event_configuration(&mut tokens)?;
        let votes = parse_voting_set(&mut tokens)?;

        Ok(Self { config, votes })
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct VotingForRemoveEventTypeDetails {
    address: MsgAddrStd,
    votes: VotingSet,
}

impl TryFrom<ContractOutput> for VotingForRemoveEventTypeDetails {
    type Error = ContractError;

    fn try_from(output: ContractOutput) -> ContractResult<Self> {
        let mut tokens = output.into_parser();

        let address = tokens.parse_next()?;
        let votes = parse_voting_set(&mut tokens)?;

        Ok(Self { address, votes })
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct VotingForUpdateConfigDetails {
    config: BridgeConfiguration,
    votes: VotingSet,
}

impl TryFrom<ContractOutput> for VotingForUpdateConfigDetails {
    type Error = ContractError;

    fn try_from(output: ContractOutput) -> ContractResult<Self> {
        let mut tokens = output.into_parser();

        let config = parse_bridge_configuration(&mut tokens)?;
        let votes = parse_voting_set(&mut tokens)?;

        Ok(Self { config, votes })
    }
}
