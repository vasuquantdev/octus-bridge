use crate::contracts::errors::*;
use crate::contracts::prelude::*;
use crate::models::*;
use crate::prelude::*;

#[derive(Debug, Clone)]
pub struct OffchainVotingSet {
    pub change_nonce: BigUint,
    pub signers: Vec<AccountId>,
    pub signatures_high_parts: Vec<UInt256>,
    pub signatures_low_parts: Vec<UInt256>,
}

#[derive(Debug, Clone)]
pub enum BridgeContractEvent {
    ConfigurationChanged(Vec<EthereumEventsConfiguration>),
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

        Ok(Self {
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

        Ok((
            ton_address,
            TonEventConfiguration {
                eth_address: tokens.parse_next()?,
                eth_event_abi: tokens.parse_next()?,
                event_proxy_address: tokens.parse_next()?,
                min_signs: tokens.parse_next()?,
                min_signs_percent: tokens.parse_next()?,
                ton_to_eth_rate: tokens.parse_next()?,
                eth_to_ton_rate: tokens.parse_next()?,
            },
        ))
    }
}
