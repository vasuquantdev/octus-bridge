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

crate::define_event!(
    EthereumEventConfigurationContractEvent,
    EthereumEventConfigurationContractEventKind,
    {
        NewEthereumEventConfirmation {
            address: MsgAddrStd,
            relay_key: UInt256,
        }
    }
);

impl TryFrom<(EthereumEventConfigurationContractEventKind, Vec<Token>)>
    for EthereumEventConfigurationContractEvent
{
    type Error = ContractError;

    fn try_from(
        (kind, tokens): (EthereumEventConfigurationContractEventKind, Vec<Token>),
    ) -> Result<Self, Self::Error> {
        Ok(match kind {
            EthereumEventConfigurationContractEventKind::NewEthereumEventConfirmation => {
                let mut tokens = tokens.into_iter();
                EthereumEventConfigurationContractEvent::NewEthereumEventConfirmation {
                    address: tokens.next().try_parse()?,
                    relay_key: tokens.next().try_parse()?,
                }
            }
        })
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct BridgeConfiguration {
    pub eth_event_configuration_required_confirmations: u8,
    pub eth_event_configuration_required_rejects: u8,
    pub eth_event_configuration_sequential_index: u8,
}

impl StandaloneToken for BridgeConfiguration {}

impl TryFrom<ContractOutput> for BridgeConfiguration {
    type Error = ContractError;

    fn try_from(output: ContractOutput) -> ContractResult<Self> {
        let mut tokens = output.into_parser();
        Ok(BridgeConfiguration {
            eth_event_configuration_required_confirmations: tokens.parse_next()?,
            eth_event_configuration_required_rejects: tokens.parse_next()?,
            eth_event_configuration_sequential_index: tokens.parse_next()?,
        })
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct EthereumEventConfiguration {
    pub ethereum_event_abi: String,
    pub ethereum_event_address: Vec<u8>,
    pub event_proxy_address: MsgAddrStd,
    pub required_confirmations: BigUint,
    pub required_rejections: BigUint,
    pub confirm_keys: Vec<UInt256>,
    pub reject_keys: Vec<UInt256>,
}

impl StandaloneToken for EthereumEventConfiguration {}

impl TryFrom<ContractOutput> for EthereumEventConfiguration {
    type Error = ContractError;

    fn try_from(output: ContractOutput) -> ContractResult<EthereumEventConfiguration> {
        let mut tuple = output.into_parser();

        Ok(EthereumEventConfiguration {
            ethereum_event_abi: String::from_utf8(tuple.parse_next()?)
                .map_err(|_| ContractError::InvalidString)?,
            ethereum_event_address: tuple.parse_next()?,
            event_proxy_address: tuple.parse_next()?,
            required_confirmations: tuple.parse_next()?,
            required_rejections: tuple.parse_next()?,
            confirm_keys: tuple.parse_next()?,
            reject_keys: tuple.parse_next()?,
        })
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct EthereumEventDetails {
    pub ethereum_event_transaction: Vec<u8>,
    pub event_index: BigUint,
    pub event_data: Cell,
    pub proxy_address: MsgAddrStd,
    pub event_configuration_address: MsgAddrStd,
    pub proxy_callback_executed: bool,
    pub confirm_keys: Vec<UInt256>,
    pub reject_keys: Vec<UInt256>,
}

impl StandaloneToken for EthereumEventDetails {}

impl TryFrom<ContractOutput> for EthereumEventDetails {
    type Error = ContractError;

    fn try_from(output: ContractOutput) -> ContractResult<Self> {
        let mut tuple = output.into_parser();

        Ok(EthereumEventDetails {
            ethereum_event_transaction: tuple.parse_next()?,
            event_index: tuple.parse_next()?,
            event_data: tuple.parse_next()?,
            proxy_address: tuple.parse_next()?,
            event_configuration_address: tuple.parse_next()?,
            proxy_callback_executed: tuple.parse_next()?,
            confirm_keys: tuple.parse_next()?,
            reject_keys: tuple.parse_next()?,
        })
    }
}
