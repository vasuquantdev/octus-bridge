use std::hash::{Hash, Hasher};

use crate::contracts::errors::*;
use crate::contracts::prelude::*;
use crate::models::*;
use crate::prelude::*;

crate::define_event!(BridgeContractEvent, BridgeContractEventKind, {
    NewEthereumEventConfiguration { address: MsgAddrStd },
});

impl TryFrom<(BridgeContractEventKind, Vec<Token>)> for BridgeContractEvent {
    type Error = ContractError;

    fn try_from((kind, tokens): (BridgeContractEventKind, Vec<Token>)) -> ContractResult<Self> {
        Ok(match kind {
            BridgeContractEventKind::NewEthereumEventConfiguration => {
                BridgeContractEvent::NewEthereumEventConfiguration {
                    address: tokens.into_iter().next().try_parse()?,
                }
            }
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
        },
        NewEthereumEventReject {
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
        let mut tokens = tokens.into_iter();

        Ok(match kind {
            EthereumEventConfigurationContractEventKind::NewEthereumEventConfirmation => {
                EthereumEventConfigurationContractEvent::NewEthereumEventConfirmation {
                    address: tokens.next().try_parse()?,
                    relay_key: tokens.next().try_parse()?,
                }
            }
            EthereumEventConfigurationContractEventKind::NewEthereumEventReject => {
                EthereumEventConfigurationContractEvent::NewEthereumEventReject {
                    address: tokens.next().try_parse()?,
                    relay_key: tokens.next().try_parse()?,
                }
            }
        })
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
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

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct EthereumEventConfiguration {
    pub ethereum_event_abi: String,
    pub ethereum_event_address: ethereum_types::Address,
    #[serde(with = "serde_std_addr")]
    pub event_proxy_address: MsgAddrStd,
    pub ethereum_event_blocks_to_confirm: BigUint,
    pub required_confirmations: BigUint,
    pub required_rejections: BigUint,
    #[serde(with = "serde_vec_uint256")]
    pub confirm_keys: Vec<UInt256>,
    #[serde(with = "serde_vec_uint256")]
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
            ethereum_event_address: ethereum_types::Address::from_str(
                String::from_utf8(tuple.parse_next()?)
                    .map_err(|_| ContractError::InvalidString)?
                    .trim_start_matches("0x"),
            )
            .map_err(|_| ContractError::InvalidAddress)?,
            event_proxy_address: tuple.parse_next()?,
            ethereum_event_blocks_to_confirm: tuple.parse_next()?,
            required_confirmations: tuple.parse_next()?,
            required_rejections: tuple.parse_next()?,
            confirm_keys: tuple.parse_next()?,
            reject_keys: tuple.parse_next()?,
        })
    }
}

#[derive(Debug, Clone, Eq, Serialize, Deserialize)]
pub struct EthereumEventDetails {
    pub ethereum_event_transaction: Vec<u8>,
    pub event_index: BigUint,
    #[serde(with = "serde_cells")]
    pub event_data: Cell,
    #[serde(with = "serde_std_addr")]
    pub proxy_address: MsgAddrStd,
    pub event_block_number: BigUint,
    pub event_block: Vec<u8>,
    #[serde(with = "serde_std_addr")]
    pub event_configuration_address: MsgAddrStd,
    pub proxy_callback_executed: bool,
    pub event_rejected: bool,
    #[serde(with = "serde_vec_uint256")]
    pub confirm_keys: Vec<UInt256>,
    #[serde(with = "serde_vec_uint256")]
    pub reject_keys: Vec<UInt256>,
}

impl Hash for EthereumEventDetails {
    fn hash<H: Hasher>(&self, state: &mut H) {
        std::hash::Hash::hash(&self.event_configuration_address, state)
    }
}
impl PartialEq for EthereumEventDetails {
    fn eq(&self, other: &Self) -> bool {
        self.event_configuration_address
            .eq(&other.event_configuration_address)
    }
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
            event_block_number: tuple.parse_next()?,
            event_block: tuple.parse_next()?,
            event_configuration_address: tuple.parse_next()?,
            proxy_callback_executed: tuple.parse_next()?,
            event_rejected: tuple.parse_next()?,
            confirm_keys: tuple.parse_next()?,
            reject_keys: tuple.parse_next()?,
        })
    }
}
