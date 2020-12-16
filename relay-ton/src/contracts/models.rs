use std::hash::Hash;

use crate::contracts::errors::*;
use crate::contracts::prelude::*;
use crate::models::*;
use crate::prelude::*;

crate::define_event!(BridgeContractEvent, BridgeContractEventKind, {
    NewEthereumEventConfiguration { address: MsgAddrStd },
    NewBridgeConfigurationUpdate { address: MsgAddrStd },
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
            BridgeContractEventKind::NewBridgeConfigurationUpdate => {
                BridgeContractEvent::NewBridgeConfigurationUpdate {
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
pub struct SequentialIndex {
    pub ethereum_event_configuration: BigUint,
    pub bridge_configuration_update_voting: BigUint,
}

fn parse_sequential_index(tokens: Vec<Token>) -> ContractResult<SequentialIndex> {
    let mut tokens = tokens.into_iter();
    Ok(SequentialIndex {
        ethereum_event_configuration: tokens.next().try_parse()?,
        bridge_configuration_update_voting: tokens.next().try_parse()?,
    })
}

impl ParseToken<SequentialIndex> for TokenValue {
    fn try_parse(self) -> ContractResult<SequentialIndex> {
        match self {
            TokenValue::Tuple(tokens) => parse_sequential_index(tokens),
            _ => Err(ContractError::InvalidAbi),
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct BridgeConfiguration {
    #[serde(with = "serde_cells")]
    pub ethereum_event_configuration_code: Cell,
    pub ethereum_event_configuration_required_confirmations: BigUint,
    pub ethereum_event_configuration_required_rejects: BigUint,
    pub ethereum_event_configuration_initial_balance: BigUint,

    #[serde(with = "serde_cells")]
    pub ethereum_event_code: Cell,

    #[serde(with = "serde_cells")]
    pub bridge_configuration_update_code: Cell,
    pub bridge_configuration_update_required_confirmations: BigUint,
    pub bridge_configuration_update_required_rejections: BigUint,
    pub bridge_configuration_update_initial_balance: BigUint,

    pub active: bool,
}

impl StandaloneToken for BridgeConfiguration {}

fn parse_bridge_configuration(tokens: Vec<Token>) -> ContractResult<BridgeConfiguration> {
    let mut tokens = tokens.into_iter();
    Ok(BridgeConfiguration {
        ethereum_event_configuration_code: tokens.next().try_parse()?,
        ethereum_event_configuration_required_confirmations: tokens.next().try_parse()?,
        ethereum_event_configuration_required_rejects: tokens.next().try_parse()?,
        ethereum_event_configuration_initial_balance: tokens.next().try_parse()?,
        ethereum_event_code: tokens.next().try_parse()?,
        bridge_configuration_update_code: tokens.next().try_parse()?,
        bridge_configuration_update_required_confirmations: tokens.next().try_parse()?,
        bridge_configuration_update_required_rejections: tokens.next().try_parse()?,
        bridge_configuration_update_initial_balance: tokens.next().try_parse()?,
        active: tokens.next().try_parse()?,
    })
}

impl FunctionArgsGroup for BridgeConfiguration {
    fn token_values(self) -> Vec<TokenValue> {
        vec![
            self.ethereum_event_configuration_code.token_value(),
            BigUint256(self.ethereum_event_configuration_required_confirmations).token_value(),
            BigUint256(self.ethereum_event_configuration_required_rejects).token_value(),
            BigUint128(self.ethereum_event_configuration_initial_balance).token_value(),
            self.ethereum_event_code.token_value(),
            self.bridge_configuration_update_code.token_value(),
            BigUint256(self.bridge_configuration_update_required_confirmations).token_value(),
            BigUint256(self.bridge_configuration_update_required_rejections).token_value(),
            BigUint128(self.bridge_configuration_update_initial_balance).token_value(),
            self.active.token_value(),
        ]
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

impl TryFrom<ContractOutput> for BridgeConfiguration {
    type Error = ContractError;

    fn try_from(output: ContractOutput) -> ContractResult<Self> {
        parse_bridge_configuration(output.tokens)
    }
}

impl TryFrom<ContractOutput> for (BridgeConfiguration, SequentialIndex) {
    type Error = ContractError;

    fn try_from(output: ContractOutput) -> Result<Self, Self::Error> {
        let mut tuple = output.into_parser();
        Ok((tuple.parse_next()?, tuple.parse_next()?))
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct NewEventConfiguration {
    pub ethereum_event_abi: String,
    pub ethereum_event_address: ethereum_types::Address,
    pub ethereum_event_blocks_to_confirm: BigUint,
    pub required_confirmations: BigUint,
    pub required_rejections: BigUint,
    pub ethereum_event_initial_balance: BigUint,
    #[serde(with = "serde_int_addr")]
    pub event_proxy_address: MsgAddressInt,
}

impl FunctionArgsGroup for NewEventConfiguration {
    fn token_values(self) -> Vec<TokenValue> {
        vec![
            self.ethereum_event_abi.token_value(),
            hex::encode(&self.ethereum_event_address.as_bytes()).token_value(),
            BigUint256(self.ethereum_event_blocks_to_confirm).token_value(),
            BigUint256(self.required_confirmations).token_value(),
            BigUint256(self.required_rejections).token_value(),
            BigUint128(self.ethereum_event_initial_balance).token_value(),
            self.event_proxy_address.token_value(),
        ]
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
    pub event_required_confirmations: BigUint,
    pub event_required_rejects: BigUint,

    #[serde(with = "serde_vec_uint256")]
    pub confirm_keys: Vec<UInt256>,
    #[serde(with = "serde_vec_uint256")]
    pub reject_keys: Vec<UInt256>,
    pub active: bool,
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
            .map_err(|_| ContractError::InvalidEthAddress)?,
            event_proxy_address: tuple.parse_next()?,
            ethereum_event_blocks_to_confirm: tuple.parse_next()?,
            required_confirmations: tuple.parse_next()?,
            required_rejections: tuple.parse_next()?,
            event_required_confirmations: tuple.parse_next()?,
            event_required_rejects: tuple.parse_next()?,

            confirm_keys: tuple.parse_next()?,
            reject_keys: tuple.parse_next()?,
            active: tuple.parse_next()?,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BridgeConfigurationUpdateDetails {
    pub bridge_configuration: BridgeConfiguration,
    pub required_confirmations: BigUint,
    pub required_rejects: BigUint,
    #[serde(with = "serde_vec_uint256")]
    pub confirm_keys: Vec<UInt256>,
    #[serde(with = "serde_vec_uint256")]
    pub reject_keys: Vec<UInt256>,
    pub executed: bool,
}

impl StandaloneToken for BridgeConfigurationUpdateDetails {}

impl TryFrom<ContractOutput> for BridgeConfigurationUpdateDetails {
    type Error = ContractError;

    fn try_from(output: ContractOutput) -> ContractResult<Self> {
        let mut tuple = output.into_parser();
        Ok(BridgeConfigurationUpdateDetails {
            bridge_configuration: tuple.parse_next()?,
            required_confirmations: tuple.parse_next()?,
            required_rejects: tuple.parse_next()?,
            confirm_keys: tuple.parse_next()?,
            reject_keys: tuple.parse_next()?,
            executed: tuple.parse_next()?,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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
    pub required_confirmations: BigUint,
    pub required_rejections: BigUint,
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
            required_confirmations: tuple.parse_next()?,
            required_rejections: tuple.parse_next()?,
        })
    }
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
pub enum Voting {
    Confirm,
    Reject,
}
