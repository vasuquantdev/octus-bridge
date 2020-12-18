use num_traits::ToPrimitive;

use relay_eth::ws::H256;
use relay_ton::contracts::EthereumEventDetails;
use relay_ton::prelude::{serde_std_addr, serde_uint256, MsgAddrStd, MsgAddressInt, UInt256};

use crate::db_managment::EthTonConfirmationData;

use super::prelude::*;

/// Event received from TON
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExtendedEventInfo {
    pub vote: EventVote,
    #[serde(with = "serde_std_addr")]
    pub event_addr: MsgAddrStd,
    #[serde(with = "serde_uint256")]
    pub relay_key: UInt256,
    pub ethereum_event_blocks_to_confirm: u64,
    pub data: EthereumEventDetails,
}

impl ExtendedEventInfo {
    pub fn validate_structure(self) -> ValidatedEventStructure {
        if self.data.ethereum_event_transaction.len() != 32 {
            return ValidatedEventStructure::Invalid(
                self,
                EventValidationError::InvalidEventTransactionHash,
            );
        }

        if self.data.event_block.len() != 32 {
            return ValidatedEventStructure::Invalid(self, EventValidationError::InvalidBlockHash);
        }

        ValidatedEventStructure::Valid(self)
    }

    pub fn target_block_number(&self) -> u64 {
        self.data
            .event_block_number
            .to_u64()
            .unwrap_or_else(u64::max_value)
            + self.ethereum_event_blocks_to_confirm
    }
}

pub enum ValidatedEventStructure {
    Valid(ExtendedEventInfo),
    Invalid(ExtendedEventInfo, EventValidationError),
}

impl std::fmt::Display for ValidatedEventStructure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Valid(event) => f.write_fmt(format_args!(
                "{:?} tx {} (block {}) from {}. executed: {}, rejected: {}",
                event.vote,
                hex::encode(&event.data.ethereum_event_transaction),
                event.data.event_block_number,
                hex::encode(&event.relay_key),
                event.data.proxy_callback_executed,
                event.data.event_rejected
            )),
            Self::Invalid(event, EventValidationError::InvalidBlockHash) => {
                f.write_fmt(format_args!(
                    "invalid {:?} tx {} (block {}) from {}. executed: {}, rejected: {}",
                    event.vote,
                    hex::encode(&event.data.ethereum_event_transaction),
                    event.data.event_block_number,
                    hex::encode(&event.relay_key),
                    event.data.proxy_callback_executed,
                    event.data.event_rejected
                ))
            }
            Self::Invalid(event, EventValidationError::InvalidEventTransactionHash) => {
                f.write_fmt(format_args!(
                    "invalid {:?} tx INVALID (block {}) from {}. executed: {}, rejected: {}",
                    event.vote,
                    event.data.event_block_number,
                    hex::encode(&event.relay_key),
                    event.data.proxy_callback_executed,
                    event.data.event_rejected
                ))
            }
        }
    }
}

#[derive(Debug, Clone, thiserror::Error)]
pub enum EventValidationError {
    #[error("Invalid event transaction hash")]
    InvalidEventTransactionHash,
    #[error("Invalid block hash")]
    InvalidBlockHash,
}

impl From<ExtendedEventInfo> for EthTonConfirmationData {
    fn from(event: ExtendedEventInfo) -> Self {
        Self {
            event_transaction: event.data.ethereum_event_transaction,
            event_index: event
                .data
                .event_index
                .to_u64()
                .unwrap_or_else(u64::max_value),
            event_data: event.data.event_data,
            event_block_number: event
                .data
                .event_block_number
                .to_u64()
                .unwrap_or_else(u64::max_value),
            event_block: event.data.event_block,
            ethereum_event_configuration_address: MsgAddressInt::AddrStd(
                event.data.event_configuration_address,
            ),
            construction_time: chrono::Utc::now(),
        }
    }
}

pub struct Status {
    pub hash: H256,
    pub success: bool,
}

#[derive(Debug, Clone, Copy, Ord, PartialOrd, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum EventVote {
    Confirm,
    Reject,
}
