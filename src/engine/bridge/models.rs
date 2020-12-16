
use num_traits::ToPrimitive;
use relay_eth::ws::H256;
use relay_ton::contracts::EthereumEventDetails;
use relay_ton::prelude::{
    serde_std_addr, serde_uint256,  MsgAddrStd, MsgAddressInt, UInt256,
};

use super::prelude::*;
use crate::db_managment::EthTonConfirmationData;

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
    pub fn target_block_number(&self) -> u64 {
        self.data
            .event_block_number
            .to_u64()
            .unwrap_or_else(u64::max_value)
            + self.ethereum_event_blocks_to_confirm
    }
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
