use num_bigint::BigUint;
use relay_eth::ws::H256;
use relay_ton::contracts::EthereumEventDetails;
use relay_ton::prelude::{serde_cells, serde_std_addr, serde_uint256, Cell, MsgAddrStd, UInt256};
use super::prelude::*;

/// Event received from TON
#[derive(Debug, Clone, Hash, Serialize, Deserialize, Eq, PartialEq)]
pub struct ExtendedEventInfo {
    #[serde(with = "serde_std_addr")]
    pub event_addr: MsgAddrStd,
    #[serde(with = "serde_uint256")]
    pub relay_key: UInt256,
    pub data: EthereumEventDetails,
}

#[derive(Debug, Clone, Hash, Serialize, Deserialize, Eq, PartialEq)]
pub struct ReducedEventInfo {
    #[serde(with = "serde_std_addr")]
    pub event_addr: MsgAddrStd,
    relay_key: UInt256,
    pub ethereum_event_transaction: Vec<u8>,
    pub event_index: BigUint,
    #[serde(with = "serde_cells")]
    pub event_data: Cell,
    #[serde(with = "serde_std_addr")]
    pub event_block_number: BigUint,
    pub event_block: Vec<u8>,
    pub event_rejected: bool,
}

impl From<ExtendedEventInfo> for ReducedEventInfo {
    fn from(event: ExtendedEventInfo) -> Self {
        let data = event.data;
        Self {
            event_addr: event.event_addr,
            relay_key: event.relay_key,
            ethereum_event_transaction: data.ethereum_event_transaction,
            event_index: data.event_index,
            event_data: data.event_data,
            event_block_number: data.event_block_number,
            event_block: data.event_block,
            event_rejected: data.event_rejected,
        }
    }
}

pub struct Status {
    pub hash: H256,
    pub success: bool,
}

#[derive(Debug, Clone, Copy, Ord, PartialOrd, Eq, PartialEq)]
pub enum EventVote {
    Confirm,
    Reject,
}
