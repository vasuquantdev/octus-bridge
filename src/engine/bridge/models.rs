use relay_eth::ws::H256;
use relay_ton::contracts::EthereumEventDetails;
use relay_ton::prelude::{serde_std_addr, serde_uint256, MsgAddrStd, UInt256};

use super::prelude::*;

/// Event received from TON
#[derive(Debug, Clone, Hash, Serialize, Deserialize)]
pub struct ExtendedEventInfo {
    #[serde(with = "serde_std_addr")]
    pub event_addr: MsgAddrStd,
    #[serde(with = "serde_uint256")]
    pub relay_key: UInt256,
    pub data: EthereumEventDetails,
}

pub struct Status {
    pub hash: H256,
    pub sucess: bool,
}
