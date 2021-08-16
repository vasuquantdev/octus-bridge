use serde::{Deserialize, Serialize};

///topics: `Keccak256("Method_Signature")`
#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct StoredEthEvent {
    pub address: ethabi::Address,
    pub data: Vec<u8>,
    pub tx_hash: web3::types::H256,
    pub topics: Vec<web3::types::H256>,
    pub event_index: u32,
    pub block_number: u64,
    pub block_hash: web3::types::H256,
}
