use num_bigint::BigInt;
use ton_abi::Token;
use ton_block::{MsgAddrStd, MsgAddressInt};
use ton_types::{SliceData, UInt256};

#[derive(Debug, Clone)]
pub struct ContractConfig {
    pub account: MsgAddressInt,
    pub timeout_sec: u32,
}

#[derive(Debug, Clone)]
pub struct AccountState {
    pub balance: BigInt,
    pub last_transaction: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct AccountId {
    pub workchain: i8,
    pub addr: UInt256,
}

impl From<AccountId> for MsgAddrStd {
    fn from(id: AccountId) -> Self {
        Self::with_address(
            None,
            id.workchain,
            SliceData::new(id.addr.as_slice().to_vec()),
        )
    }
}

#[derive(Debug, Clone)]
pub struct TransactionIdShort {
    pub lt: u64,
    pub hash: UInt256,
}

#[derive(Debug, Clone)]
pub struct TransactionId {
    pub workchain: i8,
    pub addr: UInt256,
    pub lt: u64,
    pub hash: UInt256,
}

#[derive(Debug, Clone)]
pub struct ExternalMessageHeader {
    pub time: u64,
    pub expire: u32,
}

#[derive(Debug, Clone)]
pub struct ExternalMessage {
    pub dest: MsgAddressInt,
    pub init: Option<SliceData>,
    pub body: Option<SliceData>,
    pub header: ExternalMessageHeader,
    pub run_local: bool,
}

#[derive(Debug, Clone)]
pub struct ContractOutput {
    pub transaction_id: Option<TransactionId>,
    pub tokens: Vec<Token>,
}
