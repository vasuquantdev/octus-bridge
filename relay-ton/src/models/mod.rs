use num_bigint::{BigInt, BigUint};
use ton_block::{MsgAddressInt, MsgAddrStd};
use ton_types::{SliceData, UInt256};

#[derive(Debug, Clone)]
pub struct AccountState {
    pub balance: BigInt,
    pub last_transaction: Option<TransactionIdShort>,
}

#[derive(Debug, Clone)]
pub struct AccountId {
    pub workchain: i8,
    pub addr: UInt256,
}

impl From<AccountId> for MsgAddressInt {
    fn from(id: AccountId) -> Self {
        Self::AddrStd(MsgAddrStd::with_address(None, id.workchain, SliceData::new(id.addr.into())))
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
pub struct ExternalMessage {
    pub dest: MsgAddressInt,
    pub init: Option<SliceData>,
    pub body: Option<SliceData>,
    pub expires_in: u32,
    pub run_local: bool,
}
