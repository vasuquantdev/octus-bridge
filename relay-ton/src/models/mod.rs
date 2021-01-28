use ethereum_types::H160;
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
    pub pubkey: Option<ed25519_dalek::PublicKey>,
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
pub struct InternalMessageHeader {
    pub src: MsgAddressInt,
    pub value: u64,
}

#[derive(Debug, Clone)]
pub struct InternalMessage {
    pub dest: MsgAddressInt,
    pub init: Option<SliceData>,
    pub body: Option<SliceData>,
    pub header: InternalMessageHeader,
}

#[derive(Debug, Clone, Default)]
pub struct ContractOutput {
    pub transaction_id: Option<TransactionId>,
    pub tokens: Vec<Token>,
}

pub struct BridgeRelay {
    pub addr: MsgAddrStd,
    pub ethereum_account: H160,
    pub action: Action,
}

pub enum Action {
    Add,
    Remove,
}

impl From<bool> for Action {
    fn from(b: bool) -> Self {
        if b {
            Action::Add
        } else {
            Action::Remove
        }
    }
}

impl From<Action> for bool {
    fn from(action: Action) -> Self {
        matches!(action, Action::Add)
    }
}

struct BridgeRelayEth {
    account: H160,
    action: Action,
}

impl From<BridgeRelayEth> for Vec<u8> {
    fn from(relay: BridgeRelayEth) -> Self {
        const LEN: usize = H160::len_bytes() + std::mem::size_of::<bool>();
        let act: bool = relay.action.into();
        let mut buf = Vec::with_capacity(LEN);
        buf.extend(relay.account.0.iter());
        buf.push(act as u8);
        buf
    }
}
