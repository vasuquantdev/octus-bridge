use num_bigint::BigUint;

use relay_eth::ws::H256;
use relay_models::models::{
    EthEventVotingDataView, EthTonTransactionView, SignedEventDataView, TonEthTransactionView,
    TonEventVotingDataView,
};
use relay_ton::contracts::*;
use relay_ton::prelude::*;

use super::prelude::*;

pub mod buf_to_hex {
    use serde::{Deserialize, Deserializer, Serializer};

    /// Serializes `buffer` to a lowercase hex string.
    pub fn serialize<T, S>(buffer: &T, serializer: S) -> Result<S::Ok, S::Error>
    where
        T: AsRef<[u8]> + ?Sized,
        S: Serializer,
    {
        serializer.serialize_str(&*hex::encode(&buffer.as_ref()))
    }

    /// Deserializes a lowercase hex string to a `Vec<u8>`.
    pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
    where
        D: Deserializer<'de>,
    {
        use serde::de::Error;
        String::deserialize(deserializer)
            .and_then(|string| hex::decode(string).map_err(|e| D::Error::custom(e.to_string())))
    }
}

pub mod h256_to_hex {
    use ethereum_types::H256;
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<T, S>(buffer: &T, serializer: S) -> Result<S::Ok, S::Error>
    where
        T: AsRef<[u8]> + ?Sized,
        S: Serializer,
    {
        serializer.serialize_str(&*hex::encode(&buffer.as_ref()))
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<H256, D::Error>
    where
        D: Deserializer<'de>,
    {
        use serde::de::Error;

        String::deserialize(deserializer).and_then(|string| {
            hex::decode(string)
                .map_err(|e| D::Error::custom(e.to_string()))
                .map(|x| H256::from_slice(&*x))
        })
    }
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct TonEventVotingData {
    #[serde(with = "serde_uint256")]
    pub event_transaction: UInt256,
    pub event_transaction_lt: u64,
    pub event_index: u64,
    #[serde(with = "serde_cells")]
    pub event_data: Cell,

    pub configuration_id: BigUint,
}

impl From<TonEventVotingData> for TonEventInitData {
    fn from(data: TonEventVotingData) -> Self {
        TonEventInitData {
            event_transaction: data.event_transaction,
            event_transaction_lt: data.event_transaction_lt,
            event_index: data.event_index,
            event_data: data.event_data,
            ton_event_configuration: Default::default(),
            required_confirmations: Default::default(),
            required_rejections: Default::default(),
        }
    }
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct EthEventVotingData {
    pub event_transaction: H256,
    pub event_index: u64,
    #[serde(with = "serde_cells")]
    pub event_data: Cell,
    pub event_block_number: u64,
    pub event_block: H256,

    pub configuration_id: BigUint,
}

impl From<EthEventVotingData> for EthEventInitData {
    fn from(data: EthEventVotingData) -> Self {
        EthEventInitData {
            event_transaction: data.event_transaction,
            event_index: data.event_index.into(),
            event_data: data.event_data,
            event_block_number: data.event_block_number.into(),
            event_block: data.event_block,

            // TODO: replace voting data model
            eth_event_configuration: Default::default(),
            required_confirmations: Default::default(),
            required_rejections: Default::default(),
            proxy_address: Default::default(),
        }
    }
}

impl From<EthEventVotingData> for EthEventVotingDataView {
    fn from(data: EthEventVotingData) -> Self {
        let event_block = match serialize_toc(&data.event_data) {
            Ok(a) => hex::encode(a),
            Err(e) => {
                log::error!("Failed serializing boc: {}", e);
                "BAD DATA IN BLOCK".to_string()
            }
        };
        EthEventVotingDataView {
            event_transaction: hex::encode(&data.event_transaction.0),
            event_index: data.event_index,
            event_data: event_block,
            event_block_number: data.event_block_number,
            event_block: hex::encode(&data.event_block.0),
            configuration_id: data.configuration_id.to_string(),
        }
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub enum EventTransaction<C, R> {
    Confirm(C),
    Reject(R),
}

#[derive(Serialize, Deserialize, Clone)]
pub struct SignedEventVotingData {
    pub data: TonEventVotingData,
    pub signature: Vec<u8>,
}

impl From<TonEventVotingData> for TonEventVotingDataView {
    fn from(data: TonEventVotingData) -> Self {
        TonEventVotingDataView {
            event_transaction: hex::encode(data.event_transaction.as_slice()),
            event_transaction_lt: data.event_transaction_lt,
            event_index: data.event_index,
            configuration_id: data.configuration_id.to_string(),
        }
    }
}

pub type EthEventTransaction = EventTransaction<EthEventVotingData, EthEventVotingData>;
pub type TonEventTransaction = EventTransaction<SignedEventVotingData, TonEventVotingData>;

impl From<EthEventTransaction> for EthTonTransactionView {
    fn from(data: EthEventTransaction) -> Self {
        match data {
            EventTransaction::Confirm(a) => EthTonTransactionView::Confirm(a.into()),
            EventTransaction::Reject(a) => EthTonTransactionView::Reject(a.into()),
        }
    }
}

impl From<TonEventTransaction> for TonEthTransactionView {
    fn from(data: TonEventTransaction) -> Self {
        match data {
            EventTransaction::Confirm(a) => TonEthTransactionView::Confirm(SignedEventDataView {
                signature: hex::encode(&a.signature),
                data: a.data.into(),
            }),
            EventTransaction::Reject(a) => TonEthTransactionView::Reject(a.into()),
        }
    }
}

#[derive(Debug, Clone)]
pub struct CommonReceivedVote<T, A> {
    configuration_id: BigUint,
    event_addr: MsgAddrStd,
    relay: MsgAddrStd,
    kind: Voting,
    additional_data: T,
    _data: std::marker::PhantomData<A>,
}

#[derive(Debug, Clone)]
pub struct CommonReceivedVoteWithData<T, D>
where
    D: ReceivedVoteEventData,
{
    info: CommonReceivedVote<T, D>,
    data: D,
}

pub type EthEventReceivedVote = CommonReceivedVote<u16, EthEventDetails>;
pub type TonEventReceivedVote = CommonReceivedVote<Arc<AbiEvent>, TonEventDetails>;

impl EthEventReceivedVote {
    pub fn new(
        configuration_id: BigUint,
        event_addr: MsgAddrStd,
        relay: MsgAddrStd,
        kind: Voting,
        eth_blocks_to_confirm: u16,
    ) -> Self {
        Self {
            configuration_id,
            event_addr,
            relay,
            kind,
            additional_data: eth_blocks_to_confirm,
            _data: Default::default(),
        }
    }
}

impl TonEventReceivedVote {
    pub fn new(
        configuration_id: BigUint,
        event_addr: MsgAddrStd,
        relay: MsgAddrStd,
        kind: Voting,
        abi: Arc<AbiEvent>,
    ) -> Self {
        Self {
            configuration_id,
            event_addr,
            relay,
            kind,
            additional_data: abi,
            _data: Default::default(),
        }
    }
}

pub type EthEventReceivedVoteWithData = <EthEventReceivedVote as ReceivedVote>::VoteWithData;
pub type TonEventReceivedVoteWithData = <TonEventReceivedVote as ReceivedVote>::VoteWithData;

pub trait ReceivedVote: Send + Sync {
    type AdditionalData;
    type Data: ReceivedVoteEventData;
    type VoteWithData: ReceivedVoteWithData;

    fn configuration_id(&self) -> &BigUint;
    fn event_address(&self) -> &MsgAddrStd;
    fn relay(&self) -> &MsgAddrStd;
    fn kind(&self) -> Voting;
    fn additional(&self) -> &Self::AdditionalData;
    fn with_data(self, data: Self::Data) -> Self::VoteWithData;
}

impl<T, D> ReceivedVote for CommonReceivedVote<T, D>
where
    T: Send + Sync,
    D: ReceivedVoteEventData + Send + Sync,
{
    type AdditionalData = T;
    type Data = D;
    type VoteWithData = CommonReceivedVoteWithData<T, D>;

    #[inline]
    fn configuration_id(&self) -> &BigUint {
        &self.configuration_id
    }

    #[inline]
    fn event_address(&self) -> &MsgAddrStd {
        &self.event_addr
    }

    #[inline]
    fn relay(&self) -> &MsgAddrStd {
        &self.relay
    }

    #[inline]
    fn kind(&self) -> Voting {
        self.kind
    }

    #[inline]
    fn additional(&self) -> &Self::AdditionalData {
        &self.additional_data
    }

    #[inline]
    fn with_data(self, data: Self::Data) -> Self::VoteWithData {
        CommonReceivedVoteWithData { info: self, data }
    }
}

pub trait ReceivedVoteWithData: Send + Sync {
    type Info: ReceivedVote + Send + Sync;
    type Data: ReceivedVoteEventData + Send + Sync;

    fn status(&self) -> EventStatus;
    fn info(&self) -> &Self::Info;
    fn data(&self) -> &Self::Data;
}

impl<T, D> ReceivedVoteWithData for CommonReceivedVoteWithData<T, D>
where
    T: Send + Sync,
    D: ReceivedVoteEventData + Send + Sync,
{
    type Info = CommonReceivedVote<T, D>;
    type Data = D;

    #[inline]
    fn status(&self) -> EventStatus {
        self.data.status()
    }

    #[inline]
    fn info(&self) -> &Self::Info {
        &self.info
    }

    #[inline]
    fn data(&self) -> &Self::Data {
        &self.data
    }
}

pub trait ReceivedVoteEventData {
    fn status(&self) -> EventStatus;
}

impl ReceivedVoteEventData for EthEventDetails {
    fn status(&self) -> EventStatus {
        self.status
    }
}

impl ReceivedVoteEventData for TonEventDetails {
    fn status(&self) -> EventStatus {
        self.status
    }
}

impl From<EthEventReceivedVoteWithData> for EthEventVotingData {
    fn from(event: EthEventReceivedVoteWithData) -> Self {
        Self {
            event_transaction: event.data.init_data.event_transaction,
            event_index: event
                .data
                .init_data
                .event_index
                .to_u64()
                .unwrap_or_else(u64::max_value),
            event_data: event.data.init_data.event_data,
            event_block_number: event
                .data
                .init_data
                .event_block_number
                .to_u64()
                .unwrap_or_else(u64::max_value),
            event_block: event.data.init_data.event_block,
            configuration_id: event.info.configuration_id,
        }
    }
}

impl From<TonEventReceivedVoteWithData> for TonEventVotingData {
    fn from(event: TonEventReceivedVoteWithData) -> Self {
        Self {
            event_transaction: event.data.init_data.event_transaction,
            event_transaction_lt: event.data.init_data.event_transaction_lt,
            event_index: event.data.init_data.event_index,
            event_data: event.data.init_data.event_data,

            configuration_id: event.info.configuration_id,
        }
    }
}
