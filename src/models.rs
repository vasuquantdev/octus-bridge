use borsh::{BorshDeserialize, BorshSerialize};

use relay_models::models::{
    EthEventVoteDataView, EthTonTransactionView, SignedVoteDataView, TonEthTransactionView,
    TonEventVoteDataView,
};
use relay_ton::contracts::*;
use relay_ton::prelude::*;
use relay_utils::exporter::*;

use super::prelude::*;

pub trait IntoView {
    type View: Serialize;

    fn into_view(self) -> Self::View;
}

impl IntoView for TonEventVoteData {
    type View = TonEventVoteDataView;

    fn into_view(self) -> Self::View {
        TonEventVoteDataView {
            configuration_id: self.configuration_id,
            event_transaction: hex::encode(self.event_transaction.as_slice()),
            event_transaction_lt: self.event_transaction_lt,
            event_index: self.event_index,
        }
    }
}

impl IntoView for SignedTonEventVoteData {
    type View = TonEventVoteDataView;

    fn into_view(self) -> Self::View {
        self.data.into_view()
    }
}

impl IntoView for EthEventVoteData {
    type View = EthEventVoteDataView;

    fn into_view(self) -> Self::View {
        let event_data = match serialize_toc(&self.event_data) {
            Ok(a) => hex::encode(a),
            Err(e) => {
                log::error!("Failed serializing boc: {}", e);
                "BAD DATA IN BLOCK".to_string()
            }
        };
        EthEventVoteDataView {
            configuration_id: self.configuration_id,
            event_transaction: hex::encode(&self.event_transaction.0),
            event_index: self.event_index,
            event_data,
            event_block_number: self.event_block_number,
            event_block: hex::encode(&self.event_block.0),
        }
    }
}

#[derive(BorshSerialize, BorshDeserialize, Clone)]
pub enum EventTransaction<C, R> {
    Confirm(C),
    Reject(R),
}

#[derive(BorshSerialize, BorshDeserialize, Clone)]
pub struct SignedTonEventVoteData {
    pub data: TonEventVoteData,
    pub signature: Vec<u8>,
}

pub type EthEventTransaction = EventTransaction<EthEventVoteData, EthEventVoteData>;
pub type TonEventTransaction = EventTransaction<SignedTonEventVoteData, TonEventVoteData>;

impl From<EthEventTransaction> for EthTonTransactionView {
    fn from(data: EthEventTransaction) -> Self {
        match data {
            EventTransaction::Confirm(a) => EthTonTransactionView::Confirm(a.into_view()),
            EventTransaction::Reject(a) => EthTonTransactionView::Reject(a.into_view()),
        }
    }
}

impl From<TonEventTransaction> for TonEthTransactionView {
    fn from(data: TonEventTransaction) -> Self {
        match data {
            EventTransaction::Confirm(a) => TonEthTransactionView::Confirm(SignedVoteDataView {
                signature: hex::encode(&a.signature),
                data: a.data.into_view(),
            }),
            EventTransaction::Reject(a) => TonEthTransactionView::Reject(a.into_view()),
        }
    }
}

#[derive(Debug, Clone)]
pub struct CommonReceivedVote<T, A> {
    configuration_id: u32,
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
        configuration_id: u32,
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
        configuration_id: u32,
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

    fn configuration_id(&self) -> u32;
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
    fn configuration_id(&self) -> u32 {
        self.configuration_id
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
    fn only_data(self) -> Self::Data;
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

    #[inline]
    fn only_data(self) -> Self::Data {
        self.data
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

pub trait IntoVote {
    type Vote;

    fn into_vote(self) -> Self::Vote;
}

impl IntoVote for EthEventReceivedVoteWithData {
    type Vote = EthEventVoteData;

    fn into_vote(self) -> Self::Vote {
        Self::Vote {
            configuration_id: self.info.configuration_id,
            event_transaction: self.data.init_data.event_transaction,
            event_index: self.data.init_data.event_index,
            event_data: self.data.init_data.event_data,
            event_block_number: self.data.init_data.event_block_number,
            event_block: self.data.init_data.event_block,
        }
    }
}

impl IntoVote for TonEventReceivedVoteWithData {
    type Vote = TonEventVoteData;

    fn into_vote(self) -> Self::Vote {
        Self::Vote {
            configuration_id: self.info.configuration_id,
            event_transaction: self.data.init_data.event_transaction,
            event_transaction_lt: self.data.init_data.event_transaction_lt,
            event_timestamp: self.data.init_data.event_timestamp,
            event_index: self.data.init_data.event_index,
            event_data: self.data.init_data.event_data,
        }
    }
}

const LABEL_CONFIGURATION_ID: &str = "configuration_id";

#[derive(Debug, Clone)]
pub struct EthEventsHandlerMetrics {
    pub configuration_id: u32,
    pub pending_vote_count: usize,
    pub failed_vote_count: usize,
    pub successful_vote_count: usize,
}

impl std::fmt::Display for EthEventsHandlerMetrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.begin_metric("eth_pending_vote_count")
            .label(LABEL_CONFIGURATION_ID, self.configuration_id)
            .value(self.pending_vote_count)?;
        f.begin_metric("eth_failed_vote_count")
            .label(LABEL_CONFIGURATION_ID, self.configuration_id)
            .value(self.failed_vote_count)?;
        f.begin_metric("eth_successful_vote_count")
            .label(LABEL_CONFIGURATION_ID, self.configuration_id)
            .value(self.successful_vote_count)
    }
}

#[derive(Debug, Clone)]
pub struct TonEventsHandlerMetrics {
    pub configuration_id: u32,
    pub verification_queue_size: usize,
    pub pending_vote_count: usize,
    pub failed_vote_count: usize,
    pub successful_vote_count: usize,
}

impl std::fmt::Display for TonEventsHandlerMetrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.begin_metric("ton_verification_queue_size")
            .label(LABEL_CONFIGURATION_ID, self.configuration_id)
            .value(self.verification_queue_size)?;
        f.begin_metric("ton_pending_vote_count")
            .label(LABEL_CONFIGURATION_ID, self.configuration_id)
            .value(self.pending_vote_count)?;
        f.begin_metric("ton_failed_vote_count")
            .label(LABEL_CONFIGURATION_ID, self.configuration_id)
            .value(self.failed_vote_count)?;
        f.begin_metric("ton_successful_vote_count")
            .label(LABEL_CONFIGURATION_ID, self.configuration_id)
            .value(self.successful_vote_count)
    }
}

#[derive(Debug, Clone)]
pub struct BridgeMetrics {
    pub eth_verification_queue_size: usize,
    pub eth_event_handlers_metrics: Vec<EthEventsHandlerMetrics>,
    pub ton_event_handlers_metrics: Vec<TonEventsHandlerMetrics>,
}

impl std::fmt::Display for BridgeMetrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.begin_metric("eth_verification_queue_size")
            .value(self.eth_verification_queue_size)?;
        for item in self.eth_event_handlers_metrics.iter() {
            item.fmt(f)?;
        }
        for item in self.ton_event_handlers_metrics.iter() {
            item.fmt(f)?;
        }
        Ok(())
    }
}
