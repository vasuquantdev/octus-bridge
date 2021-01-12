use num_traits::ToPrimitive;

use relay_eth::ws::H256;
use relay_models::models::EventVote;
use relay_ton::contracts::{EthEventDetails, TonEventDetails};
use relay_ton::prelude::{serde_std_addr, serde_uint256, MsgAddrStd, UInt256};

use crate::db_management::EthEventVotingData;

use super::prelude::*;

/// Vote for TON->ETH event, received from TON
#[derive(Debug, Clone, Copy)]
pub struct TonEventReceivedVote<'a> {
    pub configuration_id: &'a BigUint,
    pub vote: EventVote,
    pub event_addr: &'a MsgAddrStd,
    pub relay_key: &'a UInt256,
    pub data: &'a TonEventDetails,
}

impl std::fmt::Display for TonEventReceivedVote<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!(
            "TON->ETH event {:?} tx {} (block {}) from {}. status: {:?}",
            self.vote,
            hex::encode(&self.data.init_data.event_transaction),
            self.data.init_data.event_block_number,
            hex::encode(&self.relay_key),
            self.data.status
        ))
    }
}

/// Vote for ETH->TON event, received from TON
#[derive(Debug, Clone, Copy)]
pub struct EthEventReceivedVote<'a> {
    pub configuration_id: &'a BigUint,
    pub vote: EventVote,
    pub event_addr: &'a MsgAddrStd,
    pub relay_key: &'a UInt256,
    pub ethereum_event_blocks_to_confirm: u64,
    pub data: &'a EthEventDetails,
}

impl EthEventReceivedVote<'_> {
    pub fn target_block_number(&self) -> u64 {
        self.data
            .init_data
            .event_block_number
            .to_u64()
            .unwrap_or_else(u64::max_value)
            + self.ethereum_event_blocks_to_confirm
    }
}

impl std::fmt::Display for EthEventReceivedVote<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!(
            "ETH->TON event {:?} tx {} (block {}) from {}. status: {:?}",
            self.vote,
            hex::encode(&self.data.init_data.event_transaction),
            self.data.init_data.event_block_number,
            hex::encode(&self.relay_key),
            self.data.status,
        ))
    }
}

#[derive(Debug, Clone, thiserror::Error)]
pub enum EventValidationError {
    #[error("Invalid event transaction hash")]
    InvalidEventTransactionHash,
    #[error("Invalid block hash")]
    InvalidBlockHash,
}

impl From<EthEventReceivedVote> for EthEventVotingData {
    fn from(event: EthEventReceivedVote) -> Self {
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
            configuration_id: event.configuration_id,
        }
    }
}

pub struct Status {
    pub hash: H256,
    pub success: bool,
}
