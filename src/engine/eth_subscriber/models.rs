use std::convert::TryFrom;

use anyhow::Context;
use serde::{Deserialize, Serialize};
use web3::types::{Log, H256};

pub type EventId = (H256, u32);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ParsedEthEvent {
    Removed(RemovedEthEvent),
    Received(ReceivedEthEvent),
}

impl ParsedEthEvent {
    pub fn event_id(&self) -> EventId {
        match self {
            Self::Removed(event) => (event.transaction_hash, event.event_index),
            Self::Received(event) => (event.transaction_hash, event.event_index),
        }
    }

    pub fn transaction_hash(&self) -> &H256 {
        match self {
            Self::Removed(event) => &event.transaction_hash,
            Self::Received(event) => &event.transaction_hash,
        }
    }

    pub fn event_index(&self) -> u32 {
        match self {
            Self::Removed(event) => event.event_index,
            Self::Received(event) => event.event_index,
        }
    }
}

impl TryFrom<Log> for ParsedEthEvent {
    type Error = anyhow::Error;

    fn try_from(log: Log) -> Result<Self, Self::Error> {
        let transaction_hash = log
            .transaction_hash
            .context("Transaction hash was not found in event")?;

        let event_index = log
            .log_index
            .map(|x| x.as_u32())
            .context("Event index was not found in event")?;

        if let Some(true) = log.removed {
            return Ok(ParsedEthEvent::Removed(RemovedEthEvent {
                transaction_hash,
                event_index,
            }));
        }

        let block_number = log
            .block_number
            .map(|x| x.as_u64())
            .context("Block number was not found in event")?;

        let block_hash = log
            .block_hash
            .context("Block hash was not found in event")?;

        let data = log.data.0;

        log::debug!(
            "Received logs from block {} with hash {}",
            block_number,
            transaction_hash
        );

        Ok(ParsedEthEvent::Received(ReceivedEthEvent {
            data,
            transaction_hash,
            event_index,
            block_number,
            block_hash,
        }))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemovedEthEvent {
    pub transaction_hash: H256,
    pub event_index: u32,
}

/// Topics: `Keccak256("Method_Signature")`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReceivedEthEvent {
    pub transaction_hash: H256,
    pub event_index: u32,
    pub block_number: u64,
    pub block_hash: H256,
    pub data: Vec<u8>,
}
