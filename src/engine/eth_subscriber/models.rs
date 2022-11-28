use std::convert::TryFrom;

use anyhow::Context;
use serde::{Deserialize, Serialize};
use web3::types::{Log, H160, H256};

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
        let address = log.address;

        let transaction_hash = log
            .transaction_hash
            .context("Transaction hash was not found in event")?;

        let event_index = log
            .log_index
            .map(|x| x.as_u32())
            .context("Event index was not found in event")?;

        if let Some(true) = log.removed {
            return Ok(ParsedEthEvent::Removed(RemovedEthEvent {
                address,
                transaction_hash,
                event_index,
            }));
        }

        let topic_hash = log
            .topics
            .into_iter()
            .next()
            .context("Topic hash not found")?;

        let block_number = log
            .block_number
            .map(|x| x.as_u64())
            .context("Block number was not found in event")?;

        let block_hash = log
            .block_hash
            .context("Block hash was not found in event")?;

        let data = log.data.0;

        tracing::debug!(
            block_number,
            tx = hex::encode(transaction_hash.0),
            "received logs",
        );

        Ok(ParsedEthEvent::Received(ReceivedEthEvent {
            address,
            topic_hash,
            transaction_hash,
            event_index,
            block_number,
            block_hash,
            data,
        }))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemovedEthEvent {
    pub address: H160,
    pub transaction_hash: H256,
    pub event_index: u32,
}

/// Topics: `Keccak256("Method_Signature")`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReceivedEthEvent {
    pub address: H160,
    pub topic_hash: H256,
    pub transaction_hash: H256,
    pub event_index: u32,
    pub block_number: u64,
    pub block_hash: H256,
    pub data: Vec<u8>,
}
