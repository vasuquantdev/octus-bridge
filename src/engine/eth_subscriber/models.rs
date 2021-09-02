use std::convert::TryFrom;

use serde::{Deserialize, Serialize};
use web3::types::Log;

/// Topics: `Keccak256("Method_Signature")`
#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ReceivedEthEvent {
    pub address: ethabi::Address,
    pub data: Vec<u8>,
    pub transaction_hash: web3::types::H256,
    pub event_index: u32,
    pub block_number: u64,
    pub block_hash: web3::types::H256,
}

impl TryFrom<Log> for ReceivedEthEvent {
    type Error = anyhow::Error;

    fn try_from(log: Log) -> Result<Self, Self::Error> {
        if let Some(true) = log.removed {
            return Ok();
        }

        let data = log.data.0;
        let transaction_hash = log
            .transaction_hash
            .ok_or(ReceivedEthEventError::TransactionHashNotFound)?;

        let block_number = log
            .block_number
            .map(|x| x.as_u64())
            .ok_or(ReceivedEthEventError::BlockNumberNotFound)?;

        let block_hash = log
            .block_hash
            .ok_or(ReceivedEthEventError::BlockHashNotFound)?;

        let event_index = log
            .log_index
            .map(|x| x.as_u32())
            .ok_or(ReceivedEthEventError::EventIndexNotFound)?;

        log::debug!(
            "Received logs from block {} with hash {}",
            block_number,
            transaction_hash
        );

        Ok(ReceivedEthEvent {
            address: log.address,
            data,
            transaction_hash,
            event_index,
            block_number,
            block_hash,
        })
    }
}

#[derive(thiserror::Error, Debug)]
enum ReceivedEthEventError {
    #[error("Transaction hash was not found in event")]
    TransactionHashNotFound,
    #[error("Block number was not found in event")]
    BlockNumberNotFound,
    #[error("Block hash was not found in event")]
    BlockHashNotFound,
    #[error("Event index was not found in event")]
    EventIndexNotFound,
}
