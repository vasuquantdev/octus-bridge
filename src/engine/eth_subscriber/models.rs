use serde::{Deserialize, Serialize};
use std::convert::TryFrom;
use web3::types::{Address, Log, H256};

use crate::state::models::StoredEthEvent;

impl TryFrom<Log> for StoredEthEvent {
    type Error = anyhow::Error;

    fn try_from(log: Log) -> Result<Self, Self::Error> {
        let data = log.data.0;
        let hash = match log.transaction_hash {
            Some(a) => a,
            None => {
                anyhow::bail!("No tx hash in log");
            }
        };
        let block_number = match log.block_number {
            Some(a) => a.as_u64(),
            None => {
                anyhow::bail!("No block number in log!")
            }
        };
        let event_index = match log.log_index {
            Some(a) => a.as_u32(),
            None => {
                log::warn!(
                    "No transaction_log_index in log. Tx hash: {}. Block: {}",
                    hash,
                    block_number,
                );
                0
            }
        };
        let block_hash = match log.block_hash {
            Some(a) => a,
            None => {
                anyhow::bail!("No hash in log. Tx hash: {}. Block: {}", hash, block_number);
            }
        };

        log::debug!("Sent logs from block {} with hash {}", block_number, hash);
        Ok(StoredEthEvent {
            address: log.address,
            data,
            tx_hash: hash,
            topics: log.topics,
            event_index,
            block_number,
            block_hash,
        })
    }
}
