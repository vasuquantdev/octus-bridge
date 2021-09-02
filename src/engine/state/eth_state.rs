use std::borrow::BorrowMut;
use std::collections::HashMap;

use anyhow::Result;
use nekoton_utils::TrustMe;
use uuid::Uuid;
use web3::types::H256;

use crate::engine::eth_subscriber::models::*;
use crate::utils::*;

pub trait EthStateTable: Sized {
    fn eth_state(&mut self) -> EthState<&mut Self> {
        EthState(self)
    }
}

impl EthStateTable for PooledConnection<'_> {}

pub struct EthState<T>(pub T);

impl<'a, T> EthState<T>
where
    T: BorrowMut<PooledConnection<'a>>,
{
    pub fn set_last_block_number(&self, chain_id: u32, last_block_number: u64) -> Result<()> {
        self.conn().execute_in_place(
            "UPDATE eth_last_block SET block_number=?1 WHERE chain_id=?2",
            rusqlite::params![last_block_number, chain_id],
        )?;
        Ok(())
    }

    pub fn get_last_block_number(&self, chain_id: u32) -> Result<u64> {
        let block_number: u64 = self.conn().query_row_in_place(
            "SELECT block_number FROM eth_last_block WHERE chain_id = ?1",
            rusqlite::params![chain_id],
            |row| row.get(0),
        )?;
        Ok(block_number)
    }

    pub fn get_last_block_numbers(&self) -> Result<HashMap<u32, u64>> {
        fn process_row(row: rusqlite::Result<(u32, u64)>) -> Option<(u32, u64)> {
            match row {
                Ok(row) => Some(row),
                Err(e) => {
                    log::error!("Failed fetching ETH event from db: {:?}", e);
                    return None;
                }
            }
        }

        let mut stmt = self
            .conn()
            .prepare("SELECT chain_id, block_number FROM eth_last_block")?;
        let result = stmt
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
            .filter_map(process_row)
            .collect();
        Ok(result)
    }

    pub fn save_events(
        &mut self,
        chain_id: u32,
        events: &[StoredEthEvent],
        last_block_number: u64,
    ) -> Result<()> {
        const QUERY: &'static str = r#"
        INSERT INTO eth_events (
            chain_id,
            event_transaction,
            event_index,
            event_data,
            event_block_number,
            event_block,
            address,
            target_event_block,
            status
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
        "#;

        let mut tx = self.0.borrow_mut().transaction()?;

        let mut stmt = tx.prepare(QUERY)?;
        for item in events {
            stmt.execute(rusqlite::params![
                chain_id,
                item.event.transaction_hash.0,
                item.event.event_index,
                item.event.data,
                item.event.block_number,
                item.event.block_hash.0,
                item.event.address.0,
                item.target_event_block,
                item.status
            ])?;
        }
        stmt.finalize()?;

        tx.execute(
            "UPDATE eth_last_block SET block_number=?1 WHERE chain_id=?2",
            rusqlite::params![last_block_number, chain_id],
        )?;

        tokio::task::block_in_place(|| tx.commit())?;

        Ok(())
    }

    pub fn get_event(&self, chain_id: u32, event_transaction: &[u8; 32]) -> Result<StoredEthEvent> {
        fn process_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<SelectedRow> {
            Ok(SelectedRow {
                event_index: row.get(0)?,
                event_data: row.get(1)?,
                event_block_number: row.get(2)?,
                event_block: row.get(3)?,
                address: row.get(4)?,
                target_event_block: row.get(5)?,
                status: row.get(6)?.into(),
            })
        }

        struct SelectedRow {
            event_index: u32,
            event_data: Vec<u8>,
            event_block_number: u64,
            event_block: [u8; 32],
            address: [u8; 20],
            target_event_block: u32,
            status: StoredEthEventStatus,
        }

        const QUERY: &'static str = r#"
        SELECT event_index, event_data, event_block_number, event_block, address, target_event_block, status 
        FROM eth_events 
        WHERE chain_id = ?1 AND event_transaction = ?2
        "#;

        let result = self.conn().query_row_in_place(
            QUERY,
            rusqlite::params![chain_id, event_transaction],
            process_row,
        )?;

        Ok(StoredEthEvent {
            event: ReceivedEthEvent {
                address: ethabi::Address::from(result.address),
                data: result.event_data,
                transaction_hash: H256::from(event_transaction),
                event_index: result.event_index,
                block_number: result.event_block_number,
                block_hash: H256::from(result.event_block),
            },
            target_event_block: result.4,
            status: StoredEthEventStatus::InProgress,
        })
    }

    pub fn remove_event(&self, chain_id: u32, transaction: &[u8; 32]) -> Result<()> {
        self.conn().execute_in_place(
            "DELETE FROM eth_events WHERE entry_id = ?1 AND transaction = ?2",
            rusqlite::params![chain_id, transaction],
        )?;
        Ok(())
    }

    pub fn get_pending_events(&self, chain_id: u32) -> Result<Vec<StoredEthEvent>> {
        fn process_row(row: &rusqlite::Row<'_>) -> Result<StoredEthEvent> {
            Ok(StoredEthEvent {
                event: ReceivedEthEvent {
                    address: From::<[u8; 32]>::from(row.get(0)?),
                    data: row.get(1)?,
                    transaction_hash: From::<[u8; 32]>::from(row.get(2)?),
                    event_index: row.get(3)?,
                    block_number: row.get(4)?,
                    block_hash: From::<[u8; 32]>::from(row.get(5)?),
                },
                target_event_block: row.get(6)?,
                status: row.get(7)?,
            })
        }

        const QUERY: &'static str = r#"
        SELECT (
            address,
            event_data,
            event_transaction,
            event_index,
            event_block_number,
            event_block,
            target_event_block,
            status
        ) FROM eth_events WHERE chain_id=?1 AND status=0
        "#;

        let mut stmt = self.conn().prepare(QUERY)?;
        let result = stmt
            .query_map(rusqlite::params![chain_id], process_row)?
            .collect::<rusqlite::Result<_>>()?;
        Ok(result)
    }

    fn conn(&self) -> &PooledConnection<'a> {
        self.0.borrow()
    }
}

pub struct StoredEthEvent {
    pub event: ReceivedEthEvent,
    pub target_event_block: u32,
    pub status: StoredEthEventStatus,
}

pub enum StoredEthEventStatus {
    /// Subscriber is waiting for several subsequent blocks
    InProgress,
    /// Transaction definitely exists
    Verified,
    /// Transaction doesn't exist
    Invalid,
}

impl From<StoredEthEventStatus> for u8 {
    fn from(status: StoredEthEventStatus) -> Self {
        match status {
            StoredEthEventStatus::InProgress => 0,
            StoredEthEventStatus::Verified => 1,
            StoredEthEventStatus::Invalid => 2,
        }
    }
}

impl From<u8> for StoredEthEventStatus {
    fn from(value: u8) -> Self {
        match value {
            1 => Self::Verified,
            2 => Self::Invalid,
            _ => Self::InProgress,
        }
    }
}
