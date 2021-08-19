use std::borrow::Borrow;
use std::str::FromStr;
use std::sync::Arc;

use anyhow::Result;
use futures::StreamExt;
use nekoton_utils::TrustMe;
use uuid::Uuid;

use crate::engine::eth_subscriber::models::*;
use crate::utils::*;

pub trait EthStateTable: Sized {
    fn eth_state(&self) -> EthState<&Self> {
        EthState(self)
    }
}

impl EthStateTable for PooledConnection<'_> {}

pub struct EthState<T>(pub T);

impl<'a, T> EthState<T>
where
    T: Borrow<PooledConnection<'a>>,
{
    pub fn block_processed(&self, id: u64, chain_id: u8) -> Result<()> {
        self.conn().execute_in_place(
            "UPDATE eth_last_block SET block_number=?1 WHERE chain_id=?2",
            rusqlite::params![id, chain_id],
        )?;
        Ok(())
    }

    pub fn get_last_block_id(&self, chain_id: u8) -> Result<u64> {
        let block_number: u64 = self.conn().query_row_in_place(
            "SELECT block_number FROM eth_last_block WHERE chain_id = ?1",
            rusqlite::params![chain_id],
            |row| row.get(0),
        )?;
        Ok(block_number)
    }

    pub fn new_event(&self, event: StoredEthEvent) -> Result<Uuid> {
        let id = Uuid::new_v4();
        let encoded = bincode::serialize(&event).trust_me();

        self.conn().execute_in_place(
            "INSERT INTO eth_events (entry_id, event_data) VALUES (?1,?2)",
            rusqlite::params![id, encoded],
        )?;
        Ok(id)
    }

    pub fn get_event(&self, id: &Uuid) -> Result<StoredEthEvent> {
        let result: Vec<u8> = self.conn().query_row_in_place(
            "SELECT event_data FROM eth_events WHERE entry_id = ?1",
            rusqlite::params![id],
            |row| row.get(0),
        )?;
        Ok(bincode::deserialize(&result)?)
    }

    pub fn commit_event_processed(&self, id: &Uuid) -> Result<()> {
        self.conn().execute_in_place(
            "DELETE FROM eth_events WHERE entry_id = ?1",
            rusqlite::params![id],
        )?;
        Ok(())
    }

    pub fn get_all_events(&self) -> Result<Vec<(Uuid, StoredEthEvent)>> {
        fn process_row(row: rusqlite::Result<(Uuid, Vec<u8>)>) -> Option<(Uuid, StoredEthEvent)> {
            let (id, data) = match row {
                Ok(row) => row,
                Err(e) => {
                    log::error!("Failed fetching ETH event from db: {:?}", e);
                    return None;
                }
            };

            let event: StoredEthEvent = match bincode::deserialize(&data) {
                Ok(data) => data,
                Err(e) => {
                    log::error!("Failed decoding ETH event from db: {:?}", e);
                    return None;
                }
            };

            Some((id, event))
        }

        let mut stmt = self
            .conn()
            .prepare("SELECT entry_id, event_data FROM eth_events")?;
        let result = stmt
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
            .filter_map(process_row)
            .collect();
        Ok(result)
    }

    fn conn(&self) -> &PooledConnection<'a> {
        self.0.borrow()
    }
}
