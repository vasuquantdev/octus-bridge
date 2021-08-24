use std::str::FromStr;
use std::sync::Arc;

use anyhow::Result;
use futures::StreamExt;
use nekoton_utils::TrustMe;
use uuid::Uuid;

use super::db::{Pool, PoolExt, PooledConnection, PooledConnectionExt};
use super::models::StoredEthEvent;

#[derive(Clone)]
pub struct EthState {
    db: Arc<Pool>,
}

impl EthState {
    pub fn new(db: Arc<Pool>) -> Self {
        Self { db }
    }

    pub async fn block_processed(&self, id: u64, chain_id: u8) -> Result<()> {
        let conn = self.db.get_connection().await?;
        conn.execute_in_place(
            "UPDATE eth_last_block SET block_number=?1 WHERE chain_id=?2",
            rusqlite::params![id, chain_id],
        )?;

        Ok(())
    }

    pub async fn get_last_block_id(&self, chain_id: u8) -> Result<u64> {
        let conn = self.db.get_connection().await?;
        let block_number: u64 = conn.query_row_in_place(
            "SELECT block_number FROM eth_last_block WHERE chain_id = ?1",
            rusqlite::params![chain_id],
            |row| row.get(0),
        )?;

        Ok(block_number)
    }

    pub async fn new_event(&self, event: StoredEthEvent) -> Result<Uuid> {
        let id = Uuid::new_v4();
        let encoded = bincode::serialize(&event).trust_me();

        let conn = self.db.get_connection().await?;
        conn.execute_in_place(
            "INSERT INTO eth_events (entry_id, event_data) VALUES (?1,?2)",
            rusqlite::params![id, encoded],
        )?;

        Ok(id)
    }

    pub async fn get_event(&self, id: &Uuid) -> Result<StoredEthEvent> {
        let conn = self.db.get_connection().await?;
        let result: Vec<u8> = conn.query_row_in_place(
            "SELECT event_data FROM eth_events WHERE entry_id = ?1",
            rusqlite::params![id],
            |row| row.get(0),
        )?;

        Ok(bincode::deserialize(&result)?)
    }

    pub async fn commit_event_processed(&self, id: &Uuid) -> Result<()> {
        let conn = self.db.get_connection().await?;
        conn.execute_in_place(
            "DELETE FROM eth_events WHERE entry_id = ?1",
            rusqlite::params![id],
        )?;

        Ok(())
    }

    pub async fn get_all_events(&self) -> Result<Vec<(Uuid, StoredEthEvent)>> {
        let conn = self.db.get_connection().await?;
        let mut stmt = conn.prepare("SELECT entry_id, event_data FROM eth_events")?;
        let result = stmt
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
            .filter_map(|row: rusqlite::Result<(Uuid, Vec<u8>)>| {
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
            })
            .collect();
        Ok(result)
    }
}
