use std::borrow::BorrowMut;

use anyhow::Result;
use nekoton_utils::TrustMe;
use tiny_adnl::utils::FxHashMap;
use uuid::Uuid;

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

    pub fn get_last_block_numbers(&self) -> Result<FxHashMap<u32, u64>> {
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
        let mut tx = self.0.borrow_mut().transaction()?;

        let mut stmt =
            tx.prepare("INSERT INTO eth_events (entry_id, event_data) VALUES (?1,?2)")?;

        for event in events {
            let id = Uuid::new_v4();
            let encoded = bincode::serialize(&event).trust_me();
            stmt.execute(rusqlite::params![id, encoded])?;
        }

        stmt.finalize()?;

        tx.execute(
            "UPDATE eth_last_block SET block_number=?1 WHERE chain_id=?2",
            rusqlite::params![last_block_number, chain_id],
        )?;

        tokio::task::block_in_place(|| tx.commit())?;

        Ok(())
    }

    pub fn get_event(&self, id: &Uuid) -> Result<StoredEthEvent> {
        let result: Vec<u8> = self.conn().query_row_in_place(
            "SELECT event_data FROM eth_events WHERE entry_id = ?1",
            rusqlite::params![id],
            |row| row.get(0),
        )?;
        Ok(bincode::deserialize(&result)?)
    }

    pub fn remove_event(&self, id: &Uuid) -> Result<()> {
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
