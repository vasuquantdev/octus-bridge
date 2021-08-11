use crate::engine::eth_subscriber::models::Event;
use anyhow::Result;
use futures::StreamExt;
use nekoton_utils::TrustMe;
use std::str::FromStr;
use uuid::Uuid;

#[derive(Clone)]
pub struct EthState {
    state: super::State,
}

impl EthState {
    pub fn new(state: super::State) -> Self {
        Self { state }
    }

    pub async fn block_processed(&self, id: u64, chain_id: u8) -> Result<()> {
        let id = id as i64;
        let chain_id = chain_id as i8;
        sqlx::query!(
            "UPDATE ETH_LAST_BLOCK SET block_number=? WHERE chain_id=?;",
            id,
            chain_id
        )
        .execute(&self.state.pool)
        .await?;
        Ok(())
    }

    pub async fn get_last_block_id(&self, chain_id: u8) -> Result<u64> {
        let num = sqlx::query!(
            "SELECT block_number FROM ETH_LAST_BLOCK WHERE chain_id = ?",
            chain_id
        )
        .fetch_one(&self.state.pool)
        .await?
        .block_number;
        Ok(num as u64)
    }

    pub async fn new_event(&self, event: Event) -> Result<Uuid> {
        let encoded = bincode::serialize(&event).trust_me();
        let id = Uuid::new_v4();
        sqlx::query!(
            "INSERT INTO eth_events (entry_id, event_data)\
            VALUES (?,?)",
            id,
            encoded
        )
        .execute(&self.state.pool)
        .await?;
        Ok(id)
    }

    pub async fn get_event(&self, id: &Uuid) -> Result<Event> {
        let res: Vec<u8> = sqlx::query!("select event_data from eth_events where entry_id = ?", id)
            .fetch_one(&self.state.pool)
            .await?
            .event_data;
        Ok(bincode::deserialize(res.as_slice())?)
    }

    pub async fn commit_event_processed(&self, id: &Uuid) -> Result<()> {
        sqlx::query!(
            "DELETE FROM eth_events
                       WHERE entry_id = ?",
            id
        )
        .execute(&self.state.pool)
        .await?;
        Ok(())
    }

    pub async fn get_all_events(&self) -> Result<Vec<(Uuid, Event)>> {
        let res: Vec<(Uuid, Event)> = sqlx::query!("select entry_id,event_data from eth_events")
            .fetch(&self.state.pool)
            .filter_map(|x| async move { x.ok() })
            .map(|x| (x.entry_id, x.event_data))
            .filter_map(|(id, data)| async move {
                let id = match Uuid::from_str(&id) {
                    Ok(a) => a,
                    Err(e) => {
                        log::error!("Bad uuid from event table: {}", e);
                        return None;
                    }
                };
                let event: Event = match bincode::deserialize(&data) {
                    Ok(a) => a,
                    Err(e) => {
                        log::error!("Failed decoding eth event from db: {}", e);
                        return None;
                    }
                };
                Some((id, event))
            })
            .collect()
            .await;
        Ok(res)
    }
}
