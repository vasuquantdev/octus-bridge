use crate::utils::retry;
use anyhow::Result;

#[derive(Clone)]
struct EthState {
    state: super::State,
}

impl EthState {
    fn new(state: super::State) -> Self {
        Self { state }
    }

    async fn block_processed(&self, id: u64) -> Result<()> {
        let id = id as i64;
        sqlx::query!("UPDATE ETH_LAST_BLOCK SET block_number=? WHERE _id=0;", id)
            .execute(&self.state.pool)
            .await?;
        Ok(())
    }

    async fn get_last_block_id(&self) -> Result<u64> {
        let num = sqlx::query!("SELECT block_number FROM ETH_LAST_BLOCK WHERE _id=0")
            .fetch_one(&self.state.pool)
            .await?
            .block_number;
        Ok(num as u64)
    }
}
