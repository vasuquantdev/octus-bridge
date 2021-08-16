pub mod eth_state;
use anyhow::Result;
use sqlx::SqlitePool;

#[derive(Clone)]
pub struct State {
    pool: SqlitePool,
}

impl State {
    pub fn new(url: &str) -> Result<Self> {
        let pool = SqlitePool::connect_lazy(url)?;
        Ok(Self { pool })
    }
}
