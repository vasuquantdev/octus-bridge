mod eth_state;
use anyhow::Result;
use sqlx::sqlite::Sqlite;
use sqlx::SqlitePool;

#[derive(Clone)]
struct State {
    pool: SqlitePool,
}

impl State {
    fn new(url: &str) -> Result<Self> {
        let pool = SqlitePool::connect_lazy(url)?;
        Ok(Self { pool })
    }
}
