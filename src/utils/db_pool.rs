use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use bb8::ManageConnection;
use rusqlite::Params;

pub type Pool = bb8::Pool<ConnectionManager>;
pub type PooledConnection<'a> = bb8::PooledConnection<'a, ConnectionManager>;

#[async_trait::async_trait]
pub trait PoolExt {
    async fn get_connection(&'_ self) -> Result<PooledConnection<'_>>;
}

#[async_trait::async_trait]
impl PoolExt for Pool {
    async fn get_connection(&'_ self) -> Result<PooledConnection<'_>> {
        match self.get().await {
            Ok(connection) => Ok(connection),
            Err(bb8::RunError::User(e)) => Err(e),
            Err(bb8::RunError::TimedOut) => anyhow::bail!("DB connection timeout"),
        }
    }
}

pub trait PooledConnectionExt {
    fn query_row_in_place<T, P, F>(&self, sql: &str, params: P, f: F) -> Result<T>
    where
        P: rusqlite::Params,
        F: FnOnce(&rusqlite::Row<'_>) -> rusqlite::Result<T>;

    fn execute_in_place<P>(&self, sql: &str, params: P) -> Result<usize>
    where
        P: rusqlite::Params;
}

impl PooledConnectionExt for PooledConnection<'_> {
    fn query_row_in_place<T, P, F>(&self, sql: &str, params: P, f: F) -> Result<T>
    where
        P: rusqlite::Params,
        F: FnOnce(&rusqlite::Row<'_>) -> rusqlite::Result<T>,
    {
        tokio::task::block_in_place(|| self.query_row(sql, params, f)).map_err(anyhow::Error::from)
    }

    fn execute_in_place<P>(&self, sql: &str, params: P) -> Result<usize>
    where
        P: Params,
    {
        tokio::task::block_in_place(|| self.execute(sql, params)).map_err(anyhow::Error::from)
    }
}

pub struct ConnectionManager {
    options: Arc<ConnectionOptions>,
}

impl ConnectionManager {
    pub fn new(options: ConnectionOptions) -> Self {
        Self {
            options: Arc::new(options),
        }
    }
}

#[async_trait::async_trait]
impl ManageConnection for ConnectionManager {
    type Connection = rusqlite::Connection;
    type Error = anyhow::Error;

    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        let options = self.options.clone();

        let result = tokio::task::spawn_blocking(move || {
            rusqlite::Connection::open_with_flags(&options.path, options.flags)
        })
        .await?;

        result.map_err(anyhow::Error::from)
    }

    async fn is_valid(
        &self,
        conn: &mut bb8::PooledConnection<'_, Self>,
    ) -> Result<(), Self::Error> {
        tokio::task::block_in_place(|| conn.execute("SELECT 1", []))?;
        Ok(())
    }

    fn has_broken(&self, _: &mut Self::Connection) -> bool {
        false
    }
}

pub struct ConnectionOptions {
    pub path: PathBuf,
    pub flags: rusqlite::OpenFlags,
}
