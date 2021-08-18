use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use bb8::ManageConnection;

pub use self::eth_state::*;
use crate::utils::*;

mod eth_state;

#[derive(Clone)]
pub struct State {
    db: Pool,
}

impl State {
    pub async fn new<P>(db_path: P) -> Result<Self>
    where
        P: AsRef<Path>,
    {
        let db = Pool::builder()
            .build(ConnectionManager::new(ConnectionOptions {
                path: db_path.as_ref().into(),
                flags: rusqlite::OpenFlags::SQLITE_OPEN_CREATE
                    | rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE,
            }))
            .await?;

        Ok(Self { db })
    }

    pub async fn apply_migrations(&self) -> Result<()> {
        let mut connection = self.db.get_connection().await?;
        embedded::migrations::runner().run(&mut *connection)?;
        Ok(())
    }

    pub async fn get_connection(&'_ self) -> Result<PooledConnection<'_>> {
        self.db.get_connection().await
    }
}

mod embedded {
    refinery::embed_migrations!();
}
