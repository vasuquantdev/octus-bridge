use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use bb8::ManageConnection;

use self::db::*;

pub mod db;
pub mod eth_state;
pub mod models;

#[derive(Clone)]
pub struct State {
    db: Arc<Pool>,
}

impl State {
    pub async fn new<P>(db_path: P) -> Result<Self>
    where
        P: AsRef<Path>,
    {
        let db = Arc::new(
            Pool::builder()
                .build(ConnectionManager::new(ConnectionOptions {
                    path: db_path.as_ref().into(),
                    flags: rusqlite::OpenFlags::SQLITE_OPEN_CREATE
                        | rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE,
                }))
                .await?,
        );

        Ok(Self { db })
    }

    pub async fn apply_migrations(&self) -> Result<()> {
        let mut connection = self.db.get_connection().await?;
        embedded::migrations::runner().run(&mut *connection)?;
        Ok(())
    }

    pub fn db(&self) -> &Arc<Pool> {
        &self.db
    }
}

mod embedded {
    refinery::embed_migrations!();
}
