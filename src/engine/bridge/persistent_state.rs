use anyhow::Error;
use ethereum_types::H160;
use futures::StreamExt;
use sled::{Db};
use tokio::sync::mpsc::UnboundedReceiver;

use relay_ton::prelude::{Arc, UInt256};

use crate::db_managment::ton_db::TonTree;
use crate::engine::bridge::models::ExtendedEventInfo;




pub struct TonWatcher {
    db: TonTree,
    relay_key: UInt256,
}

impl TonWatcher {
    pub fn new(db: &Db, relay_key: UInt256) -> Result<Self, Error> {
        Ok(Self {
            db: TonTree::new(&db)?,
            relay_key,
        })
    }

    pub async fn watch(self: Arc<Self>, mut events_rx: UnboundedReceiver<ExtendedEventInfo>) {
        log::info!("Started watching other relay events");

        let db = &self.db;
        while let Some(event) = events_rx.next().await {
            log::info!("Received event");
            if event.relay_key == self.relay_key {
                log::info!(
                    "Met event for our transaction. Eth hash: {}",
                    hex::encode(&event.data.ethereum_event_transaction)
                );
                continue;
            }
            log::info!(
                "Received other relay event. Relay key: {}",
                hex::encode(&event.relay_key)
            );

            db.insert(
                H160::from_slice(&*event.data.ethereum_event_transaction),
                &event,
            )
            .unwrap();
        }
    }

    pub fn remove_event_by_hash(&self, key: &[u8]) -> Result<(), Error> {
        self.db.remove_event_by_hash(&H160::from_slice(key))?;
        Ok(())
    }

    pub fn get_event_by_hash(&self, hash: &[u8]) -> Result<Option<ExtendedEventInfo>, Error> {
        Ok(self.db.get_event_by_hash(&H160::from_slice(&hash))?)
    }
}
