use anyhow::Error;
use futures::StreamExt;
use relay_ton::prelude::{Arc, BigUint};
use sled::{Db, Tree};
use tokio::sync::mpsc::UnboundedReceiver;

use crate::engine::bridge::ton_config_listener::ExtendedEventInfo;

pub const PERSISTENT_TREE_NAME: &str = "unconfirmed_events";

pub struct TonWatcher {
    db: Tree,
}

impl TonWatcher {
    pub fn new(db: Db) -> Result<Self, Error> {
        Ok(Self {
            db: db.open_tree(PERSISTENT_TREE_NAME)?,
        })
    }

    pub async fn watch(self: Arc<Self>, mut events_rx: UnboundedReceiver<ExtendedEventInfo>) {
        log::info!("Started watching other relay events");

        let db = &self.db;
        while let Some(event) = events_rx.next().await {
            log::warn!("Recieved event");
            let tx_hash = &event.data.ethereum_event_transaction;
            db.insert(tx_hash, bincode::serialize(&event).expect("Shouldn't fail"))
                .unwrap();
        }
    }

    pub fn remove_event_by_hash(&self, key: &[u8]) -> Result<(), Error> {
        self.db.remove(key)?;
        Ok(())
    }

    pub fn get_event_by_hash(&self, hash: &[u8]) -> Result<Option<ExtendedEventInfo>, Error> {
        Ok(self
            .db
            .get(hash)?
            .and_then(|x| bincode::deserialize(&x).expect("Shouldn't fail")))
    }

    /// Get all blocks before specified block number
    pub fn scan_for_block_lower_bound(
        tree: &Tree,
        block_number: BigUint,
    ) -> Vec<ExtendedEventInfo> {
        tree.iter()
            .values()
            .filter_map(|x| match x {
                Ok(a) => Some(a),
                Err(e) => {
                    log::error!("Bad value in {}: {}", PERSISTENT_TREE_NAME, e);
                    None
                }
            })
            .map(|x| bincode::deserialize::<ExtendedEventInfo>(&x))
            .filter_map(|x| x.ok()) // shouldn't fail
            .filter(|x| x.data.event_block_number == block_number)
            .collect()
    }
}
