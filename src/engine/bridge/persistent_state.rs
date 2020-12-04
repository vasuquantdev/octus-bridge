use anyhow::Error;
use bincode::{deserialize, serialize};
use futures::StreamExt;
use sled::{Db, Tree};
use tokio::sync::mpsc::UnboundedReceiver;

use relay_ton::prelude::{Arc, BigUint};
use relay_ton::transport::Transport;

use crate::engine::bridge::ton_config_listener::ExtendedEventInfo;

const PERSISTENT_TREE_NAME: &str = "unconfirmed_events";

pub struct TonWatcher {
    db: Tree,
    transport: Arc<dyn Transport>,
}

impl TonWatcher {
    pub fn new(
        db: Db,
        transport: Arc<dyn Transport>,
    ) -> Result<Self, Error> {
        Ok(Self {
            db: db.open_tree(PERSISTENT_TREE_NAME)?,
            transport,
        })
    }

    pub async fn watch(self: Arc<Self>, events: UnboundedReceiver<ExtendedEventInfo>) {
        let db = &self.db;
        let mut events = events;
        while let Some(event) = events.next().await {
            let tx_hash = &event.data.ethereum_event_transaction;
            db.insert(tx_hash, serialize(&event).expect("Shouldn't fail")).unwrap();
        }
    }

    pub fn drop_event_by_hash(&self, key: &[u8]) -> Result<(), Error> {
        self.db.remove(key)?;
        Ok(())
    }

    pub fn get_event_by_hash(&self, hash: &[u8]) -> Result<Option<ExtendedEventInfo>, Error> {
        Ok(self
            .db
            .get(hash)?
            .and_then(|x| deserialize(&x).expect("Shouldn't fail")))
    }

    pub fn scan_for_block_lower_bound(tree: &Tree, block_number: BigUint) -> Vec<ExtendedEventInfo> {
        tree
            .iter()
            .values()
            .filter_map(|x| match x {
                Ok(a) => Some(a),
                Err(e) => {
                    log::error!("Bad value in {}: {}", PERSISTENT_TREE_NAME, e);
                    None
                }
            })
            .map(|x| deserialize::<ExtendedEventInfo>(&x))
            .filter_map(|x| x.ok()) // shouldn't fail
            .filter(|x| x.data.event_block_number == block_number)
            .collect()
    }
}
