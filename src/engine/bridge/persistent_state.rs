use anyhow::Error;
use futures::Stream;
use futures::StreamExt;
use sled::{Db, Tree};
use tokio::sync::mpsc::UnboundedReceiver;
use bincode::serialize;

use relay_ton::contracts::{
    ContractWithEvents, EthereumEventConfigurationContract,
    EthereumEventConfigurationContractEvent, EthereumEventContract, EthereumEventDetails,
};
use relay_ton::prelude::Arc;
use relay_ton::transport::Transport;
use crate::engine::bridge::ton_config_listener::ExtendedEventInfo;

const PERSISTENT_TREE_NAME: &str = "unconfirmed_transactions";

pub struct TonWatcher {
    db: Tree,
    contract_configuration: Arc<EthereumEventConfigurationContract>,
    transport: Arc<dyn Transport>,
}

impl TonWatcher {
    pub fn new(
        db: Db,
        contract_configuration: Arc<EthereumEventConfigurationContract>,
        transport: Arc<dyn Transport>,
    ) -> Result<Self, Error> {
        Ok(Self {
            db: db.open_tree(PERSISTENT_TREE_NAME)?,
            contract_configuration,
            transport,
        })
    }

    pub async fn watch(&self, events: UnboundedReceiver<ExtendedEventInfo>) {
        let db = &self.db;
        let mut events = events;
        while let Some(event) = events.next().await {
            let addr = &event.address;
                let tx_hash = &event.data.ethereum_event_transaction;
                db.insert(tx_hash, serialize(&event).expect("Shouldn't fail"));
        }
    }
}
