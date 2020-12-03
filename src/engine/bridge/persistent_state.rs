use anyhow::Error;
use futures::Stream;
use futures::StreamExt;
use sled::{Db, Tree};
use tokio::sync::mpsc::UnboundedReceiver;

use relay_ton::contracts::{
    ContractWithEvents, EthereumEventConfigurationContract,
    EthereumEventConfigurationContractEvent, EthereumEventContract, EthereumEventDetails,
};
use relay_ton::prelude::Arc;
use relay_ton::transport::Transport;

const PERSISTENT_TREE_NAME: &str = "ton_data";

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

    pub async fn watch(&self, events: UnboundedReceiver<EthereumEventDetails>) {
        let mut events = events;
        while let Some(event) = events.next().await {
            println!("{:?}", event);
        }
    }
}
