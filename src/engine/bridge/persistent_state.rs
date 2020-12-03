use anyhow::Error;
use futures::StreamExt;
use sled::{Db, Tree};
use futures::Stream;
use relay_ton::contracts::{
    ContractWithEvents, EthereumEventConfigurationContract, EthereumEventConfigurationContractEvent,
};
use relay_ton::prelude::Arc;

const PERSISTENT_TREE_NAME: &str = "ton_data";

pub struct TonWatcher {
    db: Tree,
    contract_configuration: Arc<EthereumEventConfigurationContract>,
}

impl TonWatcher {
    pub fn new(
        db: Db,
        contract_configuration: Arc<EthereumEventConfigurationContract>,
    ) -> Result<Self, Error> {
        Ok(Self {
            db: db.open_tree(PERSISTENT_TREE_NAME)?,
            contract_configuration,
        })
    }

    pub async fn watch(&self) {
        let mut events = self.contract_configuration.events();
        while let Some(event) = events.next().await {
            match event {
                EthereumEventConfigurationContractEvent::NewEthereumEventConfirmation {
                    address,
                    relay_key,
                } => {

                }
            }
        }
    }
}
