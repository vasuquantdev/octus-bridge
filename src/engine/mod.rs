use std::sync::Arc;

use anyhow::Result;
use nekoton_utils::NoFailure;
use ton_block::Serializable;
use ton_types::UInt256;

use self::ton_subscriber::*;
use crate::config::*;

mod ton_subscriber;

pub struct Engine {
    bridge_account: UInt256,
    ton_engine: Arc<ton_indexer::Engine>,
    ton_subscriber: Arc<TonSubscriber>,
}

impl Engine {
    pub async fn new(
        config: RelayConfig,
        global_config: ton_indexer::GlobalConfig,
    ) -> Result<Arc<Self>> {
        let bridge_account =
            UInt256::from_be_bytes(&config.relay_address.address().get_bytestring(0));

        let ton_subscriber = TonSubscriber::new();

        let ton_engine = ton_indexer::Engine::new(
            config.indexer,
            global_config,
            vec![ton_subscriber.clone() as Arc<dyn ton_indexer::Subscriber>],
        )
        .await?;

        Ok(Arc::new(Self {
            bridge_account,
            ton_engine,
            ton_subscriber,
        }))
    }

    pub async fn start(&self) -> Result<()> {
        self.ton_engine.start().await?;
        self.ton_subscriber.start().await?;

        // TEMP:

        log::info!("REQUESTING SHARD ACCOUNT");
        let account = self
            .ton_subscriber
            .get_contract_state(self.bridge_account)
            .await?;
        log::info!("SHARD ACCOUNT: {:?}", account);

        // TODO: start eth indexer

        Ok(())
    }

    async fn send_ton_message(&self, message: &ton_block::Message) -> Result<()> {
        let to = match message.header() {
            ton_block::CommonMsgInfo::ExtInMsgInfo(header) => {
                ton_block::AccountIdPrefixFull::prefix(&header.dst).convert()?
            }
            _ => return Err(EngineError::ExternalTonMessageExpected.into()),
        };

        let cells = message.write_to_new_cell().convert()?.into();
        let serialized = ton_types::serialize_toc(&cells).convert()?;

        self.ton_engine
            .broadcast_external_message(&to, &serialized)
            .await
    }
}

#[derive(thiserror::Error, Debug)]
enum EngineError {
    #[error("External ton message expected")]
    ExternalTonMessageExpected,
}
