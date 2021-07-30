use std::sync::Arc;

use anyhow::Result;
use nekoton_utils::NoFailure;
use tokio::sync::mpsc;
use ton_block::Serializable;

use self::ton_subscriber::*;
use crate::config::*;

mod ton_subscriber;

pub struct Engine {
    ton_engine: Arc<ton_indexer::Engine>,
    _ton_subscriber: Arc<TonSubscriber>,
}

impl Engine {
    pub async fn new(
        config: RelayConfig,
        global_config: ton_indexer::GlobalConfig,
    ) -> Result<Arc<Self>> {
        let (external_messages_tx, external_messages_rx) = mpsc::channel(10);
        let ton_subscriber = TonSubscriber::new(external_messages_tx);

        let ton_engine = ton_indexer::Engine::new(
            config.indexer,
            global_config,
            vec![ton_subscriber.clone() as Arc<dyn ton_indexer::Subscriber>],
        )
        .await?;

        let engine = Arc::new(Self {
            ton_engine,
            _ton_subscriber: ton_subscriber,
        });

        engine.start_message_sender(external_messages_rx);

        Ok(engine)
    }

    pub async fn start(&self) -> Result<()> {
        self.ton_engine.start().await?;
        // TODO: start eth indexer

        Ok(())
    }

    fn start_message_sender(self: &Arc<Self>, mut external_messages_rx: ExternalMessagesRx) {
        let engine = Arc::downgrade(self);

        tokio::spawn(async move {
            while let Some(message) = external_messages_rx.recv().await {
                let engine = match engine.upgrade() {
                    Some(engine) => engine,
                    None => break,
                };

                if let Err(e) = engine.send_ton_message(&message).await {
                    log::error!("Failed to send external message: {:?}", e);
                }
            }

            external_messages_rx.close();
            while external_messages_rx.recv().await.is_some() {}
        });
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
