use std::sync::Arc;

use tokio::sync::mpsc;

pub struct TonSubscriber {
    _external_messages_tx: ExternalMessagesTx,
}

impl TonSubscriber {
    pub fn new(external_messages_tx: ExternalMessagesTx) -> Arc<Self> {
        Arc::new(Self {
            _external_messages_tx: external_messages_tx,
        })
    }
}

#[async_trait::async_trait]
impl ton_indexer::Subscriber for TonSubscriber {}

pub type ExternalMessagesTx = mpsc::Sender<ton_block::Message>;
pub type ExternalMessagesRx = mpsc::Receiver<ton_block::Message>;
