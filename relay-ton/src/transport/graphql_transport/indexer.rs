use super::node_client::*;
use crate::prelude::*;
use crate::transport::errors::*;
use crate::transport::{AccountSubscription, AccountSubscriptionFull, RunLocal, Transport};

pub struct Indexer {
    node_client: Arc<NodeClient>,
}

impl Indexer {
    pub fn new(node_client: Arc<NodeClient>) -> Self {
        Self { node_client }
    }

    pub async fn start(self: &Arc<Self>) -> TransportResult<()> {
        let latest_masterchain_block = self.node_client.get_latest_masterchain_block().await?;
        let indexer = Arc::downgrade(self);

        tokio::spawn(async move {
            let indexer = match indexer.upgrade() {
                Some(indexer) => indexer,
                None => return,
            };
        });

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_indexer() {
        let node_client = Arc::new(NodeClient::new(
            "https://main.ton.dev/graphql".to_owned(),
            2,
            std::time::Duration::from_secs(60),
        ));
        let indexer = Indexer::new(node_client);
    }
}
