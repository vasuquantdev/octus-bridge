use super::node_client::*;
use crate::prelude::*;
use crate::transport::errors::*;

#[allow(dead_code)]
pub struct Indexer {
    node_client: Arc<NodeClient>,
}

impl Indexer {
    #[allow(dead_code)]
    pub fn new(node_client: Arc<NodeClient>) -> Self {
        Self { node_client }
    }

    #[allow(dead_code)]
    pub async fn start(self: &Arc<Self>) -> TransportResult<()> {
        let _latest_masterchain_block = self.node_client.get_latest_masterchain_block().await?;
        let indexer = Arc::downgrade(self);

        tokio::spawn(async move {
            let _indexer = match indexer.upgrade() {
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
