use std::convert::TryFrom;

use anyhow::Result;
use dashmap::DashSet;

use crate::utils::retry;
use eth_config::EthConfig;
use std::sync::Arc;
use tokio::sync::Semaphore;
use tokio::time::timeout;
use web3::transports::Http;
use web3::Web3;

mod eth_config;
mod models;
mod utils;

pub struct EthSubscriberRegistry {
    subscribed: DashSet<u16>,
}

impl EthSubscriberRegistry {
    async fn new_subscriber(
        &self,
        config: EthConfig,
        chain_id: u16,
    ) -> Option<Result<EthSubscriber>> {
        if self.subscribed.insert(chain_id) {
            return Some(EthSubscriber::new(config, chain_id).await);
        }
        None
    }
}

#[derive(Clone)]
pub struct EthSubscriber {
    config: EthConfig,
    id: u16,
    web3: Web3<Http>,
    pool: Arc<Semaphore>,
}

impl EthSubscriber {
    async fn new(config: EthConfig, id: u16) -> Result<Self> {
        let transport = web3::transports::Http::new(config.endpoint.as_str())?;
        let web3 = web3::Web3::new(transport);
        let pool = Arc::new(Semaphore::new(config.pool_size));
        Ok(Self {
            config,
            id,
            web3,
            pool,
        })
    }
    pub async fn subscribe(self) {}

    async fn get_current_height(&self) -> Option<u64> {
        match timeout(self.config.get_timeout, self.web3.eth().block_number()).await {
            Ok(a) => match a {
                Ok(a) => {
                    let height = a.as_u64();
                    log::debug!("Got height: {}", height);
                    Some(height)
                }
                Err(e) => {
                    if let web3::error::Error::Transport(e) = &e {
                        if e == "hyper::Error(IncompleteMessage)" {
                            log::debug!("Failed getting height: {}", e);
                            return None;
                        }
                    }
                    log::error!("Failed getting block number: {:?}", e);
                    None
                }
            },
            Err(e) => {
                log::error!("Timed out on getting actual eth block number: {:?}", e);
                None
            }
        }
    }
}
