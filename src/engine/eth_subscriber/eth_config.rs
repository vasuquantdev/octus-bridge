use serde::{Deserialize, Serialize};
use std::time::Duration;
use url::Url;

#[derive(Serialize, Deserialize, Clone)]
pub struct EthConfig {
    pub endpoint: Url,
    pub get_timeout: Duration,
    pub pool_size: usize,
    pub poll_interval: Duration,
}
