use std::time::Duration;

use serde::{Deserialize, Serialize};
use url::Url;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EthConfig {
    pub endpoint: Url,
    pub get_timeout: Duration,
    pub pool_size: usize,
    pub poll_interval: Duration,
    pub maximum_failed_responses_time: Duration,
}
