use serde::{Deserialize, Serialize};
use url::Url;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EthConfig {
    pub chain_id: u32,
    pub endpoint: Url,
    pub get_timeout_sec: u64,
    pub pool_size: usize,
    pub poll_interval_sec: u64,
    pub maximum_failed_responses_time_sec: u64,
}
