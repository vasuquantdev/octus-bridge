use serde::{Deserialize, Serialize};
use url::Url;

/// EVM network settings
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EthConfig {
    /// Chain ID of EVM network
    pub chain_id: u32,

    /// RPC endpoint
    pub endpoint: Url,

    /// Timeout, used for simple getter requests. Default: 10
    #[serde(default = "default_get_timeout_sec")]
    pub get_timeout_sec: u64,

    /// Max simultaneous connection count. Default: 10
    #[serde(default = "default_pool_size")]
    pub pool_size: usize,

    /// Blocks polling interval. Default: 60
    #[serde(default = "default_poll_interval_sec")]
    pub poll_interval_sec: u64,

    /// Max request duration (including all failed retires). Default: 604800
    #[serde(default = "default_maximum_failed_responses_time_sec")]
    pub maximum_failed_responses_time_sec: u64,
}

fn default_get_timeout_sec() -> u64 {
    10
}

fn default_pool_size() -> usize {
    10
}

fn default_poll_interval_sec() -> u64 {
    60
}

fn default_maximum_failed_responses_time_sec() -> u64 {
    604800
}
