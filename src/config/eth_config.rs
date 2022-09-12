use serde::{Deserialize, Serialize};
use url::Url;

use crate::utils::*;

/// EVM network settings
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EthConfig {
    /// Chain ID of EVM network
    pub chain_id: u32,

    /// RPC endpoint
    pub endpoint: Url,

    /// Timeout, used for simple getter requests. Default: 10
    #[serde(default = "const_u64::<10>")]
    pub get_timeout_sec: u64,

    #[serde(default = "const_u64::<120>")]
    pub blocks_processing_timeout_sec: u64,

    /// Max simultaneous connection count. Default: 10
    #[serde(default = "const_usize::<10>")]
    pub pool_size: usize,

    /// Blocks polling interval. Default: 60
    #[serde(default = "const_u64::<60>")]
    pub poll_interval_sec: u64,

    /// Max blocks range. Default: None
    #[serde(default)]
    pub max_block_range: Option<u64>,

    /// Max request duration (including all failed retires). Default: 604800
    #[serde(default = "const_u64::<604800>")]
    pub maximum_failed_responses_time_sec: u64,
}
