use serde::{Deserialize, Serialize};
use solana_sdk::commitment_config::CommitmentConfig;

use crate::utils::*;

/// Solana network settings
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SolConfig {
    /// RPC endpoint
    pub endpoint: String,

    /// Connection timeout. Default: 60
    #[serde(default = "default_connection_timeout_sec")]
    pub connection_timeout_sec: u64,

    /// Commitment level
    #[serde(with = "serde_commitment")]
    pub commitment: CommitmentConfig,

    /// Max simultaneous connection count. Default: 10
    #[serde(default = "default_pool_size")]
    pub pool_size: usize,

    /// Blocks polling interval. Default: 60
    #[serde(default = "default_poll_interval_sec")]
    pub poll_interval_sec: u64,

    /// Max request duration (including all failed retires). Default: 604800
    #[serde(default = "default_maximum_failed_responses_time_sec")]
    pub maximum_failed_responses_time_sec: u64,

    /// Skip invalid TON->SOL event is Solana. Default: false
    #[serde(default)]
    pub clear_invalid_events: bool,
}

fn default_connection_timeout_sec() -> u64 {
    60
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
