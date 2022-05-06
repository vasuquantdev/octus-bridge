use serde::{Deserialize, Serialize};
use solana_sdk::commitment_config::CommitmentConfig;

use crate::utils::*;

/// Solana network settings
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SolConfig {
    /// RPC endpoint
    pub endpoint: String,

    /// Commitment level
    #[serde(with = "serde_commitment")]
    pub commitment: CommitmentConfig,

    /// Timeout, used for simple getter requests. Default: 10
    #[serde(default = "default_get_timeout_sec")]
    pub get_timeout_sec: u64,

    /// Max request duration (including all failed retires). Default: 604800
    #[serde(default = "default_maximum_failed_responses_time_sec")]
    pub maximum_failed_responses_time_sec: u64,
}

fn default_get_timeout_sec() -> u64 {
    10
}

fn default_maximum_failed_responses_time_sec() -> u64 {
    604800
}
