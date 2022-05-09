use serde::{Deserialize, Serialize};
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::pubkey::Pubkey;

use crate::utils::*;

/// Solana network settings
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SolConfig {
    /// RPC endpoint
    pub endpoint: String,

    /// WS RPC endpoint
    pub ws_endpoint: String,

    /// Programs to subscribe
    #[serde(with = "serde_vec_pubkey")]
    pub programs: Vec<Pubkey>,

    /// Commitment level
    #[serde(with = "serde_commitment")]
    pub commitment: CommitmentConfig,

    /// Timeout, used for simple getter requests. Default: 10
    #[serde(default = "default_get_timeout_sec")]
    pub get_timeout_sec: u64,

    /// Blocks polling interval. Default: 60
    #[serde(default = "default_poll_interval_sec")]
    pub poll_interval_sec: u64,

    /// Timeout, used for simple getter requests. Default: 1
    #[serde(default = "default_reconnect_timeout_sec")]
    pub reconnect_timeout_sec: u64,

    /// Max request duration (including all failed retires). Default: 604800
    #[serde(default = "default_maximum_failed_responses_time_sec")]
    pub maximum_failed_responses_time_sec: u64,
}

fn default_get_timeout_sec() -> u64 {
    10
}

fn default_poll_interval_sec() -> u64 {
    60
}

fn default_reconnect_timeout_sec() -> u64 {
    1
}

fn default_maximum_failed_responses_time_sec() -> u64 {
    604800
}
