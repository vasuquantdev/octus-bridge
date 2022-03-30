use serde::{Deserialize, Serialize};
use solana_sdk::commitment_config::CommitmentConfig;
use url::Url;

/// Solana network settings
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SolConfig {
    /// RPC endpoint
    pub endpoint: Url,

    /// Commitment level
    pub commitment: CommitmentConfig,
}
