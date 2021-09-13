use serde::{Deserialize, Serialize};
use url::Url;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EthConfig {
    pub chain_id: u32,
    #[serde(with = "serde_eth_address")]
    pub staker_address: ethabi::Address,
    #[serde(with = "serde_eth_address")]
    pub verifier_address: ethabi::Address,
    pub endpoint: Url,
    pub get_timeout_sec: u64,
    pub pool_size: usize,
    pub poll_interval_sec: u64,
    pub maximum_failed_responses_time_sec: u64,
}

pub mod serde_eth_address {
    use super::*;
    use serde::de::Error;

    pub fn serialize<S>(data: &ethabi::Address, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&hex::encode(&data.as_ref()))
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<ethabi::Address, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let data = String::deserialize(deserializer)?;
        let data = data.strip_prefix("0x").unwrap_or_else(|| data.as_str());
        let data = hex::decode(data).map_err(D::Error::custom)?;
        if data.len() != 160 / 8 {
            return Err(D::Error::custom(format!(
                "Expected len 20. Found: {}",
                data.len()
            )));
        }
        Ok(ethabi::Address::from_slice(data.as_slice()))
    }
}
