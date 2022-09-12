pub const fn const_usize<const N: usize>() -> usize {
    N
}

pub const fn const_u64<const N: u64>() -> u64 {
    N
}

pub mod serde_commitment {
    use std::str::FromStr;

    use serde::de::Error;
    use serde::Deserialize;
    use solana_sdk::commitment_config::CommitmentConfig;

    pub fn serialize<S>(data: &CommitmentConfig, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&data.commitment.to_string())
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<CommitmentConfig, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let data = String::deserialize(deserializer)?;
        CommitmentConfig::from_str(&data).map_err(D::Error::custom)
    }
}
