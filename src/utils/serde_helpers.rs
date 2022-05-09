pub mod serde_pubkey {
    use std::str::FromStr;

    use serde::de::Error;
    use serde::Deserialize;
    use solana_sdk::pubkey::Pubkey;

    pub fn serialize<S>(data: &Pubkey, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&data.to_string())
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Pubkey, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let data = String::deserialize(deserializer)?;
        Pubkey::from_str(&data).map_err(D::Error::custom)
    }
}

pub mod serde_vec_pubkey {
    use std::str::FromStr;

    use serde::de::{Error, SeqAccess, Visitor};
    use serde::ser::SerializeSeq;
    use solana_sdk::pubkey::Pubkey;

    pub fn serialize<S>(data: &[Pubkey], serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut seq = serializer.serialize_seq(Some(data.len()))?;
        for item in data {
            seq.serialize_element(&item.to_string())?;
        }
        seq.end()
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<Pubkey>, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct VecVisitor;

        impl<'de> Visitor<'de> for VecVisitor {
            type Value = Vec<Pubkey>;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("vector of solana public keys")
            }

            fn visit_seq<V>(self, mut visitor: V) -> Result<Self::Value, V::Error>
            where
                V: SeqAccess<'de>,
            {
                let mut vec = Vec::new();
                while let Some(elem) = visitor.next_element::<String>()? {
                    let item = Pubkey::from_str(&elem)
                        .map_err(|_| V::Error::custom("Invalid solana public key"))?;
                    vec.push(item);
                }
                Ok(vec)
            }
        }

        deserializer.deserialize_seq(VecVisitor)
    }
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
