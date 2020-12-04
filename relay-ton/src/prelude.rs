pub use std::borrow::Cow;
pub use std::collections::HashMap;
pub use std::convert::{TryFrom, TryInto};
pub use std::io::Cursor;
pub use std::pin::Pin;
pub use std::str::FromStr;
pub use std::sync::Arc;

pub use async_trait::async_trait;
pub use chrono::Utc;
pub use ed25519_dalek::Keypair;
pub use futures::future::{BoxFuture, Future, FutureExt};
pub use futures::stream::{BoxStream, Stream, StreamExt};
pub use num_bigint::{BigInt, BigUint};
pub use once_cell::sync::OnceCell;
pub use serde::{Deserialize, Serialize};
pub use sled::Db;
pub use tokio::sync::{mpsc, oneshot, watch, RwLock};
pub use ton_abi::{Contract as AbiContract, Event as AbiEvent, Function as AbiFunction};
pub use ton_block::{MsgAddrStd, MsgAddressInt};
pub use ton_types::{BuilderData, SliceData, UInt256};

pub mod serde_int_addr {
    use super::*;
    use ton_block::{Deserializable, Serializable};

    pub fn serialize<S>(data: &ton_block::MsgAddressInt, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::Error;

        let data = data
            .write_to_bytes()
            .map_err(|e| S::Error::custom(e.to_string()))?;
        serializer.serialize_bytes(&data)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<ton_block::MsgAddressInt, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::Error;

        let data = Vec::<u8>::deserialize(deserializer)?;
        ton_block::MsgAddressInt::construct_from_bytes(&data)
            .map_err(|e| D::Error::custom(e.to_string()))
    }
}

pub mod serde_std_addr {
    use super::*;
    use ton_block::{Deserializable, Serializable};

    pub fn serialize<S>(data: &ton_block::MsgAddrStd, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::Error;

        let data = data
            .write_to_bytes()
            .map_err(|e| S::Error::custom(e.to_string()))?;
        serializer.serialize_bytes(&data)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<ton_block::MsgAddrStd, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::Error;

        let data = Vec::<u8>::deserialize(deserializer)?;
        ton_block::MsgAddrStd::construct_from_bytes(&data)
            .map_err(|e| D::Error::custom(e.to_string()))
    }
}

pub mod serde_cells {
    use super::*;
    use ton_block::{Deserializable, Serializable};

    pub fn serialize<S>(cell: &ton_types::Cell, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::Error;

        let data = cell
            .write_to_bytes()
            .map_err(|e| S::Error::custom(e.to_string()))?;
        serializer.serialize_bytes(&data)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<ton_types::Cell, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::Error;

        let data = Vec::<u8>::deserialize(deserializer)?;
        ton_types::Cell::construct_from_bytes(&data).map_err(|e| D::Error::custom(e.to_string()))
    }
}

pub mod serde_uint256 {
    use super::*;

    pub fn serialize<S>(data: &ton_types::UInt256, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_bytes(data.as_ref())
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<ton_types::UInt256, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Vec::<u8>::deserialize(deserializer).map(ton_types::UInt256::from)
    }
}

pub mod serde_vec_uint256 {
    use super::*;
    use serde::ser::SerializeSeq;

    pub fn serialize<S>(data: &[ton_types::UInt256], serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        #[derive(Serialize)]
        #[serde(transparent)]
        struct Helper<'a>(
            #[serde(serialize_with = "super::serde_uint256::serialize")] &'a ton_types::UInt256,
        );

        let mut seq = serializer.serialize_seq(Some(data.len()))?;
        for item in data {
            seq.serialize_element(&Helper(item))?;
        }
        seq.end()
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<ton_types::UInt256>, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(transparent)]
        struct Helper(
            #[serde(deserialize_with = "super::serde_uint256::deserialize")] ton_types::UInt256,
        );
        let data = Vec::<Helper>::deserialize(deserializer)?;
        Ok(data.into_iter().map(|helper| helper.0).collect())
    }
}
