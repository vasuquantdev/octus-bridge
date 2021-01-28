#![allow(unused_imports)]

pub(crate) use std::borrow::Cow;
pub(crate) use std::collections::HashMap;
pub(crate) use std::convert::{TryFrom, TryInto};
pub(crate) use std::fmt::{LowerHex, UpperHex};
pub(crate) use std::io::Cursor;
pub(crate) use std::pin::Pin;
pub(crate) use std::str::FromStr;
pub(crate) use std::sync::Arc;

pub(crate) use async_trait::async_trait;
pub(crate) use chrono::Utc;
pub(crate) use futures::future::{BoxFuture, Future, FutureExt};
pub(crate) use futures::stream::{BoxStream, Stream, StreamExt};
pub(crate) use num_bigint::{BigInt, BigUint};
pub(crate) use once_cell::sync::OnceCell;
pub(crate) use serde::{Deserialize, Serialize};
pub(crate) use sled::Db;
pub(crate) use tokio::sync::{mpsc, oneshot, RwLock};

pub use ed25519_dalek::Keypair;
pub use ton_abi::{Contract as AbiContract, Event as AbiEvent, Function as AbiFunction};
pub use ton_block::{MsgAddrStd, MsgAddressInt};
pub use ton_types::{serialize_toc, BuilderData, Cell, SliceData, UInt256};

pub(crate) type RawEventsRx = EventsRx<SliceData>;
pub(crate) type FullEventsRx = EventsRx<FullEventInfo>;

pub type EventsTx<T> = mpsc::UnboundedSender<T>;
pub type EventsRx<T> = mpsc::UnboundedReceiver<T>;

pub struct FullEventInfo {
    pub event_transaction: UInt256,
    pub event_transaction_lt: u64,
    pub event_timestamp: u32,
    pub event_index: u32,
    pub event_data: SliceData,
}

#[allow(clippy::derive_hash_xor_eq)]
#[derive(Clone, Default, PartialEq, Eq, Hash, Ord, PartialOrd)]
pub struct UInt128([u8; 16]);

impl PartialEq<SliceData> for UInt128 {
    fn eq(&self, other: &SliceData) -> bool {
        if other.remaining_bits() == 128 {
            return self.0 == other.get_bytestring(0).as_slice();
        }
        false
    }
}

impl PartialEq<UInt128> for &UInt128 {
    fn eq(&self, other: &UInt128) -> bool {
        self.0 == other.0
    }
}

impl PartialEq<&UInt128> for UInt128 {
    fn eq(&self, other: &&UInt128) -> bool {
        self.0 == other.0
    }
}

impl UInt128 {
    pub fn is_zero(&self) -> bool {
        for b in &self.0 {
            if b != &0 {
                return false;
            }
        }
        true
    }

    pub fn as_slice(&self) -> &[u8; 16] {
        &self.0
    }

    pub fn to_hex_string(&self) -> String {
        hex::encode(self.0)
    }

    pub fn max() -> Self {
        UInt128([0xFF; 16])
    }

    pub const MIN: UInt128 = UInt128([0; 16]);
    pub const MAX: UInt128 = UInt128([0xFF; 16]);
}

impl FromStr for UInt128 {
    type Err = failure::Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.len() != 32 {
            ton_types::fail!("invalid account ID string length (32 expected)")
        } else {
            let bytes = hex::decode(value)?;
            Ok(UInt128::from(bytes))
        }
    }
}

impl From<[u8; 16]> for UInt128 {
    fn from(data: [u8; 16]) -> Self {
        UInt128(data)
    }
}

impl From<UInt128> for [u8; 16] {
    fn from(data: UInt128) -> Self {
        data.0
    }
}

impl<'a> From<&'a UInt128> for &'a [u8; 16] {
    fn from(data: &'a UInt128) -> Self {
        &data.0
    }
}

impl<'a> From<&'a [u8; 16]> for UInt128 {
    fn from(data: &[u8; 16]) -> Self {
        UInt128(*data)
    }
}

impl From<&[u8]> for UInt128 {
    fn from(value: &[u8]) -> Self {
        let mut data = [0; 16];
        let len = std::cmp::min(value.len(), 16);
        (0..len).for_each(|i| data[i] = value[i]);
        Self(data)
    }
}

impl From<Vec<u8>> for UInt128 {
    fn from(value: Vec<u8>) -> Self {
        UInt128::from(value.as_slice())
    }
}

impl std::fmt::Debug for UInt128 {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        LowerHex::fmt(self, f)
    }
}

impl std::fmt::Display for UInt128 {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "UInt128[{:X?}]", self.as_slice())
    }
}

impl LowerHex for UInt128 {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        if f.alternate() {
            write!(f, "0x")?;
        }
        write!(f, "{}", hex::encode(&self.0))
    }
}

impl UpperHex for UInt128 {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        if f.alternate() {
            write!(f, "0x")?;
        }
        write!(f, "{}", hex::encode_upper(&self.0))
    }
}

impl std::convert::AsRef<[u8]> for &UInt128 {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
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
