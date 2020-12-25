use ton_block::MsgAddressInt;

use relay_eth::ws::H256;
use relay_models::models::{EthTonConfirmationDataView, EthTonTransactionView};
use relay_ton::prelude::{serde_cells, serde_int_addr, serialize_toc, Cell};

use super::prelude::*;

pub mod buf_to_hex {
    use serde::{Deserialize, Deserializer, Serializer};

    /// Serializes `buffer` to a lowercase hex string.
    pub fn serialize<T, S>(buffer: &T, serializer: S) -> Result<S::Ok, S::Error>
    where
        T: AsRef<[u8]> + ?Sized,
        S: Serializer,
    {
        serializer.serialize_str(&*hex::encode(&buffer.as_ref()))
    }

    /// Deserializes a lowercase hex string to a `Vec<u8>`.
    pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
    where
        D: Deserializer<'de>,
    {
        use serde::de::Error;
        String::deserialize(deserializer)
            .and_then(|string| hex::decode(string).map_err(|e| D::Error::custom(e.to_string())))
    }
}

pub mod h256_to_hex {
    use ethereum_types::H256;
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<T, S>(buffer: &T, serializer: S) -> Result<S::Ok, S::Error>
    where
        T: AsRef<[u8]> + ?Sized,
        S: Serializer,
    {
        serializer.serialize_str(&*hex::encode(&buffer.as_ref()))
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<H256, D::Error>
    where
        D: Deserializer<'de>,
    {
        use serde::de::Error;

        String::deserialize(deserializer).and_then(|string| {
            hex::decode(string)
                .map_err(|e| D::Error::custom(e.to_string()))
                .map(|x| H256::from_slice(&*x))
        })
    }
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct EthTonConfirmationData {
    pub event_transaction: H256,
    pub event_index: u64,
    #[serde(with = "serde_cells")]
    pub event_data: Cell,
    pub event_block_number: u64,
    pub event_block: H256,
    #[serde(with = "serde_int_addr")]
    pub ethereum_event_configuration_address: MsgAddressInt,
}

#[derive(Serialize, Deserialize, Clone)]
pub enum EthTonTransaction {
    Confirm(EthTonConfirmationData),
    Reject(EthTonConfirmationData),
}

impl EthTonTransaction {
    pub fn inner(&self) -> &EthTonConfirmationData {
        match self {
            EthTonTransaction::Confirm(data) => data,
            EthTonTransaction::Reject(data) => data,
        }
    }
}

impl From<EthTonConfirmationData> for EthTonConfirmationDataView {
    fn from(data: EthTonConfirmationData) -> Self {
        let event_block = match serialize_toc(&data.event_data) {
            Ok(a) => hex::encode(a),
            Err(e) => {
                log::error!("Failed serializing boc: {}", e);
                "BAD DATA IN BLOCK".to_string()
            }
        };
        EthTonConfirmationDataView {
            event_transaction: hex::encode(&data.event_transaction.0),
            event_index: data.event_index,
            event_data: event_block,
            event_block_number: data.event_block_number,
            event_block: hex::encode(&data.event_block.0),
            ethereum_event_configuration_address: data
                .ethereum_event_configuration_address
                .to_string(),
        }
    }
}

impl From<EthTonTransaction> for EthTonTransactionView {
    fn from(data: EthTonTransaction) -> Self {
        match data {
            EthTonTransaction::Confirm(a) => EthTonTransactionView::Confirm(a.into()),
            EthTonTransaction::Reject(a) => EthTonTransactionView::Reject(a.into()),
        }
    }
}
