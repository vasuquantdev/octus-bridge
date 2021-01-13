use chrono::{DateTime, Utc};

use relay_eth::ws::H256;
use relay_models::models::{EthEventVotingDataView, EthTonTransactionView};
use relay_ton::contracts::{EthEventInitData, TonEventInitData, Voting};
use relay_ton::prelude::{serde_cells, serialize_toc, Cell};

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

#[derive(Deserialize, Serialize)]
pub struct StoredTxStat {
    pub tx_hash: H256,
    pub met: DateTime<Utc>,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct TonEventVotingData {
    pub event_transaction: H256,
    #[serde(with = "serde_cells")]
    pub event_data: Cell,
    pub event_block_number: u64,
    pub event_block: H256,

    pub configuration_id: BigUint,
}

impl From<TonEventVotingData> for (BigUint, TonEventInitData) {
    fn from(data: EthEventVotingData) -> Self {
        (
            data.configuration_id,
            TonEventInitData {
                event_transaction: data.event_transaction,
                event_index: Default::default(),
                event_data: data.event_data,
                event_block_number: data.event_block_number.into(),
                event_block: data.event_block,

                // TODO: replace voting data model
                ton_event_configuration: Default::default(),
                required_confirmations: Default::default(),
                required_rejections: Default::default(),
            },
        )
    }
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct EthEventVotingData {
    pub event_transaction: H256,
    pub event_index: u64,
    #[serde(with = "serde_cells")]
    pub event_data: Cell,
    pub event_block_number: u64,
    pub event_block: H256,

    pub configuration_id: BigUint,
}

impl From<EthEventVotingData> for (BigUint, EthEventInitData) {
    fn from(data: EthEventVotingData) -> Self {
        (
            data.configuration_id,
            EthEventInitData {
                event_transaction: data.event_transaction,
                event_index: data.event_index.into(),
                event_data: data.event_data,
                event_block_number: data.event_block_number.into(),
                event_block: data.event_block,

                // TODO: replace voting data model
                eth_event_configuration: Default::default(),
                required_confirmations: Default::default(),
                required_rejections: Default::default(),
                proxy_address: Default::default(),
            },
        )
    }
}

impl From<EthEventVotingData> for EthEventVotingDataView {
    fn from(data: EthEventVotingData) -> Self {
        let event_block = match serialize_toc(&data.event_data) {
            Ok(a) => hex::encode(a),
            Err(e) => {
                log::error!("Failed serializing boc: {}", e);
                "BAD DATA IN BLOCK".to_string()
            }
        };
        EthEventVotingDataView {
            event_transaction: hex::encode(&data.event_transaction.0),
            event_index: data.event_index,
            event_data: event_block,
            event_block_number: data.event_block_number,
            event_block: hex::encode(&data.event_block.0),
            configuration_id: data.configuration_id.to_string(),
        }
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub enum EventTransaction<C, R> {
    Confirm(C),
    Reject(R),
}

#[derive(Serialize, Deserialize, Clone)]
pub struct SignedEventVotingData {
    pub data: TonEventVotingData,
    pub signature: Vec<u8>,
}

pub type EthEventTransaction = EventTransaction<EthEventVotingData, EthEventVotingData>;
pub type TonEventTransaction = EventTransaction<SignedEventVotingData, TonEventVotingData>;

impl From<EthEventTransaction> for EthTonTransactionView {
    fn from(data: EthEventTransaction) -> Self {
        match data {
            EventTransaction::Confirm(a) => EthTonTransactionView::Confirm(a.into()),
            EventTransaction::Reject(a) => EthTonTransactionView::Reject(a.into()),
        }
    }
}
