use std::hash::{Hash, Hasher};

use ethereum_types::H160;
use ton_block::MsgAddressInt;

use relay_ton::prelude::{serde_cells, serde_int_addr};
use relay_ton::prelude::{BigUint, Cell};

use super::prelude::*;

#[derive(Deserialize, Serialize, Debug)]
pub struct EthTonConfirmationData {
    pub event_transaction: Vec<u8>,
    pub event_index: BigUint,
    #[serde(with = "serde_cells")]
    pub event_data: Cell,
    pub event_block_number: BigUint,
    pub event_block: Vec<u8>,
    #[serde(with = "serde_int_addr")]
    pub ethereum_event_configuration_address: MsgAddressInt,
}

impl Hash for EthTonConfirmationData {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.event_transaction.hash(state)
    }
}

impl PartialEq for EthTonConfirmationData {
    fn eq(&self, other: &Self) -> bool {
        self.event_transaction.eq(&other.event_transaction)
    }
}

impl Eq for EthTonConfirmationData {}

#[derive(Deserialize, Serialize)]
pub struct Stats {
    pub times_voted: u64,
    pub eth_ton_delivered_transactions: Vec<H160>,
}
