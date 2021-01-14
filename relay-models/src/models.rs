use opg::OpgModel;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug, OpgModel)]
pub struct InitData {
    pub ton_seed: String,
    pub eth_seed: String,
    pub password: String,
    pub language: String,
}

#[derive(Deserialize, Debug, OpgModel, Serialize)]
pub struct Password {
    pub password: String,
}

#[derive(Deserialize, Debug, Serialize, OpgModel)]
pub struct RescanEthData {
    pub block: u64,
}

#[derive(Deserialize, Serialize, OpgModel)]
pub struct NewEventConfiguration {
    pub ethereum_event_abi: String,
    pub ethereum_event_address: String,
    pub ethereum_event_blocks_to_confirm: u64,
    pub required_confirmations: u64,
    pub required_rejections: u64,
    pub ethereum_event_initial_balance: u64,
    pub event_proxy_address: String,
}

#[derive(Deserialize, Serialize)]
pub struct EventConfiguration {
    pub configuration_id: String,
    pub ethereum_event_abi: String,
    pub ethereum_event_address: String,
    pub event_proxy_address: String,
    pub ethereum_event_blocks_to_confirm: u16,
    pub event_required_confirmations: u64,
    pub event_required_rejects: u64,
    pub event_initial_balance: u64,
    pub bridge_address: String,
    pub event_code: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, OpgModel)]
pub struct VotingAddress {
    pub address: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, OpgModel)]
#[serde(rename_all = "lowercase", tag = "vote", content = "address")]
pub enum Voting {
    Confirm(String),
    Reject(String),
}

#[derive(Serialize, Deserialize, OpgModel)]
pub struct Status {
    pub password_needed: bool,
    pub init_data_needed: bool,
    pub is_working: bool,
    pub ton_pubkey: Option<String>,
    pub eth_pubkey: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, opg::OpgModel)]
pub struct EthTonVoteView {
    pub event_address: String,
    #[serde(flatten)]
    pub transaction: EthTonTransactionView,
}

#[derive(Serialize, Deserialize, Clone, opg::OpgModel)]
#[serde(tag = "type")]
pub enum EthTonTransactionView {
    Confirm(EthEventVotingDataView),
    Reject(EthEventVotingDataView),
}

#[derive(Deserialize, Serialize, Debug, Clone, opg::OpgModel)]
pub struct EthEventVotingDataView {
    #[opg(format = "hex")]
    pub event_transaction: String,
    pub event_index: u64,
    #[opg(format = "hex")]
    pub event_data: String,
    pub event_block_number: u64,
    #[opg(format = "hex")]
    pub event_block: String,
    pub configuration_id: String,
}

#[derive(Deserialize, Serialize, opg::OpgModel)]
#[serde(rename_all = "lowercase")]
pub struct EthTxStatView {
    pub tx_hash: String,
    pub met: i64,
    pub event_addr: String,
    pub vote: EventVote,
}

#[derive(Deserialize, Serialize, opg::OpgModel)]
#[serde(rename_all = "lowercase")]
pub struct TonTxStatView {
    pub tx_hash: String,
    pub tx_lt: String,
    pub met: i64,
    pub event_addr: String,
    pub vote: EventVote,
}

#[derive(Debug, Clone, Copy, Ord, PartialOrd, Eq, PartialEq, Serialize, Deserialize, OpgModel)]
#[serde(rename_all = "lowercase")]
pub enum EventVote {
    Confirm,
    Reject,
}
