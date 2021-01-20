use std::convert::Infallible;
use std::sync::Arc;

use borsh::{BorshDeserialize, BorshSerialize};
use serde::Serialize;
use tokio::sync::RwLock;
use ton_block::MsgAddrStd;
use warp::Reply;

use relay_models::models::*;

use crate::db::*;
use crate::engine::models::*;
use crate::models::*;

pub async fn get_status(state: Arc<RwLock<State>>) -> Result<impl Reply, Infallible> {
    let state = state.read().await;

    let result = match &state.bridge_state {
        BridgeState::Uninitialized => Status {
            password_needed: true,
            init_data_needed: true,
            is_working: false,
            ton_relay_address: None,
            eth_pubkey: None,
        },
        BridgeState::Locked => Status {
            password_needed: true,
            init_data_needed: true,
            is_working: false,
            ton_relay_address: None,
            eth_pubkey: None,
        },
        BridgeState::Running(bridge) => {
            let ton_relay_address = bridge.ton_relay_address();
            let eth_pubkey = bridge.eth_pubkey();

            Status {
                ton_relay_address: Some(ton_relay_address.to_string()),
                eth_pubkey: Some(eth_pubkey.to_string()),
                password_needed: false,
                init_data_needed: false,
                is_working: true,
            }
        }
    };
    Ok(serde_json::to_string(&result).expect("Shouldn't fail"))
}

pub async fn pending<Confirm, Reject, View>(
    state: Arc<RwLock<State>>,
) -> Result<impl Reply, Infallible>
where
    EventTransaction<Confirm, Reject>: VotesQueueExt,
    EventTransaction<Confirm, Reject>: Into<View> + BorshSerialize + BorshDeserialize,
    View: Serialize + From<EventTransaction<Confirm, Reject>>,
{
    let state = state.read().await;
    let provider =
        EventTransaction::<Confirm, Reject>::new(&state.state_manager).expect("Fatal db error");
    let pending = fold_ton_stats(provider.get_all_pending());
    Ok(serde_json::to_string(&pending).expect("Shouldn't fail"))
}

pub async fn failed<Confirm, Reject, View>(
    state: Arc<RwLock<State>>,
) -> Result<impl Reply, Infallible>
where
    EventTransaction<Confirm, Reject>: VotesQueueExt,
    EventTransaction<Confirm, Reject>: Into<View> + BorshSerialize + BorshDeserialize,
    View: Serialize + From<EventTransaction<Confirm, Reject>>,
{
    let state = state.read().await;
    let provider =
        EventTransaction::<Confirm, Reject>::new(&state.state_manager).expect("Fatal db error");
    let failed = fold_ton_stats(provider.get_all_failed());
    Ok(serde_json::to_string(&failed).expect("Shouldn't fail"))
}

pub async fn eth_queue(state: Arc<RwLock<State>>) -> Result<impl Reply, Infallible> {
    let state = state.read().await;
    let provider = EthVerificationQueue::new(&state.state_manager).expect("Fatal db error");
    let data = provider.dump_elements();
    Ok(serde_json::to_string(&data).expect("Shouldn't fail"))
}

pub async fn ton_queue(
    state: Arc<RwLock<State>>,
    configuration_id: u64,
) -> Result<impl Reply, Infallible> {
    let state = state.read().await;
    let provider =
        TonVerificationQueue::new(&state.state_manager, configuration_id).expect("Fatal db error");
    let data = provider.dump_elements();
    Ok(serde_json::to_string(&data).expect("Shouldn't fail"))
}

pub async fn eth_relay_stats(state: Arc<RwLock<State>>) -> Result<impl Reply, Infallible> {
    let state = state.read().await;
    let provider = EthVotingStats::new(&state.state_manager).expect("Fatal db error");
    let data = provider.dump_elements();
    Ok(serde_json::to_string(&data).expect("Shouldn't fail"))
}

pub async fn ton_relay_stats(state: Arc<RwLock<State>>) -> Result<impl Reply, Infallible> {
    let state = state.read().await;
    let provider = TonVotingStats::new(&state.state_manager).expect("Fatal db error");
    let data = provider.dump_elements();
    Ok(serde_json::to_string(&data).expect("Shouldn't fail"))
}

pub async fn retry_failed(state: Arc<RwLock<State>>) -> Result<impl Reply, Infallible> {
    let state = state.read().await;
    let res = match &state.bridge_state {
        BridgeState::Running(a) => {
            a.retry_failed();
            warp::http::StatusCode::OK
        }
        _ => warp::http::StatusCode::FORBIDDEN,
    };
    Ok(warp::reply::with_status("", res))
}

fn fold_ton_stats<I, Confirm, Reject, View>(iter: I) -> Vec<View>
where
    I: Iterator<Item = (MsgAddrStd, EventTransaction<Confirm, Reject>)>,
    EventTransaction<Confirm, Reject>: VotesQueueExt,
    EventTransaction<Confirm, Reject>: Into<View>,
    View: Serialize + From<EventTransaction<Confirm, Reject>>,
{
    iter.map(|(_, transaction)| transaction.into()).collect()
}
