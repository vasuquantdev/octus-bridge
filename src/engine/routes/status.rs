use std::convert::Infallible;
use std::sync::Arc;

use serde::de::DeserializeOwned;
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
            ton_pubkey: None,
            eth_pubkey: None,
        },
        BridgeState::Locked => Status {
            password_needed: true,
            init_data_needed: true,
            is_working: false,
            ton_pubkey: None,
            eth_pubkey: None,
        },
        BridgeState::Running(bridge) => {
            let ton_pubkey = bridge.ton_pubkey();
            let eth_pubkey = bridge.eth_pubkey();

            Status {
                ton_pubkey: Some(ton_pubkey.to_hex_string()),
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
    EventTransaction<Confirm, Reject>: VotesQueueExt + Serialize + DeserializeOwned,
    EventTransaction<Confirm, Reject>: Into<View>,
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
    EventTransaction<Confirm, Reject>: VotesQueueExt + Serialize + DeserializeOwned,
    EventTransaction<Confirm, Reject>: Into<View>,
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
    let provider = EthQueue::new(&state.state_manager).expect("Fatal db error");
    let data = provider.dump_elements();
    Ok(serde_json::to_string(&data).expect("Shouldn't fail"))
}

pub async fn all_relay_stats(state: Arc<RwLock<State>>) -> Result<impl Reply, Infallible> {
    let state = state.read().await;
    let provider = EthVotingStats::new(&state.state_manager).expect("Fatal db error");
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
    EventTransaction<Confirm, Reject>: VotesQueueExt + Serialize + DeserializeOwned,
    EventTransaction<Confirm, Reject>: Into<View>,
    View: Serialize + From<EventTransaction<Confirm, Reject>>,
{
    iter.map(|(_, transaction)| transaction.into()).collect()
}
