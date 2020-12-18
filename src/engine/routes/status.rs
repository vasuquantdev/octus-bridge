use std::convert::Infallible;
use std::sync::Arc;

use chrono::{DateTime, NaiveDateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use warp::Reply;

use crate::db_managment::{EthQueue, StatsDb, Table, TonQueue};
use crate::engine::models::{BridgeState, State};

pub async fn get_status(state: Arc<RwLock<State>>) -> Result<impl Reply, Infallible> {
    let state = state.read().await;

    #[derive(Serialize)]
    struct Status {
        password_needed: bool,
        init_data_needed: bool,
        is_working: bool,
        ton_pubkey: Option<String>,
        eth_pubkey: Option<String>,
    }
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

pub async fn pending(state: Arc<RwLock<State>>) -> Result<impl Reply, Infallible> {
    let state = state.read().await;
    let provider = TonQueue::new(&state.state_manager).expect("Fatal db error");
    let pending = provider.get_all_pending();
    Ok(serde_json::to_string(&pending).expect("Shouldn't fail"))
}

pub async fn failed(state: Arc<RwLock<State>>) -> Result<impl Reply, Infallible> {
    let state = state.read().await;
    let provider = TonQueue::new(&state.state_manager).expect("Fatal db error");
    let failed = provider.get_all_failed();
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
    let provider = StatsDb::new(&state.state_manager).expect("Fatal db error");
    let data = provider.dump_elements();
    Ok(serde_json::to_string(&data).expect("Shouldn't fail"))
}

pub async fn remove_failed_older_than(
    state: Arc<RwLock<State>>,
    time: GcOlderThen,
) -> Result<impl Reply, Infallible> {
    let state = state.read().await;
    match &state.bridge_state {
        BridgeState::Running(_) => (),
        _ => {
            log::error!("Bridge is locked. Not allowing to gc events");
            return Ok(warp::reply::with_status(
                "Relay is not running".into(),
                warp::http::StatusCode::FORBIDDEN,
            ));
        }
    }
    let dt = DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(time.time, 0), Utc);
    log::info!("Got remove_failed_older_than {} request", dt);
    let queue = TonQueue::new(&state.state_manager).expect("Fatal db error");
    let result = match queue.remove_failed_older_than(&dt) {
        Err(e) => {
            let message = format!("Faied removing old entries in ton queue: {}", e);
            log::error!("{}", message);
            warp::reply::with_status(message, warp::http::StatusCode::INTERNAL_SERVER_ERROR)
        }
        Ok(a) => {
            let message = format!("Removed {} old entries from failed ton queue", a);
            log::info!("{}", message);
            warp::reply::with_status(message, warp::http::StatusCode::OK)
        }
    };
    Ok(result)
}

#[derive(Deserialize, Serialize, Debug)]
pub struct GcOlderThen {
    pub time: i64,
}
