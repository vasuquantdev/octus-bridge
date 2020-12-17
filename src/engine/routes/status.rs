use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use warp::Reply;

use crate::db_managment::{EthQueue, StatsDb, Table, TonQueue, TxStat};
use crate::engine::models::{BridgeState, State};

pub async fn get_status(state: Arc<RwLock<State>>) -> Result<impl Reply, Infallible> {
    let state = state.read().await;

    #[derive(Serialize)]
    struct Status {
        password_needed: bool,
        init_data_needed: bool,
        is_working: bool,
        relay_stats: HashMap<String, Vec<TxStat>>,
    }
    let provider = StatsDb::new(&state.state_manager).unwrap();
    let relay_stats = provider.dump_elements();
    let result = match &state.bridge_state {
        BridgeState::Uninitialized => Status {
            password_needed: true,
            init_data_needed: true,
            is_working: false,
            relay_stats,
        },
        BridgeState::Locked => Status {
            password_needed: true,
            init_data_needed: true,
            is_working: false,
            relay_stats,
        },
        BridgeState::Running(_) => Status {
            password_needed: false,
            init_data_needed: false,
            is_working: true,
            relay_stats,
        },
    };

    drop(state);
    Ok(serde_json::to_string(&result).expect("Can't fail"))
}

pub async fn pending(state: Arc<RwLock<State>>) -> Result<impl Reply, Infallible> {
    let state = state.read().await;
    let provider = TonQueue::new(&state.state_manager).unwrap();
    let pending = provider.get_all_pending();
    Ok(serde_json::to_string(&pending).unwrap())
}

pub async fn failed(state: Arc<RwLock<State>>) -> Result<impl Reply, Infallible> {
    let state = state.read().await;
    let provider = TonQueue::new(&state.state_manager).unwrap();
    let failed = provider.get_all_failed();
    Ok(serde_json::to_string(&failed).unwrap())
}

pub async fn eth_queue(state: Arc<RwLock<State>>) -> Result<impl Reply, Infallible>
{
    let state = state.read().await;
    let provider = EthQueue::new(&state.state_manager).unwrap();
    let data = provider.dump_elements();
    Ok(serde_json::to_string(&data).unwrap())
}
