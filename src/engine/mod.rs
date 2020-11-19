use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::Error;
use bip39::Language;
use log::error;
use log::{info, warn};
use serde::Deserialize;
use serde_json::json;
use tokio::sync::Mutex;
use url::Url;
use warp::http::StatusCode;
use warp::reply::{json, with_status};
use warp::{Filter, Reply};

use recovery::derive_from_words;
use relay_eth::ws::EthListener;

use crate::config::RelayConfig;
use crate::crypto::key_managment::KeyData;
use crate::crypto::recovery;
use crate::engine::bridge::Bridge;
use crate::storage;
use crate::storage::PersistentStateManager;

mod bridge;

struct State {
    init_data_needed: bool,
    password_needed: bool,
    bridge: Option<Arc<Bridge>>,
    relay_state: RelayState,
}

#[derive(Deserialize, Debug)]
pub struct InitData {
    ton_seed: Vec<String>,
    eth_seed: String,
    password: String,
    language: String,
}

#[derive(Deserialize, Debug)]
struct Password {
    password: String,
}

async fn serve(config: RelayConfig, state: Arc<Mutex<State>>) {
    info!("Starting server");

    fn json_data<T>() -> impl Filter<Extract = (T,), Error = warp::Rejection> + Clone
    where
        for<'a> T: serde::Deserialize<'a> + Send,
    {
        warp::body::content_length_limit(1024 * 1024).and(warp::filters::body::json::<T>())
    }

    let state = warp::any().map(move || (Arc::clone(&state), config.clone()));

    let password = warp::path!("unlock")
        .and(warp::path::end())
        .and(json_data::<Password>())
        .and(state.clone())
        .and_then(
            |data: Password, (state, config): (Arc<Mutex<State>>, RelayConfig)| {
                wait_for_password(data, config, state)
            },
        );
    let status = warp::path!("status")
        .and(warp::path::end())
        .and(state.clone())
        .and_then(|(state, _): (Arc<Mutex<State>>, RelayConfig)| get_status(state));

    let init = warp::path!("init")
        .and(warp::path::end())
        .and(json_data::<InitData>())
        .and(state.clone())
        .and_then(
            |data: InitData, (state, config): (Arc<Mutex<State>>, RelayConfig)| {
                wait_for_init(data, config, state)
            },
        );

    let routes = init.or(password).or(status);
    let addr: SocketAddr = "127.0.0.1:12345".parse().unwrap();
    warp::serve(routes).run(addr).await;
}

async fn get_status(state: Arc<Mutex<State>>) -> Result<impl Reply, Infallible> {
    let state = state.lock().await;
    let json = json!({
        "password_needed": state.password_needed,
        "init_data_needed": state.init_data_needed,
        "is_working": state.bridge.is_some()
    });
    drop(state);
    Ok(serde_json::to_string(&json).expect("Can't fail"))
}

async fn wait_for_init(
    data: InitData,
    config: RelayConfig,
    state: Arc<Mutex<State>>,
) -> Result<impl Reply, Infallible> {
    info!("Recieved init data");
    let mut state = state.lock().await;
    if !state.init_data_needed {
        let err = "Already initialized".to_string();
        error!("{}", &err);
        return Ok(with_status(err, StatusCode::METHOD_NOT_ALLOWED));
    }
    let language = match Language::from_language_code(&data.language) {
        Some(a) => a,
        None => {
            let error = format!("Bad language code provided: {}.", &data.language);
            error!("{}", &error);
            return Ok(with_status(error, StatusCode::BAD_REQUEST));
        }
    };
    let key = match derive_from_words(language, &data.eth_seed) {
        Ok(a) => a,
        Err(e) => {
            let error = format!("Failed deriving from eth seed: {}", e);
            error!("{}", &error);
            return Ok(with_status(error, StatusCode::BAD_REQUEST));
        }
    };
    let key_data = match KeyData::init(
        &config.encrypted_data,
        data.password.into(),
        vec![1, 2, 3, 4],
        key,
    ) {
        Err(e) => {
            let error = format!("Failed initializing: {}", e);
            error!("{}", &error);
            return Ok(with_status(error, StatusCode::BAD_REQUEST));
        }
        Ok(a) => a,
    };

    let bridge = Arc::new(Bridge::new(
        key_data.eth,
        state.relay_state.eth_listener.clone(),
    )); //todo  ton
    info!("Successfully initialized");
    let spawned_bridge = bridge.clone();
    tokio::spawn(async move { spawned_bridge.run().await });
    *state = State {
        init_data_needed: false,
        password_needed: state.password_needed,
        bridge: Some(bridge),
        relay_state: state.relay_state.clone(),
    };
    Ok(with_status(
        "Initialized successfully".to_string(),
        StatusCode::ACCEPTED,
    ))
}

async fn wait_for_password(
    data: Password,
    config: RelayConfig,
    state: Arc<Mutex<State>>,
) -> Result<impl Reply, Infallible> {
    info!("Received unlock request");
    let mut state = state.lock().await;
    if !state.password_needed && state.init_data_needed {
        return Ok(with_status(
            "Need to initialize first".to_string(),
            StatusCode::METHOD_NOT_ALLOWED,
        ));
    } else if !state.password_needed {
        return Ok(with_status(
            "Already unlocked".to_string(),
            StatusCode::METHOD_NOT_ALLOWED,
        ));
    }

    match KeyData::from_file(config.encrypted_data, data.password.into()) {
        Ok(a) => {
            let (signer, _) = (a.eth, a.ton); //todo ton part
            let bridge = Arc::new(Bridge::new(signer, state.relay_state.eth_listener.clone()));

            *state = State {
                init_data_needed: false,
                password_needed: false,
                bridge: Some(bridge.clone()),
                relay_state: state.relay_state.clone(),
            };
            tokio::spawn(async move { bridge.run().await });
        }
        Err(e) => {
            let error = format!("Failed unlocking relay: {}", &e);
            error!("{}", &error);
            return Ok(with_status(error, StatusCode::BAD_REQUEST));
        }
    };
    Ok(with_status(
        "Password accepted".to_string(),
        StatusCode::ACCEPTED,
    ))
}

#[derive(Clone)]
struct RelayState {
    persistent_state_manager: PersistentStateManager,
    eth_listener: EthListener,
}

pub async fn run(config: RelayConfig) -> Result<(), Error> {
    let eth_config = EthListener::new(
        Url::parse(config.eth_node_address.as_str())
            .map_err(|e| Error::new(e).context("Bad url for eth_config provided"))?,
    )
    .await;

    let state_manager = storage::PersistentStateManager::new(&config.storage_path)?;
    let crypto_data_metadata = std::fs::File::open(&config.encrypted_data);
    let mut state = State {
        bridge: None,
        password_needed: false,
        init_data_needed: false,
        relay_state: RelayState {
            eth_listener: eth_config,
            persistent_state_manager: state_manager,
        },
    };
    let file_size = match crypto_data_metadata {
        Err(e) => {
            warn!("Error opening file with encrypted config: {}", e);
            0
        }
        Ok(a) => a.metadata()?.len(),
    };

    if file_size == 0 {
        state = State {
            init_data_needed: true,
            ..state
        };
    } else {
        state = State {
            password_needed: true,
            ..state
        }
    }
    info!(
        "State is: password_needed: {}, init_data_needed:{}",
        state.password_needed, state.init_data_needed
    );
    serve(config, Arc::new(Mutex::new(state))).await;
    Ok(())
}
