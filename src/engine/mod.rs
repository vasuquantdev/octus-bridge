mod bridge;

use std::convert::Infallible;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;

use anyhow::Error;
use bip39::Language;
use serde::{Deserialize, Serialize};
use sled::Db;
use tokio::sync::RwLock;
use ton_block::MsgAddressInt;
use url::Url;
use warp::http::StatusCode;
use warp::{reply, Filter, Reply};

use relay_eth::ws::EthListener;
use relay_ton::contracts::BridgeContract;
use relay_ton::transport::Transport;

use crate::config::RelayConfig;
use crate::crypto::key_managment::KeyData;
use crate::crypto::recovery::*;
use crate::engine::bridge::Bridge;

struct State {
    state_manager: Db,
    bridge_state: BridgeState,
}

enum BridgeState {
    Uninitialized,
    Locked,
    Running(Arc<Bridge>),
}

impl State {
    async fn finalize(&mut self, config: RelayConfig, key_data: KeyData) -> Result<(), Error> {
        let bridge = create_bridge(self.state_manager.clone(), config, key_data).await?;

        log::info!("Successfully initialized");

        let spawned_bridge = bridge.clone();
        tokio::spawn(async move { spawned_bridge.run().await });

        self.bridge_state = BridgeState::Running(bridge);
        Ok(())
    }
}

#[derive(Deserialize, Debug)]
pub struct InitData {
    ton_seed: String,
    eth_seed: String,
    password: String,
    language: String,
}

#[derive(Deserialize, Debug)]
struct Password {
    password: String,
}

async fn serve(config: RelayConfig, state: Arc<RwLock<State>>) {
    log::info!("Starting server");

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
        .and_then(|data, (state, config)| wait_for_password(data, config, state));

    let status = warp::path!("status")
        .and(warp::path::end())
        .and(state.clone())
        .and_then(|(state, _)| get_status(state));

    let init = warp::path!("init")
        .and(warp::path::end())
        .and(json_data::<InitData>())
        .and(state.clone())
        .and_then(|data, (state, config)| wait_for_init(data, config, state));

    let routes = init.or(password).or(status);
    let addr: SocketAddr = "127.0.0.1:12345".parse().unwrap();
    warp::serve(routes).run(addr).await;
}

async fn get_status(state: Arc<RwLock<State>>) -> Result<impl Reply, Infallible> {
    let state = state.read().await;

    #[derive(Serialize)]
    struct Status {
        password_needed: bool,
        init_data_needed: bool,
        is_working: bool,
    }

    let result = match &state.bridge_state {
        BridgeState::Uninitialized => Status {
            password_needed: true,
            init_data_needed: true,
            is_working: false,
        },
        BridgeState::Locked => Status {
            password_needed: true,
            init_data_needed: true,
            is_working: false,
        },
        BridgeState::Running(_) => Status {
            password_needed: false,
            init_data_needed: false,
            is_working: true,
        },
    };

    drop(state);
    Ok(serde_json::to_string(&result).expect("Can't fail"))
}

async fn wait_for_init(
    data: InitData,
    config: RelayConfig,
    state: Arc<RwLock<State>>,
) -> Result<impl Reply, Infallible> {
    log::info!("Received init data");

    let mut state = state.write().await;

    if !matches!(&state.bridge_state, BridgeState::Uninitialized) {
        let err = "Already initialized".to_string();
        log::error!("{}", &err);
        return Ok(reply::with_status(err, StatusCode::METHOD_NOT_ALLOWED));
    }

    let language = match Language::from_language_code(&data.language) {
        Some(a) => a,
        None => {
            let error = format!("Bad language code provided: {}.", &data.language);
            log::error!("{}", &error);
            return Ok(reply::with_status(error, StatusCode::BAD_REQUEST));
        }
    };

    let eth_private_key = match derive_from_words_eth(language, &data.eth_seed) {
        Ok(a) => a,
        Err(e) => {
            let error = format!("Failed deriving from eth seed: {}", e);
            log::error!("{}", &error);
            return Ok(reply::with_status(error, StatusCode::BAD_REQUEST));
        }
    };

    let ton_key_pair = match derive_from_words_ton(language, &data.ton_seed) {
        Ok(a) => a,
        Err(e) => {
            let error = format!("Failed deriving from ton seed: {}", e);
            log::error!("{}", &error);
            return Ok(reply::with_status(error, StatusCode::BAD_REQUEST));
        }
    };

    let key_data = match KeyData::init(
        &config.encrypted_data,
        data.password.into(),
        eth_private_key,
        ton_key_pair,
    ) {
        Ok(key_data) => key_data,
        Err(e) => {
            let error = format!("Failed initializing: {}", e);
            log::error!("{}", &error);
            return Ok(reply::with_status(error, StatusCode::BAD_REQUEST));
        }
    };

    if let Err(e) = state.finalize(config, key_data).await {
        let error = format!("Failed finalize relay state: {}", &e);
        log::error!("{}", &error);
        return Ok(reply::with_status(error, StatusCode::BAD_REQUEST));
    };

    Ok(reply::with_status(
        "Initialized successfully".to_string(),
        StatusCode::ACCEPTED,
    ))
}

async fn wait_for_password(
    data: Password,
    config: RelayConfig,
    state: Arc<RwLock<State>>,
) -> Result<impl Reply, Infallible> {
    log::info!("Received unlock request");

    let mut state = state.write().await;

    match &state.bridge_state {
        BridgeState::Uninitialized => {
            return Ok(reply::with_status(
                "Need to initialize first".to_string(),
                StatusCode::METHOD_NOT_ALLOWED,
            ));
        }
        BridgeState::Running(_) => {
            return Ok(reply::with_status(
                "Already unlocked".to_string(),
                StatusCode::METHOD_NOT_ALLOWED,
            ));
        }
        _ => {}
    }

    let key_data = match KeyData::from_file(config.encrypted_data.clone(), data.password.into()) {
        Ok(key_data) => key_data,
        Err(e) => {
            let error = format!("Failed unlocking relay: {}", &e);
            log::error!("{}", &error);
            return Ok(reply::with_status(error, StatusCode::BAD_REQUEST));
        }
    };

    if let Err(e) = state.finalize(config, key_data).await {
        let error = format!("Failed finalize relay state: {}", &e);
        log::error!("{}", &error);
        return Ok(reply::with_status(error, StatusCode::BAD_REQUEST));
    };

    Ok(reply::with_status(
        "Password accepted".to_string(),
        StatusCode::ACCEPTED,
    ))
}

async fn create_bridge(
    state_manager: Db,
    config: RelayConfig,
    key_data: KeyData,
) -> Result<Arc<Bridge>, Error> {
    let transport: Arc<dyn Transport> =
        Arc::new(relay_ton::transport::TonlibTransport::new(config.ton_config.clone()).await?);

    let contract_address = MsgAddressInt::from_str(&*config.ton_contract_address.0)
        .map_err(|e| Error::msg(e.to_string()))?;

    let ton_client =
        BridgeContract::new(transport, &contract_address, key_data.ton.keypair()).await?;

    let eth_client = EthListener::new(
        Url::parse(config.eth_node_address.as_str())
            .map_err(|e| Error::new(e).context("Bad url for eth_config provided"))?,
        state_manager,
    )
    .await;

    Ok(Arc::new(Bridge::new(key_data.eth, eth_client, ton_client)))
}

pub async fn run(config: RelayConfig) -> Result<(), Error> {
    let state_manager = sled::open(&config.storage_path)?;

    let crypto_data_metadata = std::fs::File::open(&config.encrypted_data);
    let file_size = match crypto_data_metadata {
        Err(e) => {
            log::warn!("Error opening file with encrypted config: {}", e);
            0
        }
        Ok(a) => a.metadata()?.len(),
    };

    let bridge_state = match file_size {
        0 => {
            log::info!("started in uninitialized state");
            BridgeState::Uninitialized
        }
        _ => {
            log::info!("stared in locked state");
            BridgeState::Locked
        }
    };

    serve(
        config,
        Arc::new(RwLock::new(State {
            state_manager,
            bridge_state,
        })),
    )
    .await;
    Ok(())
}
