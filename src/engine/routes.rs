use std::convert::Infallible;
use std::str::FromStr;

use anyhow::Error;
use bip39::Language;
use serde::Serialize;
use sled::Db;
use ton_block::MsgAddressInt;
use url::Url;
use warp::http::StatusCode;
use warp::{reply, Filter, Reply};

use relay_eth::ws::update_eth_state;
use relay_eth::ws::EthListener;
use relay_ton::contracts::BridgeContract;
use relay_ton::prelude::{Arc, RwLock};
use relay_ton::transport::Transport;

use crate::config::{RelayConfig, TonConfig};
use crate::crypto::key_managment::KeyData;
use crate::crypto::recovery::*;
use crate::engine::bridge::Bridge;
use crate::engine::models::{BridgeState, InitData, Password, RescanEthData, State};

pub async fn serve(config: RelayConfig, state: Arc<RwLock<State>>) {
    log::info!("Starting server");
    let serve_address = config.listen_address;
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

    let rescan_from_block_eth = warp::path!("rescan_eth")
        .and(warp::path::end())
        .and(json_data::<RescanEthData>())
        .and(state.clone())
        .and_then(|data, (state, _)| set_eth_block_height(state, data));

    let routes = init.or(password).or(status).or(rescan_from_block_eth);
    warp::serve(routes).run(serve_address).await;
}

async fn set_eth_block_height(
    state: Arc<RwLock<State>>,
    height: RescanEthData,
) -> Result<impl Reply, Infallible> {
    let state = state.write().await;
    let db = state.state_manager.clone();
    Ok(match update_eth_state(&db, height.block) {
        Ok(_) => {
            log::info!("Changed  eth scan height to {}", height.block);
            reply::with_status("OK".to_string(), StatusCode::OK)
        }
        Err(e) => {
            let err = format!("Failed changeging eth scan height: {}", e);
            log::error!("{}", &err);
            reply::with_status(err, StatusCode::INTERNAL_SERVER_ERROR)
        }
    })
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

pub async fn create_bridge(
    state_manager: Db,
    config: RelayConfig,
    key_data: KeyData,
) -> Result<Arc<Bridge>, Error> {
    let transport = config
        .ton_config
        .make_transport(state_manager.clone())
        .await?;

    let contract_address = MsgAddressInt::from_str(&*config.ton_contract_address.0)
        .map_err(|e| Error::msg(e.to_string()))?;

    let ton_client =
        BridgeContract::new(transport.clone(), contract_address, key_data.ton.keypair()).await?;

    let eth_client = EthListener::new(
        Url::parse(config.eth_node_address.as_str())
            .map_err(|e| Error::new(e).context("Bad url for eth_config provided"))?,
        state_manager,
    )
    .await?;

    Ok(Arc::new(Bridge::new(
        key_data.eth,
        eth_client,
        ton_client,
        transport,
    )))
}

impl TonConfig {
    pub async fn make_transport(&self, db: Db) -> Result<Arc<dyn Transport>, Error> {
        #[allow(unreachable_code)]
        Ok(match self {
            #[cfg(feature = "tonlib-transport")]
            TonConfig::Tonlib(config) => Arc::new(
                relay_ton::transport::TonlibTransport::new(config.clone(), db.clone()).await?,
            ),
            #[cfg(feature = "graphql-transport")]
            TonConfig::GraphQL(config) => Arc::new(
                relay_ton::transport::GraphQLTransport::new(config.clone(), db.clone()).await?,
            ),
        })
    }
}
