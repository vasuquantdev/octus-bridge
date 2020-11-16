use std::collections::HashMap;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::Error;
use bip39::Language;
use futures::{AsyncReadExt, FutureExt, TryFutureExt};
use log::error;
use log::{info, warn};
use secstr::SecStr;
use serde::Deserialize;
use tiny_hderive::bip44::IntoDerivationPath;
use tokio::sync::Mutex;
use url::Url;
use warp::http::StatusCode;
use warp::reject::custom;
use warp::reply::{with_status, Json};
use warp::{reject, Filter, Rejection, Reply};

use recovery::derive_from_words;
use relay_eth::ws::EthConfig;

use crate::config::RelayConfig;
use crate::crypto::key_managment::KeyData;
use crate::crypto::recovery;
use crate::storage;

#[derive(Debug, Eq, PartialEq)]
enum State {
    Sleeping,
    InitDataWaiting,
    InitDataReceived,
    PasswordWaiting,
    PasswordReceived(KeyData),
}

#[derive(Deserialize, Debug)]
pub struct InitData {
    ton_seed: Vec<String>,
    eth_seed: String,
    password: String,
    language: String,
}

// async fn serve(key_data: Option<KeyData>) {
//     #[derive(Deserialize, Debug)]
//     struct Password {
//         password: String,
//     }
//     let json_data = warp::body::content_length_limit(1024 * 1024).and(warp::filters::body::json());
//
//     let init = warp::path!("init")
//         .and(json_data)
//         .map(|data: InitData| format!("{:?}", &data));
//     // let password = warp::path("unlock")
//     //     .and(json_data)
//     //     .map(|data: Password|{
//     //         let
//     //     });
//     // );
//
//     let routes = warp::post().and(init);
//     let addr: SocketAddr = "127.0.0.1:12345".parse().unwrap();
//     warp::serve(routes).run(addr).await;
// }

fn create_error<R>(reason: R) -> Json
where
    R: AsRef<str>,
{
    let mut obj = HashMap::new();
    obj.insert("reason", reason.as_ref());
    warp::reply::json(&obj)
}

#[derive(Debug)]
struct PasswordError(String, StatusCode);

impl PasswordError {
    async fn handle_rejection(err: Rejection) -> std::result::Result<impl Reply, Infallible> {
        if let Some(PasswordError(a, b)) = err.find() {
            let err = create_error(a);
            return Ok(with_status(err, *b));
        } else {
            return Ok(with_status(
                create_error("Internal Server Error"),
                StatusCode::INTERNAL_SERVER_ERROR,
            ));
        }
    }
}

impl reject::Reject for PasswordError {}

#[derive(Deserialize, Debug)]
struct Password {
    password: String,
}

async fn serve(config: RelayConfig, state: Arc<Mutex<State>>) {
    info!("Waiting for config data");
    let json_data = warp::body::content_length_limit(1024 * 1024).and(warp::filters::body::json());

    let state = warp::any().map(move || (Arc::clone(&state), config.clone()));

    let password = warp::path!("unlock")
        .and(json_data)
        .and(state.clone())
        .and_then(
            |data: Password, (state, config): (Arc<Mutex<State>>, RelayConfig)| {
                wait_for_password(data, config, state)
            },
        )
        .recover(PasswordError::handle_rejection);

    let init = warp::path!("init")
        .and(json_data)
        .and(state.clone())
        .and_then(
            |data: InitData, (state, config): (Arc<Mutex<State>>, RelayConfig)| {
                wait_for_init(data, config, state)
            },
        )
        .recover(WaitForInitError::handle_rejection);

    let routes = init.or(password);
    let addr: SocketAddr = "127.0.0.1:12345".parse().unwrap();
    warp::serve(routes).run(addr).await;
}

#[derive(Debug)]
struct WaitForInitError(String, StatusCode);

impl reject::Reject for WaitForInitError {}

impl WaitForInitError {
    async fn handle_rejection(err: Rejection) -> std::result::Result<impl Reply, Infallible> {
        return if let Some(WaitForInitError(a, b)) = err.find() {
            let err = create_error(a);
            Ok(with_status(err, *b))
        } else {
            Ok(with_status(
                create_error("Internal Server Error"),
                StatusCode::INTERNAL_SERVER_ERROR,
            ))
        }
    }
}

async fn wait_for_init(
    data: InitData,
    config: RelayConfig,
    state: Arc<Mutex<State>>,
) -> Result<impl Reply, Rejection> {
    info!("Recieved init data");
    let mut state = state.lock().await;
    if *state != State::InitDataWaiting {
        let err = format!("Not waiting for init data now");
        error!("{}", &err);
        return Err(custom(WaitForInitError(
            err,
            StatusCode::METHOD_NOT_ALLOWED,
        )));
    }
    let language = match Language::from_language_code(&data.language) {
        Some(a) => a,
        None => {
            let error = format!("Bad error code provided: {}", &data.language);
            error!("{}", &error);
            return Err(custom(WaitForInitError(error, StatusCode::BAD_REQUEST)));
        }
    };
    let key = match derive_from_words(language, &data.eth_seed) {
        Ok(a) => a,
        Err(e) => {
            let error = format!("Failed deriving from eth seed: {}", e);
            error!("{}", &error);
            return Err(custom(WaitForInitError(error, StatusCode::BAD_REQUEST)));
        }
    };

    if let Err(e) = KeyData::init(
        &config.pem_location,
        data.password.into(),
        vec![1, 2, 3, 4],
        key,
    ) {
        let err = format!("Failed initializing: {}", e);
        error!("{}", &err);
        return Err(custom(WaitForInitError(
            err,
            StatusCode::INTERNAL_SERVER_ERROR,
        )));
    };
    info!("Successfully initialized");
    *state = State::InitDataReceived;
    Ok(warp::reply())
}

async fn wait_for_password(
    data: Password,
    config: RelayConfig,
    state: Arc<Mutex<State>>,
) -> Result<impl Reply, Rejection> {
    let mut state = state.lock().await;
    if *state != State::PasswordWaiting {
        return Err(custom(PasswordError(
            "Not waiting for password".to_string(),
            StatusCode::NOT_ACCEPTABLE,
        )));
    }

    match KeyData::from_file(config.pem_location, data.password.into()) {
        Ok(a) => *state = State::PasswordReceived(a),
        Err(e) => {
            let error = format!("Failed unlocking relay: {}", &e);
            error!("{}", &error);
            return Err(custom(PasswordError(error, StatusCode::BAD_REQUEST)));
        }
    };
    Ok(warp::reply())
}

// }

pub async fn run(config: RelayConfig) -> Result<(), Error> {
    let eth_config = EthConfig::new(
        Url::parse(config.eth_node_address.as_str())
            .map_err(|e| Error::new(e).context("Bad url for eth_config provided"))?,
    )
    .await;

    let state_manager = storage::StateManager::new(&config.storage_path)?;
    let crypto_data_metadata = std::fs::File::open(&config.pem_location);
    let mut state = Arc::new(Mutex::new(State::Sleeping));
    let file_size = match crypto_data_metadata {
        Err(e) => {
            warn!("Error opening file with encrypted config: {}", e);
            0
        }
        Ok(a) => a.metadata()?.len(),
    };

    if file_size == 0 {
        serve(config, state).await;
    }
    // let crypto_config = KeyData::from_file(&config.pem_location)?;

    // serve().await;
    Ok(())
}
