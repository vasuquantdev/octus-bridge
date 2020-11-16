use std::collections::HashMap;
use std::net::SocketAddr;

use anyhow::Error;
use bip39::Language;
use futures::FutureExt;
use log::error;
use log::{info, warn};
use secstr::SecStr;
use serde::Deserialize;
use url::Url;
use warp::http::StatusCode;
use warp::reply::{with_status, Json};
use warp::Filter;

use recovery::derive_from_words;
use relay_eth::ws::EthConfig;

use crate::config::RelayConfig;
use crate::crypto::key_managment::KeyData;
use crate::crypto::recovery;
use crate::storage;

enum State {
    Sleeping,
    InitDataWaiting,
    InitDataReceived(InitData),
    PasswordWaiting,
    PasswordReceived(SecStr),
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

async fn wait_for_init(config: RelayConfig) {
    info!("Waiting for config data");
    let json_data = warp::body::content_length_limit(1024 * 1024).and(warp::filters::body::json());
    let init = warp::path!("init")
        .and(json_data)
        .map(move |data: InitData| {
            info!("Recieved init data");
            let language = match Language::from_language_code(&data.language) {
                Some(a) => a,
                None => {
                    error!("Bad error code provided: {}", &data.language);
                    return with_status(
                        create_error("Bad error code provided"),
                        StatusCode::BAD_REQUEST,
                    );
                }
            };
            let key = match derive_from_words(language, &data.eth_seed) {
                Ok(a) => a,
                Err(e) => {
                    let error = format!("Failed deriving from eth seed: {}", e);
                    error!("{}", &error);
                    return with_status(create_error(&error), StatusCode::BAD_REQUEST);
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
                return with_status(create_error(&err), StatusCode::INTERNAL_SERVER_ERROR);
            };
            info!("Successfully initialized");
            with_status(warp::reply::json(&""), StatusCode::OK) //fixme Fucking shame, fix it asap
        });
    let addr: SocketAddr = "127.0.0.1:12345".parse().unwrap();
    warp::serve(init).run(addr).await;
}

pub async fn run(config: RelayConfig) -> Result<(), Error> {
    let eth_config = EthConfig::new(
        Url::parse(config.eth_node_address.as_str())
            .map_err(|e| Error::new(e).context("Bad url for eth_config provided"))?,
    )
    .await;

    let state_manager = storage::StateManager::new(&config.storage_path)?;
    let crypto_data_metadata = std::fs::File::open(&config.pem_location);
    let file_size = match crypto_data_metadata {
        Err(e) => {
            warn!("Error opening file with encrypted config: {}", e);
            0
        }
        Ok(a) => a.metadata()?.len(),
    };

    if file_size == 0 {
        wait_for_init(config).await;
    }
    // let crypto_config = KeyData::from_file(&config.pem_location)?;

    // serve().await;
    Ok(())
}
