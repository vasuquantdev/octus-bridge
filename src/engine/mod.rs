use crate::config::RelayConfig;
use crate::crypto::key_managment::KeyData;
use crate::crypto::{key_managment, recovery};
use crate::storage;
use anyhow::Error;
use futures::{future, FutureExt};
use log::{warn, info};
use machine::machine;
use relay_eth::ws::EthConfig;
use secstr::SecStr;
use serde::Deserialize;
use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::ops::Deref;
use tokio::sync::Mutex;
use tokio::time::Duration;
use url::Url;
use warp::Filter;
use recovery::derive_from_words;
use bip39::Language;
use warp::reject::Rejection;

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
    eth_seed: Vec<String>,
    password: String,
    language: String
}

async fn serve(key_data: Option<KeyData>) {
    #[derive(Deserialize, Debug)]
    struct Password {
        password: String,
    }
    let json_data = warp::body::content_length_limit(1024 * 1024).and(warp::filters::body::json());

    let init = warp::path!("init")
        .and(json_data)
        .map(|data: InitData| format!("{:?}", &data));
    // let password = warp::path("unlock")
    //     .and(json_data)
    //     .map(|data: Password|{
    //         let
    //     });
    // );

    let routes = warp::post().and(init);
    let addr: SocketAddr = "127.0.0.1:12345".parse().unwrap();
    warp::serve(routes).run(addr).await;
}

async fn wait_for_init(config: &RelayConfig){
    let init = warp::path!("init")
        .and(json_data)
        .map(|data: InitData|{
            info!("Recieved init data");
            let language = match  Language::from_language_code(&data.language){
                Some(a) =>a,
                None =>return Err(warp::reject::custom("Bad language code"))
            };

            // let key = derive_from_words()
            KeyData::init(&config.pem_location, data.password.into(),)
        }
        );
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
    if file_size == 0{

    }
    // let crypto_config = KeyData::from_file(&config.pem_location)?;

    // serve().await;
    Ok(())
}
