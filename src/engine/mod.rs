use crate::config::RelayConfig;
use crate::crypto::{key_managment, recovery};
use crate::storage;
use anyhow::Error;
use futures::{future, FutureExt};
use relay_eth::ws::EthConfig;
use serde::Deserialize;
use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use tokio::time::Duration;
use url::Url;
use warp::Filter;

async fn serve() {
    #[derive(Deserialize, Debug)]
    struct InitData {
        ton_seed: Vec<String>,
        eth_seed: Vec<String>,
        password: String,
    }


    let init = warp::path!("init")
        .and(warp::body::content_length_limit(1024 * 1024))
        .and(warp::filters::body::json())
        .map(|data: InitData| {
            format!("{:?}", &data)
        });

    let routes = warp::post().and(init);
    let addr: SocketAddr = "127.0.0.1:12345".parse().unwrap();
    // let hi = warp::path("hello")
    //     .and(warp::path::param())
    //     .and(warp::header("user-agent"))
    //     .map(|param: String, agent: String| format!("Hello {}, whose agent is {}", param, agent));
    warp::serve(routes).run(addr).await;
}

pub async fn run(config: RelayConfig) -> Result<(), Error> {
    let eth_config = EthConfig::new(
        Url::parse(config.eth_node_address.as_str())
            .map_err(|e| Error::new(e).context("Bad url for eth_config provided"))?,
    )
    .await;
    let state_manager = storage::StateManager::new(&config.storage_path)?;
    // let crypto_config = key_managment::KeyData::from_file();
    serve().await;
    Ok(())
}
