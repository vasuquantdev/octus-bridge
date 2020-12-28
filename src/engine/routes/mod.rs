use bip39::Language;
use tokio::sync::oneshot::Receiver;
use tokio::sync::RwLock;
use url::Url;
use warp::http::StatusCode;
use warp::{reply, Filter, Reply};

use relay_eth::ws::{update_height, EthListener};
use relay_ton::contracts::{self, BridgeContract};
use relay_ton::transport::Transport;

use crate::config::{RelayConfig, TonConfig};
use crate::crypto::key_managment::KeyData;
use crate::crypto::recovery::*;
use crate::engine::bridge::Bridge;
use crate::engine::models::*;
use crate::engine::routes::api::get_api;
use crate::prelude::*;

mod api;
mod status;

pub async fn serve(config: RelayConfig, state: Arc<RwLock<State>>, signal_handler: Receiver<()>) {
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
        .and(warp::post())
        .and(json_data::<Password>())
        .and(state.clone())
        .and_then(|data, (state, config)| wait_for_password(data, config, state));

    let status = warp::path!("status")
        .and(warp::get())
        .and(state.clone())
        .and_then(|(state, _)| status::get_status(state));

    let init = warp::path!("init")
        .and(warp::post())
        .and(json_data::<InitData>())
        .and(state.clone())
        .and_then(|data, (state, config)| wait_for_init(data, config, state));

    let rescan_from_block_eth = warp::path!("rescan_eth")
        .and(warp::post())
        .and(json_data::<RescanEthData>())
        .and(state.clone())
        .and_then(|data, (state, _)| set_eth_block_height(state, data));

    let get_event_configuration = warp::path!("event_configurations")
        .and(warp::get())
        .and(state.clone())
        .and_then(|(state, _)| get_event_configurations(state));

    let add_event_configuration = warp::path!("event_configurations")
        .and(warp::post())
        .and(json_data::<NewEventConfiguration>())
        .and(state.clone())
        .and_then(|data, (state, _)| start_voting_for_event_configuration(state, data));

    let vote_for_event_configuration = warp::path!("event_configurations" / "vote")
        .and(warp::post())
        .and(json_data::<Voting>())
        .and(state.clone())
        .and_then(|data, (state, _)| vote_for_event_configuration(state, data));

    let pending_transactions = warp::path!("status" / "pending")
        .and(warp::get())
        .and(state.clone())
        .and_then(|(state, _)| status::pending(state));

    let failed_transactions = warp::path!("status" / "failed")
        .and(warp::get())
        .and(state.clone())
        .and_then(|(state, _)| status::failed(state));

    let eth_transactions = warp::path!("status" / "eth")
        .and(warp::get())
        .and(state.clone())
        .and_then(|(state, _)| status::eth_queue(state));

    let relay_stats = warp::path!("status" / "relay")
        .and(warp::get())
        .and(state.clone())
        .and_then(|(state, _)| status::all_relay_stats(state));

    let retry_failed = warp::path!("status" / "failed" / "retry")
        .and(warp::get())
        .and(state.clone())
        .and_then(|(state, _)| status::retry_failed(state));

    let swagger = warp::path!("swagger.yaml").and(warp::get()).map(get_api);

    let routes = init
        .or(password)
        .or(status)
        .or(pending_transactions)
        .or(failed_transactions)
        .or(eth_transactions)
        .or(relay_stats)
        .or(rescan_from_block_eth)
        .or(get_event_configuration)
        .or(add_event_configuration)
        .or(retry_failed)
        .or(vote_for_event_configuration)
        .or(swagger);

    let server = warp::serve(routes);
    let (_, server) = server.bind_with_graceful_shutdown(serve_address, async {
        signal_handler.await.ok();
    });
    server.await;
}

async fn vote_for_event_configuration(
    state: Arc<RwLock<State>>,
    voting: Voting,
) -> Result<impl Reply, Infallible> {
    let (address, voting) = match <(_, _)>::try_from(voting) {
        Ok(voting) => voting,
        Err(err) => {
            log::error!("{}", err);
            return Ok(reply::with_status(err.to_string(), StatusCode::BAD_REQUEST));
        }
    };

    let state = state.write().await;
    let bridge = match &state.bridge_state {
        BridgeState::Running(bridge) => bridge,
        _ => {
            let err = "Bridge was not initialized";
            log::error!("{}", err);
            return Ok(reply::with_status(err.to_string(), StatusCode::BAD_REQUEST));
        }
    };

    Ok(
        match bridge
            .vote_for_new_event_configuration(&address, voting)
            .await
        {
            Ok(_) => reply::with_status(String::new(), StatusCode::OK),
            Err(err) => {
                let err = format!("Failed voting for new configuration event: {}", err);
                log::error!("{}", err);
                reply::with_status(err, StatusCode::INTERNAL_SERVER_ERROR)
            }
        },
    )
}

async fn get_event_configurations(state: Arc<RwLock<State>>) -> Result<impl Reply, Infallible> {
    let state = state.read().await;
    let bridge = match &state.bridge_state {
        BridgeState::Running(bridge) => bridge,
        _ => {
            let err = "Bridge was not initialized".to_owned();
            log::error!("{}", err);
            return Ok(reply::with_status(err, StatusCode::BAD_REQUEST));
        }
    };

    Ok(
        match bridge
            .get_event_configurations()
            .await
            .map(|configurations| {
                configurations
                    .into_iter()
                    .map(EventConfiguration::from)
                    .collect::<Vec<_>>()
            }) {
            Ok(event_configurations) => reply::with_status(
                serde_json::to_string(&event_configurations).expect("shouldn't fail"),
                StatusCode::OK,
            ),
            Err(err) => {
                let err = format!("Failed getting configuration events: {}", err);
                log::error!("{}", err);
                reply::with_status(err, StatusCode::INTERNAL_SERVER_ERROR)
            }
        },
    )
}

async fn start_voting_for_event_configuration(
    state: Arc<RwLock<State>>,
    new_configuration: NewEventConfiguration,
) -> Result<impl Reply, Infallible> {
    let new_configuration: contracts::models::NewEventConfiguration =
        match new_configuration.try_into() {
            Ok(configuration) => configuration,
            Err(err) => {
                log::error!("{}", err);
                return Ok(reply::with_status(err.to_string(), StatusCode::BAD_REQUEST));
            }
        };

    let state = state.write().await;
    let bridge = match &state.bridge_state {
        BridgeState::Running(bridge) => bridge,
        _ => {
            let err = "Bridge was not initialized".to_owned();
            log::error!("{}", err);
            return Ok(reply::with_status(err, StatusCode::BAD_REQUEST));
        }
    };

    Ok(
        match bridge
            .start_voting_for_new_event_configuration(new_configuration)
            .await
        {
            Ok(address) => reply::with_status(
                serde_json::to_string(&VotingAddress {
                    address: address.to_string(),
                })
                .expect("shouldn't fail"),
                StatusCode::OK,
            ),
            Err(err) => {
                log::error!(
                    "Failed starting voting for new configuration event: {:?}",
                    err
                );
                let err = format!(
                    "Failed starting voting for new configuration event: {}",
                    err
                );
                reply::with_status(err, StatusCode::INTERNAL_SERVER_ERROR)
            }
        },
    )
}

async fn set_eth_block_height(
    state: Arc<RwLock<State>>,
    height: RescanEthData,
) -> Result<impl Reply, Infallible> {
    let state = state.write().await;
    let db = state.state_manager.clone();
    Ok(match update_height(&db, height.block) {
        Ok(_) => {
            log::info!("Changed  eth scan height to {}", height.block);
            reply::with_status("OK".to_string(), StatusCode::OK)
        }
        Err(e) => {
            let err = format!("Failed changing eth scan height: {}", e);
            log::error!("{}", err);
            reply::with_status(err, StatusCode::INTERNAL_SERVER_ERROR)
        }
    })
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
        log::error!("{}", err);
        return Ok(reply::with_status(err, StatusCode::METHOD_NOT_ALLOWED));
    }

    let language = match Language::from_language_code(&data.language) {
        Some(a) => a,
        None => {
            let error = format!("Bad language code provided: {}.", &data.language);
            log::error!("{}", error);
            return Ok(reply::with_status(error, StatusCode::BAD_REQUEST));
        }
    };

    let eth_private_key = match derive_from_words_eth(language, &data.eth_seed) {
        Ok(a) => a,
        Err(e) => {
            let error = format!("Failed deriving from eth seed: {}", e);
            log::error!("{}", error);
            return Ok(reply::with_status(error, StatusCode::BAD_REQUEST));
        }
    };

    let ton_key_pair = match derive_from_words_ton(
        language,
        &data.ton_seed,
        config.ton_derivation_path.as_deref(),
    ) {
        Ok(a) => a,
        Err(e) => {
            let error = format!("Failed deriving from ton seed: {}", e);
            log::error!("{}", error);
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
            log::error!("{}", error);
            return Ok(reply::with_status(error, StatusCode::BAD_REQUEST));
        }
    };

    if let Err(e) = state.finalize(config, key_data).await {
        log::error!("Failed finalize relay state: {:?}", e);

        let error = format!("Failed finalize relay state: {}", e);
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
            log::error!("{}", error);
            return Ok(reply::with_status(error, StatusCode::BAD_REQUEST));
        }
    };

    if let Err(e) = state.finalize(config, key_data).await {
        log::error!("Failed finalize relay state: {:?}", e);

        let error = format!("Failed finalize relay state: {}", e);
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

    let ton_client = Arc::new(
        BridgeContract::new(transport.clone(), contract_address, key_data.ton.keypair()).await?,
    );

    let eth_client = Arc::new(
        EthListener::new(
            Url::parse(config.eth_node_address.as_str())
                .map_err(|e| Error::new(e).context("Bad url for eth_config provided"))?,
            state_manager.clone(),
            100 //todo move to config
        )
        .await?,
    );

    Ok(Arc::new(
        Bridge::new(
            key_data.eth,
            eth_client,
            ton_client,
            transport,
            config.ton_operation_timeouts.clone(),
            state_manager.clone(),
        )
        .await?,
    ))
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
