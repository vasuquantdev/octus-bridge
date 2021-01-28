use bip39::Language;
use tokio::sync::oneshot::Receiver;
use tokio::sync::RwLock;
use warp::http::StatusCode;
use warp::{reply, Filter, Reply};

use relay_models::models::*;
use relay_ton::contracts::{BridgeConfiguration, EthEventVoteData, TonEventVoteData, VoteData};
use relay_ton::transport::Transport;

use crate::config::{RelayConfig, TonTransportConfig};
use crate::crypto::key_managment::KeyData;
use crate::crypto::recovery::*;
use crate::engine::models::*;
use crate::engine::routes::api::get_api;
use crate::models::SignedTonEventVoteData;
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

    let swagger = warp::path!("swagger.yaml").and(warp::get()).map(get_api);

    let init = warp::path!("init")
        .and(warp::post())
        .and(json_data::<InitData>())
        .and(state.clone())
        .and_then(|data, (state, config)| wait_for_init(data, config, state));

    let unlock = warp::path!("unlock")
        .and(warp::post())
        .and(json_data::<Password>())
        .and(state.clone())
        .and_then(|data, (state, config)| wait_for_password(data, config, state));

    let retry_failed = warp::path!("retry-failed")
        .and(warp::post())
        .and(state.clone())
        .and_then(|(state, _)| status::retry_failed(state));

    let rescan_eth = warp::path!("rescan-eth")
        .and(warp::post())
        .and(json_data::<RescanEthData>())
        .and(state.clone())
        .and_then(|data, (state, _)| set_eth_block_height(state, data));

    let status = warp::path!("status")
        .and(warp::get())
        .and(state.clone())
        .and_then(|(state, _)| status::get_status(state));

    let get_all_event_configurations = warp::path!("event-configurations")
        .and(warp::get())
        .and(state.clone())
        .and_then(|(state, _)| get_event_configurations(state));

    // TODO: add request for getting event configuration by id

    let create_event_configuration = warp::path!("event-configurations")
        .and(warp::post())
        .and(json_data::<NewEventConfiguration>())
        .and(state.clone())
        .and_then(|data, (state, _)| create_event_configuration(state, data));

    let vote_for_ethereum_event_configuration = warp::path!("event-configurations" / "vote")
        .and(warp::post())
        .and(json_data::<Voting>())
        .and(state.clone())
        .and_then(|data, (state, _)| vote_for_ethereum_event_configuration(state, data));

    let pending_transactions_eth_to_ton = warp::path!("eth-to-ton" / "pending")
        .and(warp::get())
        .and(state.clone())
        .and_then(|(state, _)| {
            status::pending::<EthEventVoteData, EthEventVoteData, EthTonTransactionView>(state)
        });

    let failed_transactions_eth_to_ton = warp::path!("eth-to-ton" / "failed")
        .and(warp::get())
        .and(state.clone())
        .and_then(|(state, _)| {
            status::failed::<EthEventVoteData, EthEventVoteData, EthTonTransactionView>(state)
        });

    let queued_transactions_eth_to_ton = warp::path!("eth-to-ton" / "queued")
        .and(warp::get())
        .and(state.clone())
        .and_then(|(state, _)| status::eth_queue(state));

    let eth_relay_stats = warp::path!("eth-to-ton" / "stats")
        .and(warp::get())
        .and(state.clone())
        .and_then(|(state, _)| status::eth_relay_stats(state));

    let pending_transactions_ton_to_eth = warp::path!("ton-to-eth" / "pending")
        .and(warp::get())
        .and(state.clone())
        .and_then(|(state, _)| {
            status::pending::<SignedTonEventVoteData, TonEventVoteData, TonEthTransactionView>(
                state,
            )
        });

    let failed_transactions_ton_to_eth = warp::path!("ton-to-eth" / "failed")
        .and(warp::get())
        .and(state.clone())
        .and_then(|(state, _)| {
            status::failed::<SignedTonEventVoteData, TonEventVoteData, TonEthTransactionView>(state)
        });

    let queued_transactions_ton_to_eth = warp::path!("ton-to-eth" / "queued" / u64)
        .and(warp::get())
        .and(state.clone())
        .and_then(|configuration_id, (state, _)| status::ton_queue(state, configuration_id));

    let ton_relay_stats = warp::path!("ton-to-eth" / "stats")
        .and(warp::get())
        .and(state.clone())
        .and_then(|(state, _)| status::ton_relay_stats(state));

    let update_bridge_configuration = warp::path!("update_bridge_configuration")
        .and(warp::post())
        .and(state.clone())
        .and(json_data::<BridgeConfigurationView>())
        .and_then(|(state, _), data| update_bridge_configuration(state, data));

    let routes = swagger
        .or(init)
        .or(unlock)
        .or(retry_failed)
        .or(rescan_eth)
        .or(status)
        .or(get_all_event_configurations)
        .or(create_event_configuration)
        .or(vote_for_ethereum_event_configuration)
        .or(pending_transactions_eth_to_ton)
        .or(failed_transactions_eth_to_ton)
        .or(queued_transactions_eth_to_ton)
        .or(eth_relay_stats)
        .or(pending_transactions_ton_to_eth)
        .or(failed_transactions_ton_to_eth)
        .or(queued_transactions_ton_to_eth)
        .or(ton_relay_stats)
        .or(update_bridge_configuration);

    let server = warp::serve(routes);
    let (_, server) = server.bind_with_graceful_shutdown(serve_address, async {
        signal_handler.await.ok();
    });
    server.await;
}

pub async fn update_bridge_configuration(
    state: Arc<RwLock<State>>,
    data: BridgeConfigurationView,
) -> Result<impl Reply, Infallible> {
    use ethabi::{Token, Uint};
    let state = state.read().await;
    if let BridgeState::Running(a) = &state.bridge_state {
        log::info!("Got request for updating bridge contract");

        let bridge_conf = BridgeConfiguration {
            event_configuration_required_confirmations: data
                .event_configuration_required_confirmations,
            event_configuration_required_rejections: data.event_configuration_required_rejections,
            bridge_configuration_update_required_confirmations: data
                .bridge_configuration_update_required_confirmations,
            bridge_configuration_update_required_rejections: data
                .bridge_configuration_update_required_rejections,
            bridge_relay_update_required_confirmations: data
                .bridge_relay_update_required_confirmations,
            bridge_relay_update_required_rejections: data.bridge_relay_update_required_rejections,
            active: data.active,
        };

        let tokens = [
            Token::Uint(Uint::from(
                bridge_conf.event_configuration_required_confirmations,
            )),
            Token::Uint(Uint::from(
                bridge_conf.event_configuration_required_rejections,
            )),
            Token::Uint(Uint::from(
                bridge_conf.bridge_configuration_update_required_confirmations,
            )),
            Token::Uint(Uint::from(
                bridge_conf.bridge_configuration_update_required_rejections,
            )),
            Token::Uint(Uint::from(
                bridge_conf.bridge_relay_update_required_confirmations,
            )),
            Token::Uint(Uint::from(
                bridge_conf.bridge_relay_update_required_rejections,
            )),
            Token::Bool(bridge_conf.active),
        ];
        //todo check packing and correctness
        let mut eth_bytes = ethabi::encode(&tokens);
        let mut signature = a.sign_with_eth_key(&*eth_bytes);
        eth_bytes.append(&mut signature);
        let vote_data = VoteData {
            signature: eth_bytes,
        };
        if let Err(e) = a.update_bridge_configuration(bridge_conf, vote_data).await {
            let message = format!("Failed updating bridge configuration: {}", e);
            log::error!("{}", &message);
            return Ok(reply::with_status(
                message,
                StatusCode::INTERNAL_SERVER_ERROR,
            ));
        }
        return Ok(reply::with_status("ok".to_string(), StatusCode::OK));
    } else {
        return Ok(reply::with_status(
            "Bridge is not running".to_string(),
            StatusCode::METHOD_NOT_ALLOWED,
        ));
    }
}

async fn vote_for_ethereum_event_configuration(
    state: Arc<RwLock<State>>,
    voting: Voting,
) -> Result<impl Reply, Infallible> {
    let (configuration_id, voting) = match FromRequest::<_>::try_from(voting) {
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
            .vote_for_ethereum_event_configuration(configuration_id, voting)
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
                    .map(<EventConfiguration as FromContractModels<_>>::from)
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

async fn create_event_configuration(
    state: Arc<RwLock<State>>,
    data: NewEventConfiguration,
) -> Result<impl Reply, Infallible> {
    let (configuration_id, address, event_type) = match FromRequest::<_>::try_from(data) {
        Ok(data) => data,
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
            .create_event_configuration(configuration_id, address, event_type)
            .await
        {
            Ok(()) => reply::with_status(String::new(), StatusCode::OK),
            Err(err) => {
                let err = format!("Failed getting configuration events: {}", err);
                log::error!("{}", err);
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
    Ok(match &state.bridge_state {
        BridgeState::Uninitialized => {
            log::info!("Trying to change ethereum scan height on uninitialized relay");
            reply::with_status(
                "Trying to change ethereum scan height on uninitialized relay".to_string(),
                StatusCode::FORBIDDEN,
            )
        }
        BridgeState::Locked => {
            log::info!("Trying to change ethereum scan height on locked relay");
            reply::with_status(
                "Trying to change ethereum scan height on locked relay".to_string(),
                StatusCode::FORBIDDEN,
            )
        }
        BridgeState::Running(bridge) => match bridge.change_eth_height(height.block).await {
            Ok(_) => {
                log::info!("Changed  eth scan height to {}", height.block);
                reply::with_status("OK".to_string(), StatusCode::OK)
            }
            Err(e) => {
                let err = format!("Failed changing eth scan height: {}", e);
                log::error!("{}", err);
                reply::with_status(err, StatusCode::INTERNAL_SERVER_ERROR)
            }
        },
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

    let eth_private_key = match derive_from_words_eth(
        language,
        &data.eth_seed,
        data.eth_derivation_path.as_deref(),
    ) {
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
        data.ton_derivation_path.as_deref(),
    ) {
        Ok(a) => a,
        Err(e) => {
            let error = format!("Failed deriving from ton seed: {}", e);
            log::error!("{}", error);
            return Ok(reply::with_status(error, StatusCode::BAD_REQUEST));
        }
    };

    let key_data = match KeyData::init(
        &config.keys_path,
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

    let key_data = match KeyData::from_file(config.keys_path.clone(), data.password.into()) {
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

impl TonTransportConfig {
    pub async fn make_transport(&self) -> Result<Arc<dyn Transport>, Error> {
        #[allow(unreachable_code)]
        Ok(match self {
            #[cfg(feature = "tonlib-transport")]
            TonTransportConfig::Tonlib(config) => {
                Arc::new(relay_ton::transport::TonlibTransport::new(config.clone()).await?)
            }
            #[cfg(feature = "graphql-transport")]
            TonTransportConfig::GraphQL(config) => {
                Arc::new(relay_ton::transport::GraphQLTransport::new(config.clone()).await?)
            }
        })
    }
}
