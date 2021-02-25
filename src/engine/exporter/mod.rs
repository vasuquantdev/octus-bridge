use relay_utils::exporter::*;
use tokio::sync::oneshot;

use crate::config::RelayConfig;
use crate::engine::models::*;
use crate::models::{BridgeMetrics, RelayMetrics, LABEL_ADDRESS};
use crate::prelude::*;

pub async fn serve(
    config: RelayConfig,
    state: Arc<RwLock<State>>,
    shutdown_signal: oneshot::Receiver<()>,
) {
    let relay_contract_address = config.ton_settings.relay_contract_address.0;
    let settings = match config.metrics_settings {
        Some(address) => address,
        None => return,
    };

    log::info!("Starting exporter");

    let exporter = MetricsExporter::new(settings.listen_address);
    let collection_interval = settings.collection_interval;

    let (stop_tx, mut stop_rx) = oneshot::channel();
    tokio::spawn({
        let state = Arc::downgrade(&state);
        let exporter = exporter.clone();

        async move {
            loop {
                if matches!(stop_rx.try_recv(), Ok(_) | Err(oneshot::error::TryRecvError::Closed)) {
                    return;
                }

                log::trace!("Collecting metrics");

                let metrics = {
                    let state = match state.upgrade() {
                        Some(state) => state,
                        None => return,
                    };

                    let state = state.read().await;
                    match &state.bridge_state {
                        BridgeState::Uninitialized => {
                            MetricsState::Uninitialized(&relay_contract_address)
                        }
                        BridgeState::Locked => MetricsState::Locked(&relay_contract_address),
                        BridgeState::Running(bridge) => MetricsState::Running(
                            &relay_contract_address,
                            bridge.get_metrics().await,
                        ),
                    }
                };

                exporter.acquire_buffer().await.write(metrics);

                tokio::time::delay_for(collection_interval).await;
            }
        }
    });

    exporter
        .listen(settings.metrics_path, shutdown_signal)
        .await;
    stop_tx.send(()).expect("Failed to stop metrics exporter");
}

enum MetricsState<'a> {
    Uninitialized(&'a str),
    Locked(&'a str),
    Running(&'a str, BridgeMetrics),
}

impl std::fmt::Display for MetricsState<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state_metric = f.begin_metric("state");
        match self {
            MetricsState::Uninitialized(address) => {
                state_metric.label(LABEL_ADDRESS, address).value(0)
            }
            MetricsState::Locked(address) => state_metric.label(LABEL_ADDRESS, address).value(1),
            MetricsState::Running(address, metrics) => {
                state_metric.label(LABEL_ADDRESS, address).value(2)?;
                std::fmt::Display::fmt(&RelayMetrics { address, metrics }, f)
            }
        }
    }
}
