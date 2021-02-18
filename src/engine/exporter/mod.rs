use relay_utils::exporter::*;
use tokio::sync::oneshot;

use crate::config::RelayConfig;
use crate::engine::models::*;
use crate::models::BridgeMetrics;
use crate::prelude::*;

pub async fn serve(
    config: RelayConfig,
    state: Arc<RwLock<State>>,
    shutdown_signal: oneshot::Receiver<()>,
) {
    let settings = match config.metrics_settings {
        Some(address) => address,
        None => return,
    };

    log::info!("Starting exporter");

    let exporter = MetricsExporter::new(settings.listen_address);

    let (stop_tx, mut stop_rx) = oneshot::channel();
    tokio::spawn({
        let state = Arc::downgrade(&state);
        let exporter = exporter.clone();

        async move {
            loop {
                if matches!(stop_rx.try_recv(), Ok(_) | Err(oneshot::error::TryRecvError::Closed)) {
                    return;
                }

                log::debug!("Collecting metrics");

                let metrics = {
                    let state = match state.upgrade() {
                        Some(state) => state,
                        None => return,
                    };

                    let state = state.read().await;
                    match &state.bridge_state {
                        BridgeState::Uninitialized => MetricsState::Uninitialized,
                        BridgeState::Locked => MetricsState::Locked,
                        BridgeState::Running(bridge) => {
                            MetricsState::Running(bridge.get_metrics().await)
                        }
                    }
                };

                exporter.acquire_buffer().await.write(metrics);

                tokio::time::delay_for(settings.collection_interval).await;
            }
        }
    });

    exporter.listen(shutdown_signal).await;
    stop_tx.send(()).expect("Failed to stop metrics exporter");
}

enum MetricsState {
    Uninitialized,
    Locked,
    Running(BridgeMetrics),
}

impl std::fmt::Display for MetricsState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state_metric = f.begin_metric("state");
        match self {
            MetricsState::Uninitialized => state_metric.value(0),
            MetricsState::Locked => state_metric.value(1),
            MetricsState::Running(metrics) => {
                state_metric.value(2)?;
                metrics.fmt(f)
            }
        }
    }
}
