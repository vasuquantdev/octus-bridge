use std::convert::Infallible;
use std::fmt::Write;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use futures::future::Either;
use tokio::sync::{Mutex, Notify, RwLock, RwLockWriteGuard};
use tokio_util::sync::CancellationToken;

use crate::config::*;

const BUFFER_COUNT: usize = 2;

// Prometheus metrics exporter
pub struct MetricsExporter {
    /// Shared exporter state
    handle: Arc<MetricsExporterHandle>,

    /// Triggers and signals for running exporter service
    running_endpoint: Mutex<Option<RunningEndpoint>>,

    completion_signal: CancellationToken,
}

impl MetricsExporter {
    pub async fn with_config(config: Option<MetricsConfig>) -> Result<Arc<Self>> {
        let exporter = Arc::new(Self {
            handle: Default::default(),
            running_endpoint: Default::default(),
            completion_signal: Default::default(),
        });

        exporter
            .reload(config)
            .await
            .context("Failed to create metrics exporter")?;

        Ok(exporter)
    }

    pub fn handle(&self) -> &Arc<MetricsExporterHandle> {
        &self.handle
    }

    pub async fn reload(&self, config: Option<MetricsConfig>) -> Result<()> {
        let mut running_endpoint = self.running_endpoint.lock().await;

        // Stop running service
        if let Some(endpoint) = running_endpoint.take() {
            // Initiate completion
            endpoint.local_completion_signal.cancel();
            // And wait until it stops completely
            endpoint.stopped_signal.cancelled().await;
        }

        let config = match config {
            Some(config) => config,
            None => {
                log::info!("Disable metrics exporter");
                self.handle.interval_sec.store(0, Ordering::Release);
                self.handle.new_config_notify.notify_waiters();
                return Ok(());
            }
        };

        // Create http service
        let server = hyper::Server::try_bind(&config.listen_address)
            .context("Failed to bind metrics exporter server port")?;

        let path = config.metrics_path.clone();
        let buffers = self.handle.buffers.clone();

        let make_service = hyper::service::make_service_fn(move |_| {
            let path = path.clone();
            let buffers = buffers.clone();

            async move {
                Ok::<_, Infallible>(hyper::service::service_fn(move |req| {
                    // Allow only GET metrics_path
                    if req.method() != hyper::Method::GET || req.uri() != path.as_str() {
                        return Either::Left(futures::future::ready(
                            hyper::Response::builder()
                                .status(hyper::StatusCode::NOT_FOUND)
                                .body(hyper::Body::empty()),
                        ));
                    }

                    let buffers = buffers.clone();

                    // Prepare metrics response
                    Either::Right(async move {
                        let data = buffers.get_metrics().await;
                        hyper::Response::builder()
                            .header("Content-Type", "text/plain; charset=UTF-8")
                            .body(hyper::Body::from(data))
                    })
                }))
            }
        });

        let stopped_signal = CancellationToken::new();
        let local_completion_signal = CancellationToken::new();

        log::info!("Metrics exporter started");

        // Spawn server
        tokio::spawn({
            // Use completion signal as graceful shutdown notify
            let completion_signal = self.completion_signal.clone();
            let stopped_signal = stopped_signal.clone();
            let local_completion_signal = local_completion_signal.clone();

            async move {
                let server = server
                    .serve(make_service)
                    .with_graceful_shutdown(async move {
                        tokio::pin!(
                            let completion_signal = completion_signal.cancelled();
                            let local_completion_signal = local_completion_signal.cancelled();
                        );
                        futures::future::select(completion_signal, local_completion_signal).await;
                    });

                if let Err(e) = server.await {
                    log::error!("Metrics exporter stopped: {:?}", e);
                } else {
                    log::info!("Metrics exporter stopped");
                }

                // Notify when server is stopped
                stopped_signal.cancel();
            }
        });

        // Update running endpoint
        *running_endpoint = Some(RunningEndpoint {
            local_completion_signal,
            stopped_signal,
        });

        // Update interval and notify waiters
        self.handle
            .interval_sec
            .store(config.collection_interval_sec, Ordering::Release);
        self.handle.new_config_notify.notify_waiters();

        // Done
        Ok(())
    }
}

impl Drop for MetricsExporter {
    fn drop(&mut self) {
        // Trigger server shutdown on drop
        self.completion_signal.cancel();
    }
}

#[derive(Default)]
pub struct MetricsExporterHandle {
    buffers: Arc<Buffers>,
    interval_sec: AtomicU64,
    new_config_notify: Notify,
}

impl MetricsExporterHandle {
    pub fn buffers(&self) -> &Arc<Buffers> {
        &self.buffers
    }

    pub async fn wait(&self) {
        loop {
            // Start waiting config change
            let new_config = self.new_config_notify.notified();
            // Load current interval
            let current_interval = self.interval_sec.load(Ordering::Acquire);

            // Zero interval means that there is no current config and we should not
            // do anything but waiting new value
            if current_interval == 0 {
                new_config.await;
                continue;
            }

            tokio::select! {
                // Wait current interval
                _ = tokio::time::sleep(Duration::from_secs(current_interval)) => return,
                // Or resolve earlier on new non-zero interval
                _ = new_config => {
                    if self.interval_sec.load(Ordering::Acquire) > 0 {
                        return
                    } else {
                        // Wait for the new config otherwise
                        continue
                    }
                },
            }
        }
    }
}

struct RunningEndpoint {
    local_completion_signal: CancellationToken,
    stopped_signal: CancellationToken,
}

#[derive(Default)]
pub struct Buffers {
    data: [RwLock<String>; BUFFER_COUNT],
    current_buffer: AtomicUsize,
}

impl Buffers {
    pub async fn acquire_buffer<'a, 's>(&'s self) -> MetricsBuffer<'a>
    where
        's: 'a,
    {
        let next_buffer = (self.current_buffer.load(Ordering::Acquire) + 1) % BUFFER_COUNT;
        let mut buffer_guard = self.data[next_buffer].write().await;
        buffer_guard.clear();
        MetricsBuffer {
            current_buffer: &self.current_buffer,
            next_buffer,
            buffer_guard,
        }
    }

    async fn get_metrics(&self) -> String {
        self.data[self.current_buffer.load(Ordering::Acquire)]
            .read()
            .await
            .clone()
    }
}

pub struct MetricsBuffer<'a> {
    current_buffer: &'a AtomicUsize,
    next_buffer: usize,
    buffer_guard: RwLockWriteGuard<'a, String>,
}

impl<'a> MetricsBuffer<'a> {
    pub fn write<T>(&mut self, metrics: T)
    where
        T: std::fmt::Display,
    {
        self.buffer_guard.push_str(&metrics.to_string())
    }
}

impl<'a> Drop for MetricsBuffer<'a> {
    fn drop(&mut self) {
        self.current_buffer
            .store(self.next_buffer, Ordering::Release);
    }
}

pub trait DisplayPrometheusExt<'b> {
    fn begin_metric<'a>(&'a mut self, name: &str) -> PrometheusFormatter<'a, 'b>;
}

impl<'b> DisplayPrometheusExt<'b> for std::fmt::Formatter<'b> {
    fn begin_metric<'a>(&'a mut self, name: &str) -> PrometheusFormatter<'a, 'b> {
        PrometheusFormatter::new(self, name)
    }
}

pub struct PrometheusFormatter<'a, 'b> {
    fmt: &'a mut std::fmt::Formatter<'b>,
    result: std::fmt::Result,
    has_labels: bool,
}

impl<'a, 'b> PrometheusFormatter<'a, 'b> {
    pub fn new(fmt: &'a mut std::fmt::Formatter<'b>, name: &str) -> Self {
        let result = fmt.write_str(name);
        Self {
            fmt,
            result,
            has_labels: false,
        }
    }

    #[inline]
    pub fn label<N, V>(self, name: N, value: V) -> Self
    where
        N: std::fmt::Display,
        V: std::fmt::Display,
    {
        let PrometheusFormatter {
            fmt,
            result,
            has_labels,
        } = self;

        let result = result.and_then(|_| {
            fmt.write_char(if has_labels { ',' } else { '{' })?;
            name.fmt(fmt)?;
            fmt.write_str("=\"")?;
            value.fmt(fmt)?;
            fmt.write_char('\"')
        });

        Self {
            fmt,
            result,
            has_labels: true,
        }
    }

    #[inline]
    pub fn value<T>(self, value: T) -> std::fmt::Result
    where
        T: num_traits::Num + std::fmt::Display,
    {
        self.result.and_then(|_| {
            if self.has_labels {
                self.fmt.write_str("} ")?;
            } else {
                self.fmt.write_char(' ')?;
            }
            value.fmt(self.fmt)?;
            self.fmt.write_char('\n')
        })
    }
}
