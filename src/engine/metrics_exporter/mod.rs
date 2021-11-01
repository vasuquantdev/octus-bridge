use std::convert::Infallible;
use std::fmt::Write;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use futures::future::Either;
use tiny_adnl::utils::*;
use tokio::sync::{Mutex, Notify, RwLock, RwLockWriteGuard};

use crate::config::*;

const BUFFER_COUNT: usize = 2;

// Prometheus metrics exporter
pub struct MetricsExporter {
    /// Shared exporter state
    handle: Arc<MetricsExporterHandle>,

    /// Triggers and signals for running exporter service
    running_endpoint: Mutex<Option<RunningEndpoint>>,

    completion_trigger: Trigger,
    completion_signal: TriggerReceiver,
}

impl MetricsExporter {
    pub async fn with_config(config: Option<MetricsConfig>) -> Result<Arc<Self>> {
        let (completion_trigger, completion_signal) = trigger();

        let exporter = Arc::new(Self {
            handle: Default::default(),
            running_endpoint: Default::default(),
            completion_trigger,
            completion_signal,
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
            endpoint.completion_trigger.trigger();
            // And wait until it stops completely
            endpoint.stopped_signal.await;
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

        // Use completion signal as graceful shutdown notify
        let completion_signal = self.completion_signal.clone();
        let (stopped_trigger, stopped_signal) = trigger();
        let (local_completion_trigger, local_completion_signal) = trigger();

        // Spawn server
        tokio::spawn(async move {
            let server = server
                .serve(make_service)
                .with_graceful_shutdown(async move {
                    futures::future::select(completion_signal, local_completion_signal).await;
                });

            if let Err(e) = server.await {
                log::error!("Metrics exporter stopped: {:?}", e);
            } else {
                log::info!("Metrics exporter stopped");
            }

            // Notify when server is stopped
            stopped_trigger.trigger();
        });

        // Update running endpoint
        *running_endpoint = Some(RunningEndpoint {
            completion_trigger: local_completion_trigger,
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
        self.completion_trigger.trigger();
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
    completion_trigger: Trigger,
    stopped_signal: TriggerReceiver,
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
