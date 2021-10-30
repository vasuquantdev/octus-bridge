use std::convert::Infallible;
use std::fmt::Write;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use futures::future::Either;
use tiny_adnl::utils::*;
use tokio::sync::{RwLock, RwLockWriteGuard};

use crate::config::*;

const BUFFER_COUNT: usize = 2;

// Prometheus metrics exporter
pub struct MetricsExporter {
    buffers: Arc<Buffers>,
    settings: MetricsConfig,

    completion_trigger: Trigger,
    completion_signal: TriggerReceiver,
}

impl MetricsExporter {
    pub fn new(settings: MetricsConfig) -> Arc<Self> {
        let (completion_trigger, completion_signal) = trigger();

        Arc::new(Self {
            buffers: Arc::new(Default::default()),
            settings,
            completion_trigger,
            completion_signal,
        })
    }

    pub fn start(&self) {
        let server = hyper::Server::bind(&self.settings.listen_address);
        let path = self.settings.metrics_path.clone();

        // Use only weak reference in service to allow completion trigger execution
        // on `MetricsExporter` drop
        let buffers = Arc::downgrade(&self.buffers);

        let make_service = hyper::service::make_service_fn(move |_| {
            let buffers = buffers.clone();
            let path = path.clone();

            async {
                Ok::<_, Infallible>(hyper::service::service_fn(move |req| {
                    // Allow only GET metrics_path
                    if req.method() != hyper::Method::GET || req.uri() != path.as_str() {
                        return Either::Left(Either::Left(futures::future::ready(
                            hyper::Response::builder()
                                .status(hyper::StatusCode::NOT_FOUND)
                                .body(hyper::Body::empty()),
                        )));
                    }

                    let buffers = match buffers.upgrade() {
                        // Buffers are still alive
                        Some(buffers) => buffers,
                        // Buffers are already dropped
                        None => {
                            return Either::Left(Either::Right(futures::future::ready(
                                hyper::Response::builder()
                                    .status(hyper::StatusCode::INTERNAL_SERVER_ERROR)
                                    .body(hyper::Body::empty()),
                            )))
                        }
                    };

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

        // Spawn server
        tokio::spawn(async move {
            let server = server
                .serve(make_service)
                .with_graceful_shutdown(completion_signal);

            if let Err(e) = server.await {
                log::error!("Metrics exporter stopped: {:?}", e);
            }
        });
    }

    pub fn interval(&self) -> Duration {
        Duration::from_secs(self.settings.collection_interval_sec)
    }

    pub fn buffers(&self) -> &Arc<Buffers> {
        &self.buffers
    }
}

impl Drop for MetricsExporter {
    fn drop(&mut self) {
        self.completion_trigger.trigger();
    }
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
