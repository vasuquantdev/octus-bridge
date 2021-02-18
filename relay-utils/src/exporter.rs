use std::convert::Infallible;
use std::fmt::Write;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use futures::future::Either;
use http::uri::PathAndQuery;
use tokio::sync::oneshot::Receiver;
use tokio::sync::{RwLock, RwLockWriteGuard};

const BUFFER_COUNT: usize = 2;

pub struct MetricsExporter {
    addr: SocketAddr,
    buffers: [RwLock<String>; BUFFER_COUNT],
    current_buffer: Arc<AtomicUsize>,
}

impl MetricsExporter {
    pub fn new(addr: SocketAddr) -> Arc<Self> {
        Arc::new(Self {
            addr,
            buffers: Default::default(),
            current_buffer: Arc::new(AtomicUsize::new(0)),
        })
    }

    pub async fn acquire_buffer<'a, 's>(&'s self) -> MetricsBuffer<'a>
    where
        's: 'a,
    {
        let next_buffer = (self.current_buffer.load(Ordering::Relaxed) + 1) % BUFFER_COUNT;
        let mut buffer_guard = self.buffers[next_buffer].write().await;
        buffer_guard.clear();
        MetricsBuffer {
            current_buffer: self.current_buffer.clone(),
            next_buffer,
            buffer_guard,
        }
    }

    pub async fn get_metrics(&self) -> String {
        self.buffers[self.current_buffer.load(Ordering::Relaxed)]
            .read()
            .await
            .clone()
    }

    pub async fn listen(self: Arc<Self>, path: PathAndQuery, shutdown_signal: Receiver<()>) {
        let server = hyper::Server::bind(&self.addr);

        let make_service = hyper::service::make_service_fn(move |_| {
            let exporter = self.clone();
            let path = path.clone();

            async {
                Ok::<_, Infallible>(hyper::service::service_fn(move |req| {
                    if req.method() != hyper::Method::GET || req.uri() != path.as_str() {
                        return Either::Left(futures::future::ready(
                            hyper::Response::builder()
                                .status(hyper::StatusCode::NOT_FOUND)
                                .body(hyper::Body::empty()),
                        ));
                    }

                    let exporter = exporter.clone();

                    Either::Right(async move {
                        let data = exporter.get_metrics().await;

                        hyper::Response::builder()
                            .header("Content-Type", "text/plain; charset=UTF-8")
                            .body(hyper::Body::from(data))
                    })
                }))
            }
        });

        let server = server.serve(make_service).with_graceful_shutdown(async {
            shutdown_signal.await.ok();
        });

        if let Err(e) = server.await {
            log::error!("{:?}", e);
        }
    }
}

pub struct MetricsBuffer<'a> {
    current_buffer: Arc<AtomicUsize>,
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
            .store(self.next_buffer, Ordering::Relaxed);
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

pub trait ToLabels {
    fn fmt<'a, 'b>(&self, fmt: LabelsFormatter<'a, 'b>) -> LabelsFormatter<'a, 'b>;
}

pub struct PrometheusFormatter<'a, 'b> {
    fmt: &'a mut std::fmt::Formatter<'b>,
    result: std::fmt::Result,
    has_labels: bool,
}

pub struct LabelsFormatter<'a, 'b>(PrometheusFormatter<'a, 'b>);

impl<'a, 'b> LabelsFormatter<'a, 'b> {
    #[inline]
    pub fn label<N, V>(self, name: N, value: V) -> Self
    where
        N: std::fmt::Display,
        V: std::fmt::Display,
    {
        Self(self.0.label(name, value))
    }

    #[inline]
    pub fn labels<T>(self, object: &T) -> Self
    where
        T: ToLabels + 'b,
    {
        object.fmt(self)
    }

    #[inline]
    fn finish(self) -> PrometheusFormatter<'a, 'b> {
        self.0
    }
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
    pub fn labels<T>(self, object: &T) -> Self
    where
        T: ToLabels + 'b,
    {
        object.fmt(LabelsFormatter(self)).finish()
    }

    #[inline]
    pub fn value<T>(self, value: T) -> std::fmt::Result
    where
        T: std::fmt::Display,
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

#[cfg(test)]
mod tests {
    use super::*;

    struct StrangeMetrics {
        now: u64,
    }

    impl std::fmt::Display for StrangeMetrics {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.begin_metric("strange_metrics")
                .label("some", "label")
                .value(self.now)
        }
    }

    #[tokio::test]
    async fn test_exporter() {
        let exporter = MetricsExporter::new("0.0.0.0:9090".parse().unwrap());

        tokio::spawn(exporter.clone().listen());

        for now in 0..5 {
            let mut buffer = exporter.acquire_buffer().await;

            buffer.write(StrangeMetrics { now });

            tokio::time::delay_for(tokio::time::Duration::from_secs(10)).await;
        }
    }
}
