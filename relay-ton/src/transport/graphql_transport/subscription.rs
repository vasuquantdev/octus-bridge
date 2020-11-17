use std::pin::Pin;
use std::task::Waker;

use futures::future::Future;
use futures::task::{Context, Poll};
use parking_lot::Mutex;
use pin_project::pin_project;
use serde_json::Value;
use tokio::sync::oneshot;
use ton_client::net;

use crate::prelude::*;
use crate::transport::errors::*;

pub struct SubscriptionHandle(Arc<Mutex<SubscriptionState>>);

impl Drop for SubscriptionHandle {
    fn drop(&mut self) {
        println!("dropping");
        let mut subscription = self.0.lock();
        subscription.running = false;
        println!("{}", subscription.waker.is_some());
        if let Some(waker) = subscription.waker.take() {
            println!("Stop waker");
            waker.wake();
        }
    }
}

struct SubscriptionState {
    running: bool,
    waker: Option<Waker>,
}

pub fn subscribe<F, T>(
    node_client: &NodeClient,
    table: &str,
    filter: &str,
    fields: &str,
    mut predicate: F,
) -> TransportResult<(SubscriptionHandle, impl Stream<Item = TransportResult<T>>)>
where
    F: FnMut(Value) -> TransportResult<T> + Send,
    T: Send,
{
    use tokio::stream::StreamExt;

    let stream = node_client
        .subscribe(table, filter, fields)
        .map_err(|e| TransportError::ApiFailure {
            reason: e.to_string(),
        })?
        .map(move |item| {
            println!("Got value: {:?}", item);
            let item = item.map_err(|e| TransportError::ApiFailure {
                reason: e.to_string(),
            })?;
            predicate(item)
        });

    let receiver_state = Arc::new(Mutex::new(SubscriptionState {
        running: true,
        waker: None,
    }));

    let handle = SubscriptionHandle(receiver_state.clone());
    let receiver = EventRx {
        response_rx: stream,
        state: receiver_state,
    };

    Ok((handle, receiver))
}

#[pin_project]
pub struct EventRx<T, RS>
where
    RS: Stream<Item = T> + Send,
{
    #[pin]
    response_rx: RS,
    state: Arc<Mutex<SubscriptionState>>,
}

impl<T, RS> futures::Stream for EventRx<T, RS>
where
    RS: Stream<Item = T> + Send,
{
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        poll_event(&this.state, this.response_rx, cx)
    }
}

fn poll_event<T, RS>(
    state: &Arc<Mutex<SubscriptionState>>,
    response_rx: Pin<&mut RS>,
    cx: &mut Context<'_>,
) -> Poll<Option<T>>
where
    RS: Stream<Item = T> + Send,
{
    println!("Polled internal_rx");

    let polled = response_rx.poll_next(cx);
    println!("After polled internal_rx");

    let res = match polled {
        Poll::Ready(item) => {
            println!("got");
            if item.is_some() {
                println!("stopped");
                let mut shared_state = state.lock();
                shared_state.running = false;
                let _ = shared_state.waker.take();
            }
            Poll::Ready(item)
        }
        Poll::Pending => {
            println!("iter");
            let mut shared_state = state.lock();
            shared_state.waker = Some(cx.waker().clone());
            Poll::Pending
        }
    };

    println!("Finished poll");

    res
}
