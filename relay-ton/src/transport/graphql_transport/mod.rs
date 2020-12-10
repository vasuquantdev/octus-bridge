pub mod config;
mod node_client;

use std::collections::hash_map;
use std::pin::Pin;

use futures::task::{Context, Poll};
use futures::{Future, FutureExt};
use reqwest::header::{self, HeaderMap, HeaderValue};
use reqwest::ClientBuilder;
use ton_abi::Function;
use ton_block::{
    CommonMsgInfo, Deserializable, HashmapAugType, InRefValue, Message, Serializable, Transaction,
};
use ton_types::HashmapType;

pub use self::config::*;
use self::node_client::*;
use super::tvm;
use super::utils::*;
use crate::models::*;
use crate::prelude::*;
use crate::transport::errors::*;
use crate::transport::{AccountEvent, AccountSubscription, RunLocal, Transport};

pub struct GraphQLTransport {
    db: Db,
    client: NodeClient,
    config: Config,
}

impl GraphQLTransport {
    pub async fn new(config: Config, db: Db) -> TransportResult<Self> {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::CONTENT_TYPE,
            HeaderValue::from_str("application/json").unwrap(),
        );

        let client_builder = ClientBuilder::new().default_headers(headers);
        let client = client_builder
            .build()
            .expect("failed to create graphql client");

        let client = NodeClient::new(client, config.addr.clone());

        Ok(Self { db, client, config })
    }
}

#[async_trait]
impl RunLocal for GraphQLTransport {
    async fn run_local(
        &self,
        abi: &Function,
        message: ExternalMessage,
    ) -> TransportResult<ContractOutput> {
        run_local(&self.client, abi, message).await
    }
}

#[async_trait]
impl Transport for GraphQLTransport {
    async fn subscribe(
        &self,
        addr: MsgAddressInt,
    ) -> TransportResult<Arc<dyn AccountSubscription>> {
        let subscription: Arc<dyn AccountSubscription> = GraphQLAccountSubscription::new(
            self.db.clone(),
            self.client.clone(),
            None,
            self.config.next_block_timeout_sec,
            addr,
        )
        .await?;

        Ok(subscription)
    }

    fn rescan_events(
        &self,
        account: MsgAddressInt,
        since_lt: Option<u64>,
        until_lt: Option<u64>,
    ) -> BoxStream<TransportResult<SliceData>> {
        EventsScanner {
            account,
            client: &self.client,
            since_lt,
            until_lt,
            request_fut: None,
            messages: None,
            current_message: 0,
        }
        .boxed()
    }
}

struct GraphQLAccountSubscription {
    _db: Db,
    client: NodeClient,
    event_notifier: watch::Receiver<AccountEvent>,
    account: MsgAddressInt,
    account_id: UInt256,
    pending_messages: RwLock<HashMap<UInt256, PendingMessage<u32>>>,
}

impl GraphQLAccountSubscription {
    async fn new(
        db: Db,
        client: NodeClient,
        max_initial_rescan_gap: Option<u32>,
        next_block_timeout: u32,
        addr: MsgAddressInt,
    ) -> TransportResult<Arc<Self>> {
        let client = client.clone();
        let known_block_id = client.get_latest_block(&addr).await?;

        let (tx, rx) = watch::channel(AccountEvent::StateChanged);

        let subscription = Arc::new(Self {
            _db: db,
            client,
            event_notifier: rx,
            account: addr.clone(),
            account_id: addr
                .address()
                .get_slice(0, 256)
                .and_then(|mut slice| slice.get_next_bytes(32))
                .map_err(|e| TransportError::FailedToInitialize {
                    reason: e.to_string(),
                })?
                .into(),
            pending_messages: RwLock::new(HashMap::new()),
        });
        subscription.start_loop(
            tx,
            known_block_id,
            max_initial_rescan_gap,
            next_block_timeout,
        );

        Ok(subscription)
    }

    fn start_loop(
        self: &Arc<Self>,
        state_notifier: watch::Sender<AccountEvent>,
        mut last_block_id: String,
        _max_interval_rescan_gap: Option<u32>,
        next_block_timeout: u32,
    ) {
        let subscription = Arc::downgrade(self);

        log::debug!("started polling account {}", self.account);

        tokio::spawn(async move {
            let mut next_iteration_time = tokio::time::Instant::now();
            'subscription_loop: loop {
                tokio::time::delay_until(next_iteration_time).await;

                // Debounce api calls
                next_iteration_time =
                    tokio::time::Instant::now() + tokio::time::Duration::from_millis(100);

                let subscription = match subscription.upgrade() {
                    Some(s) => s,
                    None => return,
                };

                let next_block_id = match subscription
                    .client
                    .wait_for_next_block(&last_block_id, &subscription.account, next_block_timeout)
                    .await
                {
                    Ok(id) => id,
                    Err(e) => {
                        log::error!("failed to get next block id. {:?}", e);
                        if let TransportError::ApiFailure { .. } = e {
                            next_iteration_time =
                                tokio::time::Instant::now() + tokio::time::Duration::from_secs(1);
                        }
                        continue 'subscription_loop;
                    }
                };

                log::trace!("current_block: {}", next_block_id);

                let (block, block_info) = match subscription
                    .client
                    .get_block(&next_block_id)
                    .await
                    .and_then(|block| {
                        let info = block.info.read_struct().map_err(|e| {
                            TransportError::FailedToParseBlock {
                                reason: e.to_string(),
                            }
                        })?;
                        Ok((block, info))
                    }) {
                    Ok(block) => block,
                    Err(e) => {
                        log::error!("failed to get next block data. {:?}", e);
                        continue 'subscription_loop;
                    }
                };

                let mut pending_messages = subscription.pending_messages.write().await;

                match block
                    .extra
                    .read_struct()
                    .and_then(|extra| extra.read_account_blocks())
                    .and_then(|account_blocks| account_blocks.get(&subscription.account_id))
                {
                    Ok(Some(data)) => {
                        log::trace!("got account block. {:?}", data);

                        let _ = state_notifier.broadcast(AccountEvent::StateChanged);

                        for item in data.transactions().iter() {
                            let transaction = match item.and_then(|(_, mut value)| {
                                InRefValue::<Transaction>::construct_from(&mut value)
                            }) {
                                Ok(transaction) => transaction.0,
                                Err(e) => {
                                    log::error!(
                                        "failed to parse account transaction. {:?}",
                                        e.to_string()
                                    );
                                    continue 'subscription_loop;
                                }
                            };

                            let out_messages = match parse_transaction_messages(&transaction) {
                                Ok(messages) => messages,
                                Err(e) => {
                                    log::error!("error during transaction processing. {:?}", e);
                                    continue 'subscription_loop;
                                }
                            };

                            if let Some(in_msg) = &transaction.in_msg {
                                if let Some(pending_message) =
                                    pending_messages.remove(&in_msg.hash())
                                {
                                    log::debug!(
                                        "got message response for {} IN {}",
                                        pending_message.abi().name,
                                        subscription.account
                                    );

                                    let result = process_out_messages(
                                        &out_messages,
                                        MessageProcessingParams {
                                            abi_function: Some(pending_message.abi()),
                                            events_tx: Some(&state_notifier),
                                        },
                                    );
                                    pending_message.set_result(result);
                                } else if let Err(e) = process_out_messages(
                                    &out_messages,
                                    MessageProcessingParams {
                                        abi_function: None,
                                        events_tx: Some(&state_notifier),
                                    },
                                ) {
                                    log::error!("error during out messages processing. {:?}", e);
                                    // Just ignore
                                }
                            }
                        }
                    }
                    Ok(None) => {
                        log::trace!("account state wasn't changed");
                        last_block_id = next_block_id;
                        continue 'subscription_loop;
                    }
                    Err(e) => {
                        log::error!("failed to parse block data. {:?}", e.to_string());
                        continue 'subscription_loop;
                    }
                };

                pending_messages
                    .retain(|_, message| block_info.gen_utime().0 <= message.expires_at());

                last_block_id = next_block_id;
            }
        });
    }
}

#[async_trait]
impl RunLocal for GraphQLAccountSubscription {
    async fn run_local(
        &self,
        abi: &Function,
        message: ExternalMessage,
    ) -> TransportResult<ContractOutput> {
        run_local(&self.client, abi, message).await
    }
}

#[async_trait]
impl AccountSubscription for GraphQLAccountSubscription {
    fn events(&self) -> watch::Receiver<AccountEvent> {
        self.event_notifier.clone()
    }

    async fn send_message(
        &self,
        abi: Arc<Function>,
        message: ExternalMessage,
    ) -> TransportResult<ContractOutput> {
        if message.run_local {
            return self.run_local(abi.as_ref(), message).await;
        }

        let expires_at = message.header.expire;

        let cells = encode_external_message(message)
            .write_to_new_cell()
            .map_err(|_| TransportError::FailedToSerialize)?
            .into();

        let serialized =
            ton_types::serialize_toc(&cells).map_err(|_| TransportError::FailedToSerialize)?;
        let hash = cells.repr_hash();

        let (tx, rx) = oneshot::channel();
        {
            let mut pending_messages = self.pending_messages.write().await;
            match pending_messages.entry(hash.clone()) {
                hash_map::Entry::Vacant(entry) => {
                    self.client.send_message_raw(&hash, &serialized).await?;

                    entry.insert(PendingMessage::new(expires_at, abi, tx))
                }
                _ => {
                    return Err(TransportError::FailedToSendMessage {
                        reason: "duplicate message hash".to_string(),
                    });
                }
            };
        }

        rx.await.unwrap_or_else(|_| {
            Err(TransportError::ApiFailure {
                reason: "subscription part dropped before receiving message response".to_owned(),
            })
        })
    }

    fn rescan_events(
        &self,
        since_lt: Option<u64>,
        until_lt: Option<u64>,
    ) -> BoxStream<TransportResult<SliceData>> {
        EventsScanner {
            account: self.account.clone(),
            client: &self.client,
            since_lt,
            until_lt,
            request_fut: None,
            messages: None,
            current_message: 0,
        }
        .boxed()
    }
}

impl PendingMessage<u32> {
    pub fn expires_at(&self) -> u32 {
        *self.data()
    }
}

const MESSAGES_PER_SCAN_ITER: u32 = 50;

struct EventsScanner<'a> {
    account: MsgAddressInt,
    client: &'a NodeClient,
    since_lt: Option<u64>,
    until_lt: Option<u64>,
    request_fut: Option<BoxFuture<'static, TransportResult<MessagesResponse>>>,
    messages: Option<MessagesResponse>,
    current_message: usize,
}

impl<'a> EventsScanner<'a>
where
    Self: Stream<Item = TransportResult<SliceData>>,
{
    fn poll_request_fut<'c, F>(fut: Pin<&mut F>, cx: &mut Context<'c>) -> Poll<MessagesResponse>
    where
        F: Future<Output = TransportResult<MessagesResponse>> + ?Sized,
    {
        match fut.poll(cx) {
            Poll::Ready(Ok(new_messages)) => Poll::Ready(new_messages),
            Poll::Ready(Err(err)) => Poll::Ready(vec![(0, Err(err))]),
            Poll::Pending => Poll::Pending,
        }
    }

    fn handle_state<'c>(&mut self, cx: &mut Context<'c>) -> Poll<Option<<Self as Stream>::Item>> {
        'outer: loop {
            match (&mut self.messages, &mut self.request_fut) {
                (Some(messages), _) if self.current_message < messages.len() => {
                    let (lt, result) = &messages[self.current_message];
                    self.until_lt = Some(*lt);

                    self.current_message += 1;

                    if matches!(self.since_lt.as_ref(), Some(since_lt) if lt < since_lt) {
                        continue 'outer;
                    }

                    let result = result.clone().and_then(|message| match message.header() {
                        CommonMsgInfo::ExtOutMsgInfo(_) => {
                            message
                                .body()
                                .ok_or_else(|| TransportError::FailedToParseMessage {
                                    reason: "event message has no body".to_owned(),
                                })
                        }
                        _ => Err(TransportError::ApiFailure {
                            reason: "got internal message for event".to_string(),
                        }),
                    });

                    return Poll::Ready(Some(result));
                }
                (Some(_), _) => self.messages = None,
                (None, Some(fut)) => match Self::poll_request_fut(fut.as_mut(), cx) {
                    Poll::Ready(response) if !response.is_empty() => {
                        log::debug!("got messages: {:?}", response);
                        self.current_message = 0;
                        self.messages = Some(response);
                        self.request_fut = None;
                    }
                    Poll::Ready(_) => {
                        log::debug!("got empty response");
                        return Poll::Ready(None);
                    }
                    Poll::Pending => return Poll::Pending,
                },
                (None, None) => {
                    let client = self.client.clone();
                    let address = self.account.clone();
                    let since_lt = self.since_lt;
                    let until_lt = self.until_lt;

                    self.request_fut = Some(
                        async move {
                            client
                                .get_outbound_messages(
                                    address,
                                    since_lt,
                                    until_lt,
                                    MESSAGES_PER_SCAN_ITER,
                                )
                                .await
                        }
                        .boxed(),
                    );
                }
            }
        }
    }
}

impl<'a> Stream for EventsScanner<'a> {
    type Item = TransportResult<SliceData>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.get_mut().handle_state(cx)
    }
}

type MessagesResponse = Vec<(u64, TransportResult<Message>)>;

async fn run_local(
    node_client: &NodeClient,
    abi: &Function,
    message: ExternalMessage,
) -> TransportResult<ContractOutput> {
    let utime = Utc::now().timestamp() as u32; // TODO: make sure it is not used by contract. Otherwise force tonlabs to add gen_utime for account response

    let account_state = node_client.get_account_state(&message.dest).await?;

    let msg = encode_external_message(message);

    let (messages, _) = tvm::call_msg(
        utime,
        account_state.storage.last_trans_lt,
        account_state,
        &msg,
    )?;
    process_out_messages(
        &messages,
        MessageProcessingParams {
            abi_function: Some(abi),
            events_tx: None,
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;

    async fn make_transport() -> GraphQLTransport {
        std::env::set_var("RUST_LOG", "relay_ton::transport::graphql_transport=debug");
        util::setup();
        let db = sled::Config::new().temporary(true).open().unwrap();

        GraphQLTransport::new(
            Config {
                addr: "https://main.ton.dev/graphql".to_string(),
                next_block_timeout_sec: 60,
            },
            db,
        )
        .await
        .unwrap()
    }

    fn elector_addr() -> MsgAddressInt {
        MsgAddressInt::from_str(
            "-1:3333333333333333333333333333333333333333333333333333333333333333",
        )
        .unwrap()
    }

    fn my_addr() -> MsgAddressInt {
        MsgAddressInt::from_str(
            "-1:17519bc2a04b6ecf7afa25ba30601a4e16c9402979c236db13e1c6f3c4674e8c",
        )
        .unwrap()
    }

    #[tokio::test]
    async fn create_transport() {
        let _transport = make_transport().await;
    }

    #[tokio::test]
    async fn account_subscription() {
        let transport = make_transport().await;

        let _subscription = transport.subscribe(elector_addr()).await.unwrap();

        tokio::time::delay_for(tokio::time::Duration::from_secs(10)).await;
    }

    #[tokio::test]
    async fn rescan_lt() {
        let transport = make_transport().await;

        let mut scanner = transport.rescan_events(my_addr(), None, None);

        let mut i = 0;
        while let Some(item) = scanner.next().await {
            println!("Data: {:?}", item);
            println!("Event: {}", i);
            i += 1;
        }
    }
}
