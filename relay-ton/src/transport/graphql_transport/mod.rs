use std::collections::hash_map;
use std::pin::Pin;

use futures::task::{Context, Poll};
use futures::{Future, FutureExt};
use reqwest::header::{self, HeaderMap, HeaderValue};
use reqwest::ClientBuilder;
use ton_abi::Function;
use ton_block::{
    CommonMsgInfo, Deserializable, HashmapAugType, Message, Serializable, Transaction,
};
use ton_types::HashmapType;

use crate::models::*;
use crate::prelude::*;
use crate::transport::errors::*;
use crate::transport::{AccountSubscription, AccountSubscriptionFull, RunLocal, Transport};

use super::tvm;
use super::utils::*;

pub use self::config::*;
use self::node_client::*;

pub mod config;
mod node_client;

pub struct GraphQLTransport {
    client: NodeClient,
    config: Config,
}

impl GraphQLTransport {
    pub async fn new(config: Config) -> TransportResult<Self> {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::CONTENT_TYPE,
            HeaderValue::from_str("application/json").unwrap(),
        );

        let client_builder = ClientBuilder::new().default_headers(headers);
        let client = client_builder
            .build()
            .expect("failed to create graphql client");

        let client = NodeClient::new(
            client,
            config.address.clone(),
            config.parallel_connections,
            std::time::Duration::from_secs(config.fetch_timeout_secs as u64),
        );

        Ok(Self { client, config })
    }
}

#[async_trait]
impl RunLocal for GraphQLTransport {
    async fn run_local(
        &self,
        abi: &Function,
        message: ExternalMessage,
    ) -> TransportResult<ContractOutput> {
        let messages = run_local(&self.client, message).await?;
        process_out_messages::<SliceData>(
            &messages,
            MessageProcessingParams {
                event_transaction: &Default::default(),
                event_transaction_lt: 0,
                abi_function: Some(abi),
                events_tx: None,
            },
        )
    }
}

#[async_trait]
impl Transport for GraphQLTransport {
    async fn subscribe(
        &self,
        addr: MsgAddressInt,
    ) -> TransportResult<(Arc<dyn AccountSubscription>, RawEventsRx)> {
        let (subscription, rx) = GraphQLAccountSubscription::new(
            self.client.clone(),
            self.config.next_block_timeout_sec,
            addr,
        )
        .await?;

        Ok((subscription, rx))
    }

    async fn subscribe_full(
        &self,
        addr: MsgAddressInt,
    ) -> TransportResult<(Arc<dyn AccountSubscriptionFull>, FullEventsRx)> {
        let (subscription, rx) = GraphQLAccountSubscription::new(
            self.client.clone(),
            self.config.next_block_timeout_sec,
            addr,
        )
        .await?;

        Ok((subscription, rx))
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
            _marker: Default::default(),
        }
        .boxed()
    }
}

struct GraphQLAccountSubscription<T> {
    since_lt: u64,
    client: NodeClient,
    account: MsgAddressInt,
    account_id: UInt256,
    pending_messages: RwLock<HashMap<UInt256, PendingMessage<u32>>>,
    _marker: std::marker::PhantomData<T>,
}

impl<T> GraphQLAccountSubscription<T>
where
    T: PrepareEvent,
{
    async fn new(
        client: NodeClient,
        next_block_timeout: u32,
        addr: MsgAddressInt,
    ) -> TransportResult<(Arc<Self>, EventsRx<T>)> {
        let client = client.clone();
        let last_block = client.get_latest_block(&addr).await?;

        let (events_tx, rx) = mpsc::unbounded_channel();

        let subscription = Arc::new(Self {
            since_lt: last_block.end_lt,
            client,
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
            _marker: Default::default(),
        });
        subscription.start_loop(events_tx, last_block.id, next_block_timeout);

        Ok((subscription, rx))
    }

    fn start_loop(
        self: &Arc<Self>,
        events_tx: EventsTx<T>,
        mut last_block_id: String,
        next_block_timeout: u32,
    ) {
        let account = self.account.clone();
        let subscription = Arc::downgrade(self);

        log::debug!("started polling account {}", self.account);

        tokio::spawn(async move {
            'subscription_loop: loop {
                let subscription = match subscription.upgrade() {
                    Some(s) => s,
                    None => {
                        log::info!("stopped account subscription loop for {}", account);
                        return;
                    }
                };

                let next_block_id = match subscription
                    .client
                    .wait_for_next_block(&last_block_id, &account, next_block_timeout)
                    .await
                {
                    Ok(id) => id,
                    Err(e) => {
                        log::error!("failed to get next block id. {:?}", e);
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

                        for item in data.transactions().iter() {
                            let (transaction, hash) = match item.and_then(|(_, value)| {
                                let cell = value.into_cell().reference(0)?;
                                let hash = cell.repr_hash();

                                Transaction::construct_from_cell(cell)
                                    .map(|transaction| (transaction, hash))
                            }) {
                                Ok(transaction) => transaction,
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
                                            event_transaction: &hash,
                                            event_transaction_lt: transaction.lt,
                                            abi_function: Some(pending_message.abi()),
                                            events_tx: Some(&events_tx),
                                        },
                                    );
                                    pending_message.set_result(result);
                                } else if let Err(e) = process_out_messages(
                                    &out_messages,
                                    MessageProcessingParams {
                                        event_transaction: &hash,
                                        event_transaction_lt: transaction.lt,
                                        abi_function: None,
                                        events_tx: Some(&events_tx),
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
                    }
                    Err(e) => {
                        log::error!("failed to parse block data. {:?}", e.to_string());
                        continue 'subscription_loop;
                    }
                };

                for (_, message) in pending_messages.iter() {
                    log::trace!(
                        "CURRENT: {}, {}, {}",
                        block_info.gen_utime().0,
                        message.expires_at(),
                        message.expires_at() as i64 - block_info.gen_utime().0 as i64
                    );
                }

                pending_messages
                    .retain(|_, message| block_info.gen_utime().0 <= message.expires_at());
                log::debug!(
                    "PENDING: {}. TIME DIFF: {}",
                    pending_messages.len(),
                    block_info.gen_utime().0 as i64 - Utc::now().timestamp(),
                );

                last_block_id = next_block_id;
            }
        });
    }
}

#[async_trait]
impl<T> RunLocal for GraphQLAccountSubscription<T>
where
    T: PrepareEvent,
{
    async fn run_local(
        &self,
        abi: &Function,
        message: ExternalMessage,
    ) -> TransportResult<ContractOutput> {
        let messages = run_local(&self.client, message).await?;
        process_out_messages::<SliceData>(
            &messages,
            MessageProcessingParams {
                event_transaction: &Default::default(),
                event_transaction_lt: 0,
                abi_function: Some(abi),
                events_tx: None,
            },
        )
    }
}

#[async_trait]
impl<T> AccountSubscription for GraphQLAccountSubscription<T>
where
    T: PrepareEvent,
{
    fn since_lt(&self) -> u64 {
        self.since_lt
    }

    async fn simulate_call(&self, message: InternalMessage) -> TransportResult<Vec<Message>> {
        run_local(&self.client, message).await
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

        let cells = message
            .encode()
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
        EventsScanner::<SliceData> {
            account: self.account.clone(),
            client: &self.client,
            since_lt,
            until_lt,
            request_fut: None,
            messages: None,
            current_message: 0,
            _marker: Default::default(),
        }
        .boxed()
    }
}

#[async_trait]
impl AccountSubscriptionFull for GraphQLAccountSubscription<FullEventInfo> {
    fn rescan_events_full(
        &self,
        since_lt: Option<u64>,
        until_lt: Option<u64>,
    ) -> BoxStream<'_, TransportResult<FullEventInfo>> {
        EventsScanner::<FullEventInfo> {
            account: self.account.clone(),
            client: &self.client,
            since_lt,
            until_lt,
            request_fut: None,
            messages: None,
            current_message: 0,
            _marker: Default::default(),
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

struct EventsScanner<'a, T: PrepareEventExt> {
    account: MsgAddressInt,
    client: &'a NodeClient,
    since_lt: Option<u64>,
    until_lt: Option<u64>,
    request_fut: Option<BoxFuture<'static, TransportResult<MessagesResponse<T>>>>,
    messages: Option<MessagesResponse<T>>,
    current_message: usize,
    _marker: std::marker::PhantomData<T>,
}

type MessagesResponse<T> = Vec<<T as PrepareEventExt>::ResponseItem>;

impl<'a, T> EventsScanner<'a, T>
where
    Self: Stream<Item = TransportResult<T>>,
    T: PrepareEventExt,
{
    fn poll_request_fut<'c, F>(fut: Pin<&mut F>, cx: &mut Context<'c>) -> Poll<MessagesResponse<T>>
    where
        F: Future<Output = TransportResult<MessagesResponse<T>>> + ?Sized,
    {
        match fut.poll(cx) {
            Poll::Ready(Ok(new_messages)) => Poll::Ready(new_messages),
            Poll::Ready(Err(err)) => Poll::Ready(vec![T::from_error(err)]),
            Poll::Pending => Poll::Pending,
        }
    }

    fn handle_state<'c>(&mut self, cx: &mut Context<'c>) -> Poll<Option<<Self as Stream>::Item>> {
        'outer: loop {
            match (&mut self.messages, &mut self.request_fut) {
                (Some(messages), _) if self.current_message < messages.len() => {
                    let message = &messages[self.current_message];
                    self.current_message += 1;

                    match T::handle_item(&mut self.until_lt, self.since_lt, message) {
                        MessageAction::Skip => continue 'outer,
                        MessageAction::Emit(result) => return Poll::Ready(Some(result)),
                    }
                }
                (Some(_), _) => self.messages = None,
                (None, Some(fut)) => match Self::poll_request_fut(fut.as_mut(), cx) {
                    Poll::Ready(response) if !response.is_empty() => {
                        log::trace!("got messages: {:?}", response);
                        self.current_message = 0;
                        self.messages = Some(response);
                        self.request_fut = None;
                    }
                    Poll::Ready(_) => {
                        log::trace!("got empty response");
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
                        T::get_outbound_messages(
                            client,
                            address,
                            since_lt,
                            until_lt,
                            MESSAGES_PER_SCAN_ITER,
                        )
                        .boxed(),
                    );
                }
            }
        }
    }
}

impl<'a, T> Stream for EventsScanner<'a, T>
where
    T: PrepareEventExt,
{
    type Item = TransportResult<T>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.get_mut().handle_state(cx)
    }
}

async fn run_local<T>(node_client: &NodeClient, message: T) -> TransportResult<Vec<Message>>
where
    T: ExecutableMessage,
{
    let utime = Utc::now().timestamp() as u32; // TODO: make sure it is not used by contract. Otherwise force tonlabs to add gen_utime for account response

    let account_state = node_client.get_account_state(message.dest()).await?;

    let msg = message.encode();

    let (messages, _) = tvm::call_msg(
        utime,
        account_state.storage.last_trans_lt,
        account_state,
        &msg,
    )?;

    Ok(messages)
}

enum MessageAction<T> {
    Skip,
    Emit(T),
}

#[async_trait]
trait PrepareEventExt: PrepareEvent + Unpin {
    type ResponseItem: std::fmt::Debug + Unpin;

    async fn get_outbound_messages(
        node: NodeClient,
        addr: MsgAddressInt,
        start_lt: Option<u64>,
        end_lt: Option<u64>,
        limit: u32,
    ) -> TransportResult<Vec<Self::ResponseItem>>;

    fn handle_item(
        until_lt: &mut Option<u64>,
        since_lt: Option<u64>,
        message: &Self::ResponseItem,
    ) -> MessageAction<TransportResult<Self>>;

    fn from_error(err: TransportError) -> Self::ResponseItem;
}

#[async_trait]
impl PrepareEventExt for SliceData {
    type ResponseItem = OutboundMessage;

    async fn get_outbound_messages(
        node: NodeClient,
        addr: MsgAddressInt,
        start_lt: Option<u64>,
        end_lt: Option<u64>,
        limit: u32,
    ) -> TransportResult<Vec<Self::ResponseItem>> {
        node.get_outbound_messages(addr, start_lt, end_lt, limit)
            .await
    }

    fn handle_item(
        until_lt: &mut Option<u64>,
        since_lt: Option<u64>,
        message: &Self::ResponseItem,
    ) -> MessageAction<TransportResult<Self>> {
        *until_lt = Some(message.lt);

        if matches!(since_lt.as_ref(), Some(since_lt) if &message.lt < since_lt) {
            return MessageAction::Skip;
        }

        let result = message
            .data
            .clone()
            .and_then(|message| match message.header() {
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

        MessageAction::Emit(result)
    }

    fn from_error(err: TransportError) -> Self::ResponseItem {
        Self::ResponseItem {
            lt: 0,
            data: Err(err),
        }
    }
}

#[async_trait]
impl PrepareEventExt for FullEventInfo {
    type ResponseItem = OutboundMessageFull;

    async fn get_outbound_messages(
        node: NodeClient,
        addr: MsgAddressInt,
        start_lt: Option<u64>,
        end_lt: Option<u64>,
        limit: u32,
    ) -> TransportResult<Vec<Self::ResponseItem>> {
        node.get_outbound_messages_full(addr, start_lt, end_lt, limit)
            .await
    }

    fn handle_item(
        until_lt: &mut Option<u64>,
        since_lt: Option<u64>,
        message: &Self::ResponseItem,
    ) -> MessageAction<TransportResult<Self>> {
        *until_lt = Some(message.transaction_lt);

        if matches!(since_lt.as_ref(), Some(since_lt) if &message.transaction_lt < since_lt) {
            return MessageAction::Skip;
        }

        let result = message
            .data
            .clone()
            .and_then(|message| match message.header() {
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

        MessageAction::Emit(result.map(|event_data| FullEventInfo {
            event_transaction: message.transaction_hash.clone(),
            event_transaction_lt: message.transaction_lt,
            event_index: message.event_index,
            event_data,
        }))
    }

    fn from_error(err: TransportError) -> Self::ResponseItem {
        Self::ResponseItem {
            data: Err(err),
            transaction_hash: Default::default(),
            transaction_lt: 0,
            event_index: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use futures::StreamExt;

    use super::*;

    async fn make_transport() -> GraphQLTransport {
        std::env::set_var("RUST_LOG", "relay_ton::transport::graphql_transport=debug");
        util::setup();
        let db = sled::Config::new().temporary(true).open().unwrap();

        GraphQLTransport::new(
            Config {
                address: "https://main.ton.dev/graphql".to_string(),
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
