pub mod config;

pub use config::*;

use std::collections::hash_map;

use futures::task::{Context, Poll};
use tokio::time::Duration;
use ton_abi::Function;
use ton_block::{
    AccountStuff, CommonMsgInfo, ExternalInboundMessageHeader, Message, Serializable, Transaction,
};
use ton_types::SliceData;
use tonlib::{AccountStats, TonlibClient};

use super::errors::*;
use super::tvm;
use super::utils::*;
use super::{AccountEvent, AccountSubscription, RunLocal, Transport};
use crate::models::*;
use crate::prelude::*;

pub struct TonlibTransport {
    db: Db,
    client: Arc<TonlibClient>,
    subscription_polling_interval: Duration,
    max_initial_rescan_gap: Option<u32>,
    max_rescan_gap: Option<u32>,
}

impl TonlibTransport {
    pub async fn new(config: Config, db: Db) -> TransportResult<Self> {
        let subscription_polling_interval =
            Duration::from_secs(config.subscription_polling_interval_sec);

        let max_initial_rescan_gap = config.max_initial_rescan_gap;
        let max_rescan_gap = config.max_rescan_gap;

        let client = tonlib::TonlibClient::new(&config.tonlib_config())
            .await
            .map_err(to_api_error)?;

        Ok(Self {
            db,
            client: Arc::new(client),
            subscription_polling_interval,
            max_initial_rescan_gap,
            max_rescan_gap,
        })
    }
}

#[async_trait]
impl RunLocal for TonlibTransport {
    async fn run_local(
        &self,
        abi: &Function,
        message: ExternalMessage,
    ) -> TransportResult<ContractOutput> {
        let mut message_header = ExternalInboundMessageHeader::default();
        message_header.dst = message.dest.clone();

        let mut msg = Message::with_ext_in_header(message_header);
        if let Some(body) = message.body {
            msg.set_body(body);
        }

        let (stats, info) = self
            .client
            .get_account_state(&message.dest)
            .await
            .map_err(to_api_error)?;

        let (messages, _) = tvm::call_msg(stats.gen_utime, stats.gen_lt, info, &msg)?;
        process_out_messages(
            &messages,
            MessageProcessingParams {
                abi_function: Some(abi),
                events_tx: None,
            },
        )
    }
}

#[async_trait]
impl Transport for TonlibTransport {
    async fn subscribe(
        &self,
        account: MsgAddressInt,
    ) -> TransportResult<Arc<dyn AccountSubscription>> {
        let subscription: Arc<dyn AccountSubscription> = TonlibAccountSubscription::new(
            &self.db,
            &self.client,
            &self.subscription_polling_interval,
            self.max_initial_rescan_gap,
            self.max_rescan_gap,
            account,
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
        EventsScanner::new(Cow::Owned(account), &self.client, since_lt, until_lt).boxed()
    }
}

struct TonlibAccountSubscription {
    db: Db,
    client: Arc<tonlib::TonlibClient>,
    event_notifier: watch::Receiver<AccountEvent>,
    account: MsgAddressInt,
    known_state: RwLock<(AccountStats, AccountStuff)>,
    pending_messages: RwLock<HashMap<UInt256, PendingMessage<(u32, u64)>>>,
}

impl TonlibAccountSubscription {
    async fn new(
        db: &Db,
        client: &Arc<tonlib::TonlibClient>,
        polling_interval: &Duration,
        max_initial_rescan_gap: Option<u32>,
        max_rescan_gap: Option<u32>,
        account: MsgAddressInt,
    ) -> TransportResult<Arc<Self>> {
        let db = db.clone();
        let client = client.clone();
        let (stats, known_state) = client
            .get_account_state(&account)
            .await
            .map_err(to_api_error)?;

        let last_trans_lt = match db.get(account.address().get_bytestring(0)).map_err(|e| {
            TransportError::FailedToInitialize {
                reason: e.to_string(),
            }
        })? {
            Some(data) => {
                // let mut lt = 0;
                // for (i, &byte) in data.iter().take(4).enumerate() {
                //     lt += (byte as u64) << i;
                // }
                // lt

                stats.last_trans_lt as u64
            }
            None => stats.last_trans_lt as u64,
        };

        let (tx, rx) = watch::channel(AccountEvent::StateChanged);

        let subscription = Arc::new(Self {
            db,
            client,
            event_notifier: rx,
            account,
            known_state: RwLock::new((stats, known_state)),
            pending_messages: RwLock::new(HashMap::new()),
        });
        subscription.start_loop(
            tx,
            last_trans_lt,
            *polling_interval,
            max_initial_rescan_gap,
            max_rescan_gap,
        );

        Ok(subscription)
    }

    fn start_loop(
        self: &Arc<Self>,
        state_notifier: watch::Sender<AccountEvent>,
        mut last_trans_lt: u64,
        interval: Duration,
        mut max_initial_rescan_gap: Option<u32>,
        max_rescan_gap: Option<u32>,
    ) {
        let subscription = Arc::downgrade(self);
        tokio::spawn(async move {
            'subscription_loop: loop {
                let subscription = match subscription.upgrade() {
                    Some(s) => s,
                    None => return,
                };

                tokio::time::delay_for(interval).await;

                let (stats, account_state) = match subscription
                    .client
                    .get_account_state(&subscription.account)
                    .await
                {
                    Ok(state) => state,
                    Err(e) => {
                        log::error!("error during account subscription loop. {:?}", e);
                        continue;
                    }
                };

                let new_trans_lt = stats.last_trans_lt;
                if last_trans_lt >= new_trans_lt {
                    log::debug!("no changes found. skipping");
                    continue;
                }

                log::debug!("got account state: {:?}, {:?}", stats, account_state);

                let gen_utime = stats.gen_utime;
                let mut current_trans_lt = new_trans_lt;
                let mut current_trans_hash = stats.last_trans_hash.clone();

                {
                    let mut known_state = subscription.known_state.write().await;
                    *known_state = (stats, account_state);
                }
                let _ = state_notifier.broadcast(AccountEvent::StateChanged);

                let mut pending_messages = subscription.pending_messages.write().await;

                log::debug!("fetching latest transactions");
                'process_transactions: loop {
                    let transactions = match subscription
                        .client
                        .get_transactions(
                            &subscription.account,
                            16,
                            current_trans_lt,
                            current_trans_hash.clone(),
                        )
                        .await
                    {
                        Ok(transactions) if !transactions.is_empty() => transactions,
                        Ok(_) => {
                            log::debug!("no transactions found");
                            break 'process_transactions;
                        }
                        Err(e) => {
                            log::error!("error during account subscription loop. {:?}", e);
                            continue 'subscription_loop;
                        }
                    };

                    for (hash, transaction) in transactions.into_iter() {
                        if transaction.lt < last_trans_lt {
                            break 'process_transactions;
                        }

                        current_trans_lt = transaction.lt;
                        current_trans_hash = hash;

                        let out_messages = match parse_transaction_messages(&transaction) {
                            Ok(messages) => messages,
                            Err(e) => {
                                log::error!("error during transaction processing. {:?}", e);
                                continue;
                            }
                        };

                        if let Some(in_msg) = &transaction.in_msg {
                            if let Some(pending_message) = pending_messages.remove(&in_msg.hash()) {
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
                                log::error!("error during out messages processing. {}", e);
                                // Just ignore
                            }
                        }

                        match max_initial_rescan_gap.or(max_rescan_gap) {
                            Some(gap) if gen_utime - transaction.now >= gap => {
                                max_initial_rescan_gap = None;
                                break 'process_transactions;
                            }
                            _ if transaction.prev_trans_lt < last_trans_lt => {
                                break 'process_transactions;
                            }
                            _ => {}
                        }
                    }
                }

                pending_messages.retain(|_, message| gen_utime <= message.expires_at());

                last_trans_lt = new_trans_lt;
                if let Err(e) = subscription.db.insert(
                    subscription.account.address().get_bytestring(0),
                    &new_trans_lt.to_le_bytes(),
                ) {
                    log::error!("failed to save state into db. {}", e);
                }
            }
        });
    }
}

#[async_trait]
impl RunLocal for TonlibAccountSubscription {
    async fn run_local(
        &self,
        abi: &AbiFunction,
        message: ExternalMessage,
    ) -> TransportResult<ContractOutput> {
        let message = message.encode();

        let account_state = self.known_state.read().await;

        let (messages, _) = tvm::call_msg(
            account_state.0.gen_utime,
            account_state.0.gen_lt,
            account_state.1.clone(),
            &message,
        )?;

        process_out_messages(
            &messages,
            MessageProcessingParams {
                abi_function: Some(abi),
                events_tx: None,
            },
        )
    }
}

#[async_trait]
impl AccountSubscription for TonlibAccountSubscription {
    fn events(&self) -> watch::Receiver<AccountEvent> {
        self.event_notifier.clone()
    }

    async fn simulate_call(
        &self,
        message: InternalMessage,
    ) -> TransportResult<Vec<ton_block::Message>> {
        let message = message.encode();

        let account_state = self.known_state.read().await;

        let (messages, _) = tvm::call_msg(
            account_state.0.gen_utime,
            account_state.0.gen_lt,
            account_state.1.clone(),
            &message,
        )?;
        Ok(messages)
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
            match pending_messages.entry(hash) {
                hash_map::Entry::Vacant(entry) => {
                    let previous_known_lt = self.known_state.read().await.0.last_trans_lt as u64;

                    self.client
                        .send_message(serialized)
                        .await
                        .map_err(to_api_error)?;

                    entry.insert(PendingMessage::new(
                        (expires_at, previous_known_lt),
                        abi,
                        tx,
                    ))
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
        EventsScanner::new(
            Cow::Borrowed(&self.account),
            &self.client,
            since_lt,
            until_lt,
        )
        .boxed()
    }
}

impl PendingMessage<(u32, u64)> {
    pub fn expires_at(&self) -> u32 {
        self.data().0
    }

    #[allow(dead_code)]
    pub fn latest_transaction_lt(&self) -> u64 {
        self.data().1
    }
}

const MESSAGES_PER_SCAN_ITER: u8 = 16;

type AccountStateResponse = Result<(AccountStats, AccountStuff), failure::Error>;
type TransactionsResponse = Result<Vec<(UInt256, Transaction)>, failure::Error>;

struct EventsScanner<'a> {
    account: Cow<'a, MsgAddressInt>,
    client: Arc<tonlib::TonlibClient>,
    since_lt: Option<u64>,
    until_lt: Option<u64>,
    latest_lt: u64,
    latest_hash: UInt256,
    account_state_fut: Option<BoxFuture<'a, AccountStateResponse>>,
    request_fut: Option<BoxFuture<'a, TransactionsResponse>>,
    transactions: Option<Vec<(UInt256, Transaction)>>,
    current_transaction: usize,
    messages: Option<Vec<Message>>,
    current_message: usize,
}

impl<'a> EventsScanner<'a>
where
    Self: Stream<Item = TransportResult<SliceData>>,
{
    fn new(
        account: Cow<'a, MsgAddressInt>,
        client: &'a Arc<TonlibClient>,
        since_lt: Option<u64>,
        until_lt: Option<u64>,
    ) -> Self {
        let account_state_fut = Some({
            let client = client.clone();
            let account = account.clone();

            async move { client.get_account_state(account.as_ref()).await }.boxed()
        });

        EventsScanner {
            account,
            client: client.clone(),
            since_lt,
            until_lt,
            latest_lt: 0,
            latest_hash: UInt256::default(),
            account_state_fut,
            request_fut: None,
            transactions: None,
            current_transaction: 0,
            messages: None,
            current_message: 0,
        }
    }

    fn get_transactions(&self) -> BoxFuture<'a, TransactionsResponse> {
        let client = self.client.clone();
        let account = self.account.as_ref().clone();
        let latest_lt = self.latest_lt;
        let latest_hash = self.latest_hash.clone();

        async move {
            client
                .get_transactions(&account, MESSAGES_PER_SCAN_ITER, latest_lt, latest_hash)
                .await
        }
        .boxed()
    }

    fn handle_state<'c>(&mut self, cx: &mut Context<'c>) -> Poll<Option<<Self as Stream>::Item>> {
        'outer: loop {
            match (
                &mut self.transactions,
                &mut self.messages,
                &mut self.request_fut,
                &mut self.account_state_fut,
            ) {
                (None, None, None, None) => return Poll::Ready(None),
                (_, Some(messages), _, _) if self.current_message < messages.len() => {
                    let message = &messages[self.current_message];
                    self.current_message += 1;

                    match message.header() {
                        CommonMsgInfo::ExtOutMsgInfo(_) => {
                            let result = message.body().ok_or_else(|| {
                                TransportError::FailedToParseMessage {
                                    reason: "event message has no body".to_owned(),
                                }
                            });
                            return Poll::Ready(Some(result));
                        }
                        _ => {
                            continue 'outer;
                        }
                    }
                }
                (_, Some(_), _, _) => {
                    self.messages = None;
                    self.current_transaction += 1
                }
                (Some(transactions), _, _, _) if self.current_transaction < transactions.len() => {
                    let (_, transaction) = &transactions[self.current_transaction];
                    self.latest_lt = transaction.prev_trans_lt;
                    self.latest_hash = transaction.prev_trans_hash.clone();

                    match (self.since_lt, self.until_lt) {
                        (Some(since_lt), _) if transaction.lt < since_lt => {
                            self.current_transaction += 1;
                            continue 'outer;
                        }
                        (_, Some(until_lt)) if transaction.lt > until_lt => {
                            self.current_transaction += 1;
                            continue 'outer;
                        }
                        _ => match parse_transaction_messages(transaction) {
                            Ok(messages) => {
                                self.messages = Some(messages);
                                self.current_message = 0;
                            }
                            Err(e) => {
                                self.transactions = None;
                                self.messages = None;
                                return Poll::Ready(Some(Err(e)));
                            }
                        },
                    }
                }
                (Some(_), _, _, _) => {
                    self.transactions = None;
                    if !matches!(self.since_lt, Some(since_lt) if self.latest_lt < since_lt) {
                        self.request_fut = Some(self.get_transactions());
                    }
                }
                (_, _, Some(fut), _) => match fut.as_mut().poll(cx) {
                    Poll::Ready(Ok(transactions)) if !transactions.is_empty() => {
                        self.request_fut = None;

                        self.transactions = Some(transactions);
                        self.current_transaction = 0;
                        self.current_message = 0;
                    }
                    Poll::Ready(Ok(_)) => {
                        return Poll::Ready(None);
                    }
                    Poll::Ready(Err(e)) => {
                        self.request_fut = None;
                        return Poll::Ready(Some(Err(to_api_error(e))));
                    }
                    Poll::Pending => return Poll::Pending,
                },
                (_, _, _, Some(fut)) => match fut.as_mut().poll(cx) {
                    Poll::Ready(Ok((stats, _))) => {
                        self.account_state_fut = None;

                        self.latest_lt = stats.last_trans_lt;
                        self.latest_hash = stats.last_trans_hash;
                        self.request_fut = Some(self.get_transactions());
                    }
                    Poll::Ready(Err(e)) => {
                        self.account_state_fut = None;
                        return Poll::Ready(Some(Err(to_api_error(e))));
                    }
                    Poll::Pending => return Poll::Pending,
                },
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

fn to_api_error(e: failure::Error) -> TransportError {
    TransportError::ApiFailure {
        reason: e.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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

    async fn make_transport() -> TonlibTransport {
        std::env::set_var("RUST_LOG", "debug");
        util::setup();
        let db = sled::Config::new().temporary(true).open().unwrap();

        TonlibTransport::new(default_mainnet_config(), db)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn test_subscription() {
        let transport = make_transport().await;

        let _subscription = transport.subscribe(elector_addr()).await.unwrap();

        tokio::time::delay_for(Duration::from_secs(10)).await;
    }

    #[tokio::test]
    async fn test_rescan() {
        let transport = make_transport().await;

        let mut events = transport.rescan_events(my_addr(), None, None);

        let mut i = 0;
        while let Some(event) = events.next().await {
            println!("Data: {:?}", event);
            println!("Event: {}", i);
            i += 1;
        }
    }
}
