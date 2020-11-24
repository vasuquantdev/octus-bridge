pub mod config;
mod tvm;

pub use config::*;

use std::collections::hash_map;

use failure::AsFail;
use tokio::sync::oneshot;
use tokio::time::Duration;
use ton_abi::Function;
use ton_api::ton;
use ton_block::{
    AccountStuff, CommonMsgInfo, Deserializable, ExternalInboundMessageHeader, Message,
    Serializable, Transaction,
};
use ton_types::SliceData;
use tonlib::{TonlibClient, TonlibError};

use super::errors::*;
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

        let client = tonlib::TonlibClient::new(&config.into())
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
        let address = tonlib::utils::make_address_from_str(&message.dest.to_string())
            .map_err(to_api_error)?;

        let mut message_header = ExternalInboundMessageHeader::default();
        message_header.dst = message.dest.clone();

        let mut msg = Message::with_ext_in_header(message_header);
        if let Some(body) = message.body {
            msg.set_body(body);
        }

        let account_state = self
            .client
            .get_account_state(address)
            .await
            .map_err(to_api_error)?;
        let info = parse_account_stuff(account_state.data.0.as_slice())?;

        let (messages, _) = tvm::call_msg(&account_state, info, &msg)?;
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
    async fn subscribe(&self, addr: &str) -> TransportResult<Arc<dyn AccountSubscription>> {
        let subscription: Arc<dyn AccountSubscription> = TonlibAccountSubscription::new(
            &self.db,
            &self.client,
            &self.subscription_polling_interval,
            self.max_initial_rescan_gap,
            self.max_rescan_gap,
            addr,
        )
        .await?;

        Ok(subscription)
    }
}

struct TonlibAccountSubscription {
    db: Db,
    client: Arc<tonlib::TonlibClient>,
    event_notifier: watch::Receiver<AccountEvent>,
    account: ton::lite_server::accountid::AccountId,
    known_state: RwLock<ton::lite_server::rawaccount::RawAccount>,
    pending_messages: RwLock<HashMap<UInt256, PendingMessage>>,
}

impl TonlibAccountSubscription {
    async fn new(
        db: &Db,
        client: &Arc<tonlib::TonlibClient>,
        polling_interval: &Duration,
        max_initial_rescan_gap: Option<u32>,
        max_rescan_gap: Option<u32>,
        addr: &str,
    ) -> TransportResult<Arc<Self>> {
        let db = db.clone();
        let client = client.clone();
        let account = tonlib::utils::make_address_from_str(addr).map_err(to_api_error)?;
        let known_state = client
            .get_account_state(account.clone())
            .await
            .map_err(to_api_error)?;

        let last_trans_lt = match db.get(account.id.0.as_slice()).map_err(|e| {
            TransportError::FailedToInitialize {
                reason: e.to_string(),
            }
        })? {
            Some(data) => {
                let mut lt = 0;
                for (i, &byte) in data.iter().take(4).enumerate() {
                    lt += (byte as u64) << i;
                }
                lt
            }
            None => known_state.last_trans_lt as u64,
        };

        let (tx, rx) = watch::channel(AccountEvent::StateChanged);

        let subscription = Arc::new(Self {
            db,
            client,
            event_notifier: rx,
            account,
            known_state: RwLock::new(known_state),
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

                let account_state = match subscription
                    .client
                    .get_account_state(subscription.account.clone())
                    .await
                {
                    Ok(state) => state,
                    Err(e) => {
                        log::error!("error during account subscription loop. {}", e);
                        continue;
                    }
                };
                log::debug!("got account state: {:?}", account_state);

                let new_trans_lt = account_state.last_trans_lt as u64;
                if last_trans_lt >= new_trans_lt {
                    log::debug!("no changes found. skipping");
                    continue;
                }

                let gen_utime = account_state.gen_utime as u32;
                let mut current_trans_lt = new_trans_lt;
                let mut current_trans_hash = account_state.last_trans_hash.clone();

                {
                    let mut known_state = subscription.known_state.write().await;
                    *known_state = account_state;
                }
                let _ = state_notifier.broadcast(AccountEvent::StateChanged);

                let mut pending_messages = subscription.pending_messages.write().await;

                log::debug!("fetching latest transactions");
                'process_transactions: loop {
                    let transactions = match subscription
                        .client
                        .get_transactions(
                            subscription.account.clone(),
                            16,
                            current_trans_lt as i64,
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
                            log::error!("error during account subscription loop. {}", e);
                            continue 'subscription_loop;
                        }
                    };

                    for transaction in transactions.into_iter() {
                        let (transaction, hash) =
                            match parse_transaction(transaction.data.0.as_slice()) {
                                Ok(transaction) => transaction,
                                Err(e) => {
                                    log::error!("error during account subscription loop. {}", e);
                                    continue 'subscription_loop;
                                }
                            };

                        current_trans_lt = transaction.lt;
                        current_trans_hash = ton::bytes(hash.as_slice().to_vec());

                        let out_messages = match parse_transaction_messages(&transaction) {
                            Ok(messages) => messages,
                            Err(e) => {
                                log::error!("error during transaction processing. {}", e);
                                continue 'subscription_loop;
                            }
                        };

                        if let Some(in_msg) = &transaction.in_msg {
                            if let Some(pending_message) = pending_messages.remove(&in_msg.hash()) {
                                let result = process_out_messages(
                                    &out_messages,
                                    MessageProcessingParams {
                                        abi_function: Some(pending_message.abi.as_ref()),
                                        events_tx: Some(&state_notifier),
                                    },
                                );
                                let _ = pending_message.tx.send(result);
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

                pending_messages.retain(|_, message| message.expires_at <= gen_utime);

                last_trans_lt = current_trans_lt;
                if let Err(e) = subscription.db.insert(
                    subscription.account.id.0.as_slice(),
                    &current_trans_lt.to_le_bytes(),
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
        let message = encode_external_message(message);

        let account_state = self.known_state.read().await;
        let info = parse_account_stuff(account_state.data.0.as_slice())?;

        let (messages, _) = tvm::call_msg(&account_state, info, &message)?;

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
            match pending_messages.entry(hash) {
                hash_map::Entry::Vacant(entry) => {
                    let previous_known_lt = self.known_state.read().await.last_trans_lt as u64;

                    self.client
                        .send_message(serialized)
                        .await
                        .map_err(to_api_error)?;

                    entry.insert(PendingMessage {
                        expires_at,
                        _previous_known_lt: previous_known_lt,
                        abi,
                        tx,
                    })
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
}

struct PendingMessage {
    expires_at: u32,
    _previous_known_lt: u64,
    abi: Arc<Function>,
    tx: oneshot::Sender<TransportResult<ContractOutput>>,
}

fn encode_external_message(message: ExternalMessage) -> Message {
    let mut message_header = ExternalInboundMessageHeader::default();
    message_header.dst = message.dest.clone();

    let mut msg = Message::with_ext_in_header(message_header);
    if let Some(body) = message.body {
        msg.set_body(body);
    }
    msg
}

fn parse_account_stuff(raw: &[u8]) -> TransportResult<AccountStuff> {
    let mut slice = ton_types::deserialize_tree_of_cells(&mut std::io::Cursor::new(raw))
        .and_then(|cell| {
            let mut slice: SliceData = cell.into();
            slice.move_by(1)?;
            Ok(slice)
        })
        .map_err(|e| TransportError::FailedToParseAccountState {
            reason: e.to_string(),
        })?;

    AccountStuff::construct_from(&mut slice).map_err(|e| {
        TransportError::FailedToParseAccountState {
            reason: e.to_string(),
        }
    })
}

fn parse_transaction(raw: &[u8]) -> TransportResult<(Transaction, UInt256)> {
    let cell =
        ton_types::deserialize_tree_of_cells(&mut std::io::Cursor::new(raw)).map_err(|e| {
            TransportError::FailedToParseTransaction {
                reason: e.to_string(),
            }
        })?;
    let hash = cell.hash(0);

    Transaction::construct_from(&mut cell.into())
        .map(|transaction| (transaction, hash))
        .map_err(|e| TransportError::FailedToParseAccountState {
            reason: e.to_string(),
        })
}

fn parse_transaction_messages(transaction: &Transaction) -> TransportResult<Vec<Message>> {
    let mut messages = Vec::new();
    transaction
        .out_msgs
        .iterate_slices(|slice| {
            if let Ok(message) = slice.reference(0).and_then(Message::construct_from_cell) {
                messages.push(message);
            }
            Ok(true)
        })
        .map_err(|e| TransportError::FailedToParseTransaction {
            reason: e.to_string(),
        })?;
    Ok(messages)
}

struct MessageProcessingParams<'a> {
    abi_function: Option<&'a AbiFunction>,
    events_tx: Option<&'a watch::Sender<AccountEvent>>,
}

fn process_out_messages<'a>(
    messages: &'a [Message],
    params: MessageProcessingParams<'a>,
) -> TransportResult<ContractOutput> {
    let mut output = None;

    for msg in messages {
        if !matches!(msg.header(), CommonMsgInfo::ExtOutMsgInfo(_)) {
            continue;
        }

        let body = msg.body().ok_or_else(|| TransportError::ExecutionError {
            reason: "output message has not body".to_string(),
        })?;

        match (&params.abi_function, &params.events_tx) {
            (Some(abi_function), _) if output.is_none() => {
                if abi_function
                    .is_my_output_message(body.clone(), false)
                    .map_err(|e| TransportError::ExecutionError {
                        reason: e.to_string(),
                    })?
                {
                    let tokens = abi_function.decode_output(body, false).map_err(|e| {
                        TransportError::ExecutionError {
                            reason: e.to_string(),
                        }
                    })?;

                    output = Some(ContractOutput {
                        transaction_id: None,
                        tokens,
                    });
                }
            }
            (_, Some(events_tx)) => {
                let _ = events_tx.broadcast(AccountEvent::OutboundEvent(Arc::new(body)));
            }
            _ => {}
        }
    }

    match (params.abi_function, output) {
        (Some(abi_function), _) if !abi_function.has_output() => Ok(Default::default()),
        (Some(_), Some(output)) => Ok(output),
        (None, _) => Ok(Default::default()),
        _ => Err(TransportError::ExecutionError {
            reason: "no external output messages".to_owned(),
        }),
    }
}

fn to_api_error(e: TonlibError) -> TransportError {
    TransportError::ApiFailure {
        reason: e.as_fail().to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MAINNET_CONFIG: &str = r#"{
        "lite_servers": [
            {                
                "addr": "54.158.97.195:3031",
                "public_key": "uNRRL+6enQjuiZ/s6Z+vO7yxUUR7uxdfzIy+RxkECrc="
            }
        ],
        "zero_state": {
            "file_hash": "0nC4eylStbp9qnCq8KjDYb789NjS25L5ZA1UQwcIOOQ=",
            "root_hash": "WP/KGheNr/cF3lQhblQzyb0ufYUAcNM004mXhHq56EU=",
            "shard": -9223372036854775808,
            "seqno": 0,
            "workchain": -1
        }
    }"#;

    const ELECTOR_ADDR: &str =
        "-1:3333333333333333333333333333333333333333333333333333333333333333";

    async fn make_transport() -> TonlibTransport {
        std::env::set_var("RUST_LOG", "debug");
        env_logger::init();

        TonlibTransport::new(Config {
            network_config: serde_json::from_str(MAINNET_CONFIG).unwrap(),
            network_name: "mainnet".to_string(),
            verbosity: 1,
            keystore: KeystoreType::InMemory,
            last_block_threshold_sec: 1,
            subscription_polling_interval_sec: 1,
            max_initial_rescan_gap: None,
            max_rescan_gap: None,
        })
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn test_subscription() {
        let transport = make_transport().await;

        let _subscription = transport.subscribe(ELECTOR_ADDR).await;

        tokio::time::delay_for(Duration::from_secs(10)).await;
    }
}
