pub mod config;
mod tvm;

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

use self::config::*;
use super::errors::*;
use super::{AccountEvent, AccountSubscription, Transport};
use crate::models::*;
use crate::prelude::*;

pub struct TonlibTransport {
    client: Arc<TonlibClient>,
    subscription_polling_interval: Duration,
}

impl TonlibTransport {
    pub async fn new(config: Config) -> TransportResult<Self> {
        let subscription_polling_interval =
            Duration::from_secs(config.subscription_polling_interval_sec);

        let client = tonlib::TonlibClient::new(&config.into())
            .await
            .map_err(to_api_error)?;

        Ok(Self {
            client: Arc::new(client),
            subscription_polling_interval,
        })
    }
}

#[async_trait]
impl Transport for TonlibTransport {
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
        process_out_messages(&messages, abi)
    }

    async fn subscribe(&self, addr: &str) -> TransportResult<Arc<dyn AccountSubscription>> {
        let subscription: Arc<dyn AccountSubscription> =
            TonlibAccountSubscription::new(&self.client, &self.subscription_polling_interval, addr)
                .await?;

        Ok(subscription)
    }
}

struct TonlibAccountSubscription {
    client: Arc<tonlib::TonlibClient>,
    event_notifier: watch::Receiver<AccountEvent>,
    account: ton::lite_server::accountid::AccountId,
    known_state: RwLock<ton::lite_server::rawaccount::RawAccount>,
    pending_messages: RwLock<HashMap<UInt256, PendingMessage>>,
}

impl TonlibAccountSubscription {
    async fn new(
        client: &Arc<tonlib::TonlibClient>,
        polling_interval: &Duration,
        addr: &str,
    ) -> TransportResult<Arc<Self>> {
        let client = client.clone();
        let account = tonlib::utils::make_address_from_str(addr).map_err(to_api_error)?;
        let known_state = client
            .get_account_state(account.clone())
            .await
            .map_err(to_api_error)?;
        let last_trans_lt = known_state.last_trans_lt as u64;

        let (tx, rx) = watch::channel(AccountEvent::StateChanged);

        let subscription = Arc::new(Self {
            client,
            event_notifier: rx,
            account,
            known_state: RwLock::new(known_state),
            pending_messages: RwLock::new(HashMap::new()),
        });
        subscription.start_loop(tx, last_trans_lt, polling_interval.clone());

        Ok(subscription)
    }

    fn start_loop(
        self: &Arc<Self>,
        state_notifier: watch::Sender<AccountEvent>,
        mut last_trans_lt: u64,
        interval: Duration,
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

                if !pending_messages.is_empty() {
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
                                        log::error!(
                                            "error during account subscription loop. {}",
                                            e
                                        );
                                        continue 'subscription_loop;
                                    }
                                };

                            current_trans_lt = transaction.lt;
                            current_trans_hash = ton::bytes(hash.as_slice().to_vec());

                            if let Some(in_msg) = &transaction.in_msg {
                                if let Some(pending_message) =
                                    pending_messages.remove(&in_msg.hash())
                                {
                                    let result = process_transaction(
                                        &transaction,
                                        pending_message.abi.as_ref(),
                                    );
                                    let _ = pending_message.tx.send(result);
                                }
                            }

                            if transaction.prev_trans_lt < last_trans_lt {
                                break 'process_transactions;
                            }
                        }
                    }

                    pending_messages.retain(|_, message| message.expires_at <= gen_utime);
                }

                last_trans_lt = current_trans_lt;
            }
        });
    }
}

#[async_trait]
impl AccountSubscription for TonlibAccountSubscription {
    fn events(&self) -> watch::Receiver<AccountEvent> {
        self.event_notifier.clone()
    }

    async fn run_local(
        &self,
        abi: &AbiFunction,
        message: ExternalMessage,
    ) -> TransportResult<ContractOutput> {
        let message = encode_external_message(message);

        let account_state = self.known_state.read().await;
        let info = parse_account_stuff(account_state.data.0.as_slice())?;

        let (messages, _) = tvm::call_msg(&account_state, info, &message)?;

        process_out_messages(&messages, abi)
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
    let mut cell =
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

fn process_transaction(
    transaction: &Transaction,
    abi_function: &AbiFunction,
) -> TransportResult<ContractOutput> {
    let mut messages = Vec::new();
    transaction
        .out_msgs
        .iterate_slices(|slice| {
            if let Ok(message) = slice
                .reference(0)
                .and_then(|cell| Message::construct_from_cell(cell))
            {
                messages.push(message);
            }
            Ok(true)
        })
        .map_err(|e| TransportError::FailedToParseTransaction {
            reason: e.to_string(),
        })?;
    process_out_messages(&messages, abi_function)
}

fn process_out_messages(
    messages: &Vec<Message>,
    abi_function: &AbiFunction,
) -> TransportResult<ContractOutput> {
    if messages.len() == 0 || !abi_function.has_output() {
        return Ok(ContractOutput {
            transaction_id: None,
            tokens: Vec::new(),
        });
    }

    for msg in messages {
        if !matches!(msg.header(), CommonMsgInfo::ExtOutMsgInfo(_)) {
            continue;
        }

        let body = msg.body().ok_or_else(|| TransportError::ExecutionError {
            reason: "output message has not body".to_string(),
        })?;

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

            return Ok(ContractOutput {
                transaction_id: None,
                tokens,
            });
        }
    }

    return Err(TransportError::ExecutionError {
        reason: "no external output messages".to_owned(),
    });
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
                "ip": "54.158.97.195",
                "port": 3031,
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
