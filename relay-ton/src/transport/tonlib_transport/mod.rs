pub mod config;
mod tvm;

use std::collections::hash_map;

use failure::AsFail;
use tokio::sync::oneshot;
use tokio::time::Duration;
use ton_abi::Function;
use ton_api::ton;
use ton_block::{
    AccountStuff, CommonMsgInfo, Deserializable, ExternalInboundMessageHeader,
    GetRepresentationHash, Message, MsgAddrVar, MsgAddressInt, Serializable, Transaction,
};
use ton_types::SliceData;
use tonlib::{TonlibClient, TonlibError};

use self::config::*;
use super::errors::*;
use super::Transport;
use crate::models::*;
use crate::prelude::*;

pub struct TonlibTransport {
    client: TonlibClient,
}

impl TonlibTransport {
    pub async fn new(config: Config) -> TransportResult<Self> {
        let client = tonlib::TonlibClient::new(&config.into())
            .await
            .map_err(to_api_error)?;

        Ok(Self { client })
    }

    async fn run_local(
        &self,
        function: &AbiFunction,
        address: &MsgAddressInt,
        message: &Message,
    ) -> TransportResult<ContractOutput> {
        let address =
            tonlib::utils::make_address_from_str(&address.to_string()).map_err(to_api_error)?;

        let account_state = self
            .client
            .get_account_state(address)
            .await
            .map_err(to_api_error)?;

        let info = parse_account_stuff(account_state.data.0.as_slice())?;

        let (messages, _) = tvm::call_msg(&account_state, info, message)?;

        process_out_messages(&messages, function)
    }
}

#[async_trait]
impl Transport for TonlibTransport {
    async fn get_account_state(
        &self,
        addr: &MsgAddressInt,
    ) -> TransportResult<Option<AccountState>> {
        let address =
            tonlib::utils::make_address_from_str(&addr.to_string()).map_err(to_api_error)?;

        let account_state = self
            .client
            .get_account_state(address)
            .await
            .map_err(to_api_error)?;

        let info = parse_account_stuff(account_state.data.0.as_slice())?;

        Ok(Some(AccountState {
            balance: info.storage.balance.grams.0.into(),
            last_transaction: Some(account_state.last_trans_lt as u64),
        }))
    }

    async fn send_message(
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

        if message.run_local {
            return self.run_local(abi, &message.dest, &msg).await;
        }

        unimplemented!()
    }
}

struct AccountSubscription {
    client: Arc<tonlib::TonlibClient>,
    account: ton::lite_server::accountid::AccountId,
    known_state: RwLock<ton::lite_server::rawaccount::RawAccount>,
    pending_messages: RwLock<HashMap<UInt256, PendingMessage>>,
}

impl AccountSubscription {
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

        let subscription = Arc::new(Self {
            client,
            account,
            known_state: RwLock::new(known_state),
            pending_messages: RwLock::new(HashMap::new()),
        });
        subscription.start_loop(last_trans_lt, polling_interval.clone());

        Ok(subscription)
    }

    pub async fn send_message(
        &self,
        abi: Arc<Function>,
        message: ExternalMessage,
    ) -> TransportResult<ContractOutput> {
        let mut message_header = ExternalInboundMessageHeader::default();
        message_header.dst = message.dest.clone();

        let mut msg = Message::with_ext_in_header(message_header);
        if let Some(body) = message.body {
            msg.set_body(body);
        }

        if message.run_local {
            return self.run_local(abi.as_ref(), &msg).await;
        }

        let cells = msg
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
                        expires_at: message.header.expire,
                        previous_known_lt,
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

        rx.await.unwrap_or_else(|e| {
            Err(TransportError::ApiFailure {
                reason: "subscription part dropped before receiving message response".to_owned(),
            })
        })
    }

    pub async fn run_local(
        &self,
        function: &AbiFunction,
        message: &Message,
    ) -> TransportResult<ContractOutput> {
        let account_state = self.known_state.read().await;
        let info = parse_account_stuff(account_state.data.0.as_slice())?;

        let (messages, _) = tvm::call_msg(&account_state, info, message)?;

        process_out_messages(&messages, function)
    }

    fn start_loop(self: &Arc<Self>, mut last_trans_lt: u64, interval: Duration) {
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

                let new_trans_lt = account_state.last_trans_lt as u64;
                if last_trans_lt >= new_trans_lt {
                    continue;
                }

                let gen_utime = account_state.gen_utime as u32;
                let mut current_trans_lt = new_trans_lt;
                let mut current_trans_hash = account_state.last_trans_hash.clone();

                {
                    let mut known_state = subscription.known_state.write().await;
                    *known_state = account_state;
                }

                let mut pending_messages = subscription.pending_messages.write().await;

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
                        Ok(transactions) => transactions,
                        Err(e) => {
                            log::error!("error during account subscription loop. {}", e);
                            continue 'subscription_loop;
                        }
                    };

                    if transactions.is_empty() {
                        break 'process_transactions;
                    }

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

                        if let Some(in_msg) = &transaction.in_msg {
                            if let Some(pending_message) = pending_messages.remove(&in_msg.hash()) {
                                let result =
                                    process_transaction(&transaction, pending_message.abi.as_ref());
                                let _ = pending_message.tx.send(result);
                            }
                        }

                        if transaction.prev_trans_lt < last_trans_lt {
                            break 'process_transactions;
                        }
                    }
                }

                pending_messages.retain(|_, message| message.expires_at <= gen_utime);

                last_trans_lt = current_trans_lt;
            }
        });
    }
}

struct PendingMessage {
    expires_at: u32,
    previous_known_lt: u64,
    abi: Arc<Function>,
    tx: oneshot::Sender<TransportResult<ContractOutput>>,
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
    use std::str::FromStr;

    const MAINNET_CONFIG: &str = r#"{
      "liteservers": [
        {
          "ip": 916349379,
          "port": 3031,
          "id": {
            "@type": "pub.ed25519",
            "key": "uNRRL+6enQjuiZ/s6Z+vO7yxUUR7uxdfzIy+RxkECrc="
          }
        }
      ],
      "validator": {
        "@type": "validator.config.global",
        "zero_state": {
          "workchain": -1,
          "shard": -9223372036854775808,
          "seqno": 0,
          "root_hash": "WP/KGheNr/cF3lQhblQzyb0ufYUAcNM004mXhHq56EU=",
          "file_hash": "0nC4eylStbp9qnCq8KjDYb789NjS25L5ZA1UQwcIOOQ="
        }
      }
    }"#;

    fn elector_addr() -> MsgAddressInt {
        MsgAddressInt::from_str(
            "-1:17519bc2a04b6ecf7afa25ba30601a4e16c9402979c236db13e1c6f3c4674e8c",
        )
        .unwrap()
    }

    async fn make_transport() -> TonlibTransport {
        TonlibTransport::new(Config {
            network_config: MAINNET_CONFIG.to_string(),
            network_name: "mainnet".to_string(),
            verbosity: 4,
            keystore: KeystoreType::InMemory,
            last_block_threshold_sec: 1,
        })
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn test_init() {
        let transport = make_transport().await;

        let account_state = transport.get_account_state(&elector_addr()).await.unwrap();
        println!("Account state: {:?}", account_state);
    }
}
