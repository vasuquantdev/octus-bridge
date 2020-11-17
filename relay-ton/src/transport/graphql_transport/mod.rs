pub mod config;
mod subscription;

pub use config::ClientConfig;

use num_traits::Num;
use serde_json::Value;
use ton_block::{Deserializable, ExternalInboundMessageHeader, Message, Serializable};
use ton_client::ClientContext;
use ton_sdk::{AbiFunction, Block, BlockId, Contract};

use self::subscription::*;
use super::errors::*;
use super::Transport;
use crate::models::*;
use crate::prelude::*;

pub struct GraphQlTransport {
    config: ClientConfig,
    context: ClientContext,
    action_queues: Arc<RwLock<AccountActionQueues>>,
}

type AccountActionQueues = HashMap<UInt256, Arc<AccountActionQueue>>;

struct AccountActionQueue {
    account: MsgAddrStd,
}

impl GraphQlTransport {
    pub async fn new(config: ClientConfig) -> TransportResult<Self> {
        let context = ClientContext::new(ton_client::ClientConfig {
            network: ton_client::net::NetworkConfig {
                server_address: config.server_address.clone(),
                ..Default::default()
            },
            crypto: Default::default(),
            abi: Default::default(),
        })
        .await
        .map_err(|e| TransportError::FailedToInitialize {
            reason: e.to_string(),
        })?;

        Ok(Self {
            config,
            context,
            action_queues: Arc::new(Default::default()),
        })
    }

    fn listen(
        &self,
        address: &MsgAddressInt,
    ) -> TransportResult<(
        SubscriptionHandle,
        impl Stream<Item = TransportResult<ton_sdk::Transaction>> + Send,
    )> {
        let node_client = self.context

        subscribe(
            &self.node_client,
            "transactions",
            &format!(
                r#"{{ 
                    "account_addr": {{ "eq": "{}" }},
                    "tr_type": {{ "eq": 0 }}
                }}"#,
                address
            ),
            r#"boc"#,
            |value| {
                let boc = match value.get("boc") {
                    Some(Value::String(boc)) if !boc.is_empty() => boc,
                    _ => {
                        return Err(TransportError::ApiFailure {
                            reason: "invalid transaction response".to_owned(),
                        })
                    }
                };

                ton_block::Transaction::construct_from_base64(boc)
                    .and_then(ton_sdk::Transaction::try_from)
                    .map_err(|e| TransportError::ApiFailure {
                        reason: e.to_string(),
                    })
            },
        )
    }

    async fn load_contract(&self, address: &MsgAddressInt) -> TransportResult<Contract> {
        Contract::load(&self.node_client, address)
            .await
            .map_err(|e| TransportError::FailedToFetchAccountState {
                reason: e.to_string(),
            })?
            .ok_or_else(|| TransportError::AccountNotFound)
    }

    async fn run_local(
        &self,
        function: &AbiFunction,
        address: &MsgAddressInt,
        message: &Message,
    ) -> TransportResult<ContractOutput> {
        let contract = self.load_contract(address).await?;
        let messages = contract.local_call_tvm(message.clone()).map_err(|e| {
            TransportError::ExecutionError {
                reason: e.to_string(),
            }
        })?;

        process_out_messages(&messages, function)
    }
}

#[async_trait]
impl Transport for GraphQlTransport {
    async fn get_account_state(
        &self,
        addr: &MsgAddressInt,
    ) -> TransportResult<Option<AccountState>> {
        let accounts = self
            .node_client
            .query(
                ACCOUNTS_TABLE_NAME,
                &format!(r#"{{ "id": {{ "eq": "{}" }} }}"#, addr),
                ACCOUNT_FIELDS,
                None,
                Some(1),
                None,
            )
            .await
            .check_sync()?;
        let account = &accounts[0];

        if account.is_null() {
            return Ok(None);
        }

        fn parse_hex_str(value: &Value) -> TransportResult<&str> {
            value
                .as_str()
                .map(|s| s.trim_start_matches("0x"))
                .ok_or_else(|| TransportError::FailedToParseAccountState {
                    reason: "invalid schema".to_owned(),
                })
        }

        let balance =
            BigInt::from_str_radix(parse_hex_str(&account["balance"])?, 16).map_err(|e| {
                TransportError::FailedToParseAccountState {
                    reason: e.to_string(),
                }
            })?;

        let last_transaction = u64::from_str_radix(parse_hex_str(&account["last_trans_lt"])?, 16)
            .map(|last| if last != 0 { Some(last) } else { None })
            .map_err(|e| TransportError::FailedToParseAccountState {
                reason: e.to_string(),
            })?;

        Ok(Some(AccountState {
            balance,
            last_transaction,
        }))
    }

    async fn send_message(
        &self,
        abi: &AbiFunction,
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

        let cells = msg
            .write_to_new_cell()
            .map_err(|_| TransportError::FailedToSerialize)?
            .into();

        let serialized =
            ton_types::serialize_toc(&cells).map_err(|_| TransportError::FailedToSerialize)?;
        let hash = cells.repr_hash();

        let shard_block_id = self.find_last_shard_block(&message.dest).await?;

        self.node_client
            .send_message(hash.as_slice(), &serialized)
            .await
            .map_err(|e| TransportError::FailedToSendMessage {
                reason: e.to_string(),
            })?;

        self.wait_for_transaction(abi, message.header.expire, &shard_block_id.to_string())
            .await
    }
}

trait CheckSync {
    fn check_sync(self) -> TransportResult<Value>;
}

impl CheckSync for ton_types::Result<Value> {
    fn check_sync(self) -> TransportResult<Value> {
        self.map_err(|e| TransportError::ApiFailure {
            reason: e.to_string(),
        })
    }
}

fn process_out_messages(
    messages: &Vec<ton_sdk::Message>,
    abi_function: &AbiFunction,
) -> TransportResult<ContractOutput> {
    if messages.len() == 0 || !abi_function.has_output() {
        return Ok(ContractOutput {
            transaction_id: None,
            tokens: Vec::new(),
        });
    }

    for msg in messages {
        if msg.msg_type() != ton_sdk::MessageType::ExternalOutbound {
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

fn check_shard_match(shard_descr: Value, address: &MsgAddressInt) -> TransportResult<bool> {
    ton_sdk::Contract::check_shard_match(shard_descr, address).map_err(|e| {
        TransportError::ApiFailure {
            reason: e.to_string(),
        }
    })
}

fn can_retry_network_error(config: &ClientConfig, retries: u8) -> bool {
    can_retry_more(retries, config.network_retries_count)
}

fn can_retry_more(retries: u8, limit: i8) -> bool {
    limit < 0 || retries <= limit as u8
}

const BLOCK_FIELDS: &str = r#"
    id
    gen_utime
    after_split
    workchain_id
    shard
    in_msg_descr {
        msg_id
        transaction_id
    }
"#;

const ACCOUNT_FIELDS: &str = r#"
    id
    balance
    last_trans_lt
"#;

const BLOCKS_TABLE_NAME: &str = "blocks";
const ACCOUNTS_TABLE_NAME: &str = "accounts";

#[cfg(test)]
mod tests {
    use super::*;

    const LOCAL_SERVER_ADDR: &str = "http://127.0.0.1:80";
    const MAIN_SERVER_ADDR: &str = "https://main.ton.dev";

    fn elector_addr() -> MsgAddressInt {
        MsgAddressInt::from_str(
            "-1:3333333333333333333333333333333333333333333333333333333333333333",
        )
        .unwrap()
    }

    fn bridge_addr() -> MsgAddressInt {
        MsgAddressInt::from_str(
            "0:7c6a933179824c23c3f684f28df909ed13cb371f7f22a118241237d7bec1a2de",
        )
        .unwrap()
    }

    async fn make_transport(url: &str) -> GraphQlTransport {
        GraphQlTransport::new(ClientConfig {
            server_address: url.to_owned(),
            ..Default::default()
        })
        .await
        .unwrap()
    }

    #[tokio::test(threaded_scheduler)]
    async fn test_subscription() {
        use tokio::stream::StreamExt;

        let transport = make_transport(MAIN_SERVER_ADDR).await;

        let (subscription, mut rx) = transport.listen(&bridge_addr()).unwrap();

        tokio::spawn(async move {
            tokio::time::delay_for(tokio::time::Duration::from_secs(2)).await;
            std::mem::drop(subscription);
        });

        while let Some(item) = rx.next().await {
            let transaction = match item {
                Ok(transaction) => transaction,
                Err(e) => {
                    log::error!("subscription error: {}", e);
                    continue;
                }
            };

            let in_msg = match &transaction.in_msg {
                Some(in_msg) => in_msg,
                None => continue,
            };

            println!("Item: {:?}", transaction);
        }
    }

    #[tokio::test]
    async fn test_get_existing_account_state() {
        let transport = make_transport(MAIN_SERVER_ADDR).await;

        let account_state = transport.get_account_state(&elector_addr()).await.unwrap();
        println!("Account state: {:?}", account_state);
    }

    #[tokio::test]
    async fn test_local_get_last_block() {
        let transport = make_transport(LOCAL_SERVER_ADDR).await;

        let last_shard_block = transport
            .find_last_shard_block(&bridge_addr())
            .await
            .unwrap();
        println!("Last block: {}", last_shard_block);
    }

    #[tokio::test]
    async fn test_get_last_block() {
        let transport = make_transport(MAIN_SERVER_ADDR).await;

        let last_shard_block = transport
            .find_last_shard_block(&elector_addr())
            .await
            .unwrap();
        let next_block = transport
            .wait_next_block(&last_shard_block.to_string(), &elector_addr(), None)
            .await
            .unwrap();

        println!("Next block: {:#?}", next_block);
    }

}
