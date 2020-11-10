pub mod config;

pub use config::ClientConfig;

use num_traits::Num;
use serde_json::Value;
use ton_block::{ExternalInboundMessageHeader, Message, Serializable};
use ton_sdk::{
    AbiFunction, Block, BlockId, Contract, NodeClient, NodeClientConfig, OrderBy, SortDirection,
};

use self::config::*;
use super::errors::*;
use super::Transport;
use crate::models::*;
use crate::prelude::*;

pub struct GraphQlTransport {
    config: ClientConfig,
    node_client: NodeClient,
}

impl GraphQlTransport {
    pub async fn new(config: ClientConfig) -> TransportResult<Self> {
        let node_client = NodeClient::new(NodeClientConfig {
            base_url: Some(config.server_address.clone()),
            timeouts: None,
            access_key: None,
        })
        .await
        .map_err(|e| TransportError::FailedToInitialize {
            reason: e.to_string(),
        })?;

        Ok(Self {
            config,
            node_client,
        })
    }

    async fn load_contract(&self, address: &MsgAddressInt) -> TransportResult<Contract> {
        Contract::load(&self.node_client, address)
            .await
            .map_err(|e| TransportError::FailedToFetchAccountState {
                reason: e.to_string(),
            })?
            .ok_or_else(|| TransportError::AccountNotFound)
    }

    async fn find_last_shard_block(&self, address: &MsgAddressInt) -> TransportResult<BlockId> {
        // Helpers
        fn get_block_id(blk: &Value) -> Option<BlockId> {
            blk.get("id")
                .and_then(|id| id.as_str())
                .map(|val| val.to_owned().into())
        }

        // Get latest block from masterchain
        let blocks = self
            .node_client
            .query(
                BLOCKS_TABLE_NAME,
                r#"{ "workchain_id": { "eq": -1 } }"#,
                "id master { shard_hashes { workchain_id shard descr { root_hash } } }",
                Some(OrderBy {
                    path: "seq_no".to_owned(),
                    direction: SortDirection::Descending,
                }),
                Some(1),
                None,
            )
            .await
            .check_sync()?;
        let block = &blocks[0];

        let workchain = address.get_workchain_id();

        // Check Node SE case (without masterchain and sharding)
        if block.is_null() {
            let blocks = self
                .node_client
                .query(
                    BLOCKS_TABLE_NAME,
                    &format!(r#"{{ "workchain_id": {{ "eq": {} }} }}"#, workchain),
                    "after_merge shard",
                    Some(OrderBy {
                        path: "seq_no".to_owned(),
                        direction: SortDirection::Descending,
                    }),
                    Some(1),
                    None,
                )
                .await
                .check_sync()?;
            let block = blocks.get(0).ok_or_else(|| TransportError::NoBlocksFound)?;

            // If workchain is sharded then it is not Node SE and missing masterchain blocks is error
            if block["after_merge"] == true || block["shard"] != "8000000000000000" {
                return Err(TransportError::NoBlocksFound);
            }

            // Get last block by seqno
            self.node_client
                .query(
                    BLOCKS_TABLE_NAME,
                    &format!(
                        r#"{{ "workchain_id": {{ "eq": {} }}, "shard": {{ "eq": "8000000000000000" }} }}"#,
                        workchain
                    ),
                    "id",
                    Some(OrderBy {
                        path: "seq_no".to_owned(),
                        direction: SortDirection::Descending,
                    }),
                    Some(1),
                    None,
                )
                .await
                .check_sync()?
                .get(0)
                .and_then(get_block_id)
                .ok_or_else(|| TransportError::NoBlocksFound)
        } else {
            // Handle simple case when searched account is in masterchain
            if workchain == -1 {
                return get_block_id(&block).ok_or_else(|| TransportError::NoBlocksFound);
            }

            // Find account's shard block
            let shards = block["master"]["shard_hashes"].as_array().ok_or(
                TransportError::FailedToInitialize {
                    reason: "no shard_hashes found in masterchain block".to_string(),
                },
            )?;

            let shard_block =
                ton_sdk::Contract::find_matching_shard(shards, address).map_err(|e| {
                    TransportError::ApiFailure {
                        reason: e.to_string(),
                    }
                })?;

            if shard_block.is_null() {
                return Err(TransportError::ApiFailure {
                    reason: format!(
                        "no matching shard for account {} in block {}",
                        address, block["id"]
                    ),
                });
            }

            shard_block["descr"]["root_hash"]
                .as_str()
                .map(|val| val.to_owned().into())
                .ok_or_else(|| TransportError::NoBlocksFound)
        }
    }

    async fn wait_next_block(
        &self,
        current: &str,
        address: &MsgAddressInt,
        timeout: Option<u32>,
    ) -> TransportResult<Block> {
        let block = self
            .node_client
            .wait_for(
                BLOCKS_TABLE_NAME,
                &format!(
                    r#"{{
                        "prev_ref": {{
                            "root_hash": {{ "eq": "{}" }}
                        }},
                        "OR": {{
                            "prev_alt_ref": {{
                                "root_hash": {{ "eq": "{}" }}
                            }}
                        }}
                    }}"#,
                    current, current
                ),
                BLOCK_FIELDS,
                timeout,
            )
            .await
            .check_sync()?;

        if block["after_split"] == true && !check_shard_match(block.clone(), address)? {
            self.node_client
                .wait_for(
                    BLOCKS_TABLE_NAME,
                    &format!(
                        r#"{{
                            "id": {{ "ne": "{}" }},
                            "prev_ref": {{
                                "root_hash": {{ "eq": "{}" }}
                            }}
                        }}"#,
                        block["id"], current
                    ),
                    BLOCK_FIELDS,
                    timeout,
                )
                .await
                .check_sync()
                .and_then(|value| {
                    serde_json::from_value(value).map_err(|e| TransportError::FailedToParseBlock {
                        reason: e.to_string(),
                    })
                })
        } else {
            serde_json::from_value(block).map_err(|e| TransportError::FailedToParseBlock {
                reason: e.to_string(),
            })
        }
    }

    async fn wait_next_shard_block(
        &self,
        block_id: &str,
        address: &MsgAddressInt,
        timeout: u32,
    ) -> TransportResult<Block> {
        let mut retries: u8 = 0;
        loop {
            match self.wait_next_block(block_id, address, Some(timeout)).await {
                Ok(block) => return Ok(block),
                Err(e) if !can_retry_network_error(&self.config, retries) => {
                    return Err(TransportError::FailedToFetchBlock {
                        reason: e.to_string(),
                    });
                }
                _ => {
                    tokio::time::delay_for(tokio::time::Duration::from_secs(1)).await;
                    retries = retries.checked_add(1).unwrap_or(retries);
                }
            }
        }
    }

    async fn wait_for_transaction(
        &self,
        abi: &AbiFunction,
        expires_at: u32,
        shard_block_id: &str,
    ) -> TransportResult<ContractOutput> {
        todo!()
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

    #[tokio::test]
    async fn test_get_existing_account_state() {
        let transport = GraphQlTransport::new(ClientConfig {
            server_address: MAIN_SERVER_ADDR.to_owned(),
            ..Default::default()
        })
        .await
        .unwrap();

        let account_state = transport.get_account_state(&elector_addr()).await.unwrap();
        println!("Account state: {:?}", account_state);
    }

    #[tokio::test]
    async fn test_local_get_last_block() {
        let transport = GraphQlTransport::new(ClientConfig {
            server_address: LOCAL_SERVER_ADDR.to_owned(),
            ..Default::default()
        })
        .await
        .unwrap();

        let last_shard_block = transport
            .find_last_shard_block(&bridge_addr())
            .await
            .unwrap();
        println!("Last block: {}", last_shard_block);
    }

    #[tokio::test]
    async fn test_get_last_block() {
        let transport = GraphQlTransport::new(ClientConfig {
            server_address: MAIN_SERVER_ADDR.to_owned(),
            ..Default::default()
        })
        .await
        .unwrap();

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
