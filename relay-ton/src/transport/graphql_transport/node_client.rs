use std::time::Duration;

use graphql_client::*;
use reqwest::Client;
use tokio::sync::Semaphore;
use ton_block::{
    Account, AccountIdPrefixFull, AccountStuff, Block, Deserializable, Message, Serializable,
    ShardIdent, Transaction,
};

use crate::prelude::*;
use crate::transport::errors::*;
use crate::transport::TransportError::ApiFailure;

#[derive(Clone)]
pub struct NodeClient {
    client: Client,
    endpoint: String,
    concurrency_limiter: Arc<Semaphore>,
    fetch_timeout: Duration,
}

impl NodeClient {
    pub fn new(
        client: Client,
        endpoint: String,
        parallel_connections: usize,
        timeout: Duration,
    ) -> Self {
        Self {
            client,
            endpoint,
            concurrency_limiter: Arc::new(Semaphore::new(parallel_connections)),
            fetch_timeout: timeout,
        }
    }

    async fn fetch<T>(&self, params: T::Variables) -> TransportResult<T::ResponseData>
    where
        T: GraphQLQuery,
    {
        let request_body = T::build_query(params);
        let permit = self.concurrency_limiter.acquire().await;
        let response = self
            .client
            .post(&self.endpoint)
            .json(&request_body)
            .timeout(self.fetch_timeout)
            .send()
            .await
            .map_err(api_failure)?;
        let response_data = response.text().await.map_err(|e| ApiFailure {
            reason: e.to_string(),
        })?;
        drop(permit);
        let parsed_response: Response<T::ResponseData> = match serde_json::from_str(&response_data)
        {
            Ok(a) => a,
            Err(e) => {
                log::error!(
                    "Failed parsing api response: {}. Response data: {}",
                    e,
                    response_data
                );
                return Err(ApiFailure {
                    reason: e.to_string(),
                });
            }
        };
        parsed_response.data.ok_or_else(invalid_response)
    }

    async fn fetch_blocking<T>(
        &self,
        params: T::Variables,
        timeout: Duration,
    ) -> TransportResult<T::ResponseData>
    where
        T: GraphQLQuery,
    {
        let request_body = T::build_query(params);
        let response = self
            .client
            .post(&self.endpoint)
            .timeout(timeout)
            .json(&request_body)
            .send()
            .await
            .map_err(api_failure)?;
        let response_data = response.text().await.map_err(|e| ApiFailure {
            reason: e.to_string(),
        })?;
        let parsed_response: Response<T::ResponseData> = match serde_json::from_str(&response_data)
        {
            Ok(a) => a,
            Err(e) => {
                log::error!(
                    "Failed parsing api response: {}. Response data: {}",
                    e,
                    response_data
                );
                return Err(ApiFailure {
                    reason: e.to_string(),
                });
            }
        };
        parsed_response.data.ok_or_else(invalid_response)
    }

    pub async fn get_account_state(&self, addr: &MsgAddressInt) -> TransportResult<AccountStuff> {
        #[derive(GraphQLQuery)]
        #[graphql(
            schema_path = "src/transport/graphql_transport/schema.graphql",
            query_path = "src/transport/graphql_transport/query_account_state.graphql"
        )]
        struct QueryAccountState;

        let account_state = self
            .fetch::<QueryAccountState>(query_account_state::Variables {
                address: addr.to_string(),
            })
            .await?
            .accounts
            .ok_or_else(invalid_response)?
            .into_iter()
            .next()
            .and_then(|item| item.and_then(|account| account.boc))
            .ok_or(TransportError::AccountNotFound)?;

        match Account::construct_from_base64(&account_state) {
            Ok(Account::Account(account_stuff)) => Ok(account_stuff),
            Ok(_) => Err(TransportError::AccountNotFound),
            Err(e) => Err(TransportError::FailedToParseAccountState {
                reason: e.to_string(),
            }),
        }
    }

    pub async fn get_latest_block(&self, addr: &MsgAddressInt) -> TransportResult<LatestBlock> {
        #[derive(GraphQLQuery)]
        #[graphql(
            schema_path = "src/transport/graphql_transport/schema.graphql",
            query_path = "src/transport/graphql_transport/query_latest_masterchain_block.graphql"
        )]
        struct QueryLatestMasterchainBlock;

        #[derive(GraphQLQuery)]
        #[graphql(
            schema_path = "src/transport/graphql_transport/schema.graphql",
            query_path = "src/transport/graphql_transport/query_node_se_conditions.graphql"
        )]
        struct QueryNodeSeConditions;

        #[derive(GraphQLQuery)]
        #[graphql(
            schema_path = "src/transport/graphql_transport/schema.graphql",
            query_path = "src/transport/graphql_transport/query_node_se_latest_block.graphql"
        )]
        struct QueryNodeSeLatestBlock;

        let workchain_id = addr.get_workchain_id();

        let blocks = self
            .fetch::<QueryLatestMasterchainBlock>(query_latest_masterchain_block::Variables)
            .await?
            .blocks
            .ok_or_else(no_blocks_found)?;

        match blocks.into_iter().flatten().next() {
            // Common case
            Some(block) => {
                // Handle simple case when searched account is in masterchain
                if workchain_id == -1 {
                    return match (block.id, block.end_lt, block.gen_utime) {
                        (Some(id), Some(end_lt), Some(gen_utime)) => Ok(LatestBlock {
                            id,
                            end_lt: u64::from_str(&end_lt).unwrap_or_default(),
                            timestamp: gen_utime as u32,
                        }),
                        _ => Err(no_blocks_found()),
                    };
                }

                // Find account's shard block
                let shards: Vec<_> = block
                    .master
                    .and_then(|master| master.shard_hashes)
                    .ok_or_else(no_blocks_found)?;

                // Find matching shard
                for item in shards.into_iter().flatten() {
                    match (item.workchain_id, item.shard) {
                        (Some(workchain_id), Some(shard)) => {
                            if check_shard_match(workchain_id, &shard, addr)? {
                                return item
                                    .descr
                                    .and_then(|descr| {
                                        Some(LatestBlock {
                                            id: descr.root_hash?,
                                            end_lt: u64::from_str(&descr.end_lt?)
                                                .unwrap_or_default(),
                                            timestamp: descr.gen_utime? as u32,
                                        })
                                    })
                                    .ok_or_else(no_blocks_found);
                            }
                        }
                        _ => return Err(no_blocks_found()),
                    }
                }

                Err(no_blocks_found())
            }
            // Check Node SE case (without masterchain and sharding)
            None => {
                let block = self
                    .fetch::<QueryNodeSeConditions>(query_node_se_conditions::Variables {
                        workchain: workchain_id as i64,
                    })
                    .await?
                    .blocks
                    .and_then(|blocks| blocks.into_iter().flatten().next())
                    .ok_or_else(no_blocks_found)?;

                match (block.after_merge, &block.shard) {
                    (Some(after_merge), Some(shard))
                        if !after_merge && shard == "8000000000000000" => {}
                    // If workchain is sharded then it is not Node SE and missing masterchain blocks is error
                    _ => return Err(no_blocks_found()),
                }

                self.fetch::<QueryNodeSeLatestBlock>(query_node_se_latest_block::Variables {
                    workchain: workchain_id as i64,
                })
                .await?
                .blocks
                .and_then(|blocks| {
                    blocks.into_iter().flatten().next().and_then(|block| {
                        Some(LatestBlock {
                            id: block.id?,
                            end_lt: u64::from_str(&block.end_lt?).unwrap_or_default(),
                            timestamp: block.gen_utime? as u32,
                        })
                    })
                })
                .ok_or_else(no_blocks_found)
            }
        }
    }

    #[allow(dead_code)]
    pub async fn get_account_transactions(
        &self,
        addr: &MsgAddressInt,
        last_trans_lt: u64,
        limit: i64,
    ) -> TransportResult<Vec<Transaction>> {
        #[derive(GraphQLQuery)]
        #[graphql(
            schema_path = "src/transport/graphql_transport/schema.graphql",
            query_path = "src/transport/graphql_transport/query_account_transactions.graphql"
        )]
        struct QueryAccountTransactions;

        self.fetch::<QueryAccountTransactions>(query_account_transactions::Variables {
            address: addr.to_string(),
            last_transaction_lt: last_trans_lt.to_string(),
            limit,
        })
        .await?
        .transactions
        .ok_or_else(invalid_response)?
        .into_iter()
        .flatten()
        .map(|transaction| {
            let boc = transaction.boc.ok_or_else(invalid_response)?;
            Transaction::construct_from_base64(&boc).map_err(api_failure)
        })
        .collect::<Result<Vec<_>, _>>()
    }

    pub async fn wait_for_next_block(
        &self,
        current: &str,
        addr: &MsgAddressInt,
        timeout: Duration,
    ) -> TransportResult<String> {
        #[derive(GraphQLQuery)]
        #[graphql(
            schema_path = "src/transport/graphql_transport/schema.graphql",
            query_path = "src/transport/graphql_transport/query_next_block.graphql"
        )]
        struct QueryNextBlock;

        let timeout_ms = timeout.as_secs_f64() * 1000.0;
        let fetch_timeout = timeout * 2;

        let block = self
            .fetch_blocking::<QueryNextBlock>(
                query_next_block::Variables {
                    id: current.to_owned(),
                    timeout: timeout_ms,
                },
                fetch_timeout,
            )
            .await?
            .blocks
            .and_then(|blocks| blocks.into_iter().flatten().next())
            .ok_or_else(no_blocks_found)?;

        let workchain_id = block.workchain_id.ok_or_else(invalid_response)?;
        let shard = block.shard.as_ref().ok_or_else(invalid_response)?;

        match (
            block.id,
            block.after_split,
            check_shard_match(workchain_id, shard, addr)?,
        ) {
            (Some(block_id), Some(true), false) => {
                #[derive(GraphQLQuery)]
                #[graphql(
                    schema_path = "src/transport/graphql_transport/schema.graphql",
                    query_path = "src/transport/graphql_transport/query_block_after_split.graphql"
                )]
                struct QueryBlockAfterSplit;

                self.fetch_blocking::<QueryBlockAfterSplit>(
                    query_block_after_split::Variables {
                        block_id,
                        prev_id: current.to_owned(),
                        timeout: timeout_ms,
                    },
                    fetch_timeout,
                )
                .await?
                .blocks
                .and_then(|block| block.into_iter().flatten().next())
                .ok_or_else(no_blocks_found)?
                .id
                .ok_or_else(invalid_response)
            }
            (Some(block_id), _, _) => Ok(block_id),
            _ => Err(invalid_response()),
        }
    }

    pub async fn get_block(&self, id: &str) -> TransportResult<Block> {
        #[derive(GraphQLQuery)]
        #[graphql(
            schema_path = "src/transport/graphql_transport/schema.graphql",
            query_path = "src/transport/graphql_transport/query_block.graphql"
        )]
        struct QueryBlock;

        let boc = self
            .fetch::<QueryBlock>(query_block::Variables { id: id.to_owned() })
            .await?
            .blocks
            .and_then(|block| block.into_iter().flatten().next())
            .ok_or_else(no_blocks_found)?
            .boc
            .ok_or_else(invalid_response)?;

        Block::construct_from_base64(&boc).map_err(|e| TransportError::FailedToParseBlock {
            reason: e.to_string(),
        })
    }

    pub async fn get_outbound_messages(
        &self,
        addr: MsgAddressInt,
        start_lt: Option<u64>,
        end_lt: Option<u64>,
        limit: u32,
    ) -> TransportResult<Vec<OutboundMessage>> {
        #[derive(GraphQLQuery)]
        #[graphql(
            schema_path = "src/transport/graphql_transport/schema.graphql",
            query_path = "src/transport/graphql_transport/query_outbound_messages.graphql"
        )]
        struct QueryOutboundMessages;

        Ok(self
            .fetch::<QueryOutboundMessages>(query_outbound_messages::Variables {
                address: addr.to_string(),
                start_lt: start_lt.unwrap_or(0).to_string(),
                end_lt: end_lt.unwrap_or_else(u64::max_value).to_string(),
                limit: limit as i64,
            })
            .await?
            .messages
            .map(|messages| messages.into_iter().flatten())
            .ok_or_else(invalid_response)?
            .filter_map(|message| {
                let boc = message.boc.as_ref()?;
                let lt = message
                    .created_lt
                    .as_ref()
                    .and_then(|lt| u64::from_str_radix(lt.trim_start_matches("0x"), 16).ok())?;

                Some(OutboundMessage {
                    lt,
                    data: ton_block::Message::construct_from_base64(boc).map_err(|e| {
                        TransportError::FailedToParseMessage {
                            reason: e.to_string(),
                        }
                    }),
                })
            })
            .collect())
    }

    pub async fn get_outbound_messages_full(
        &self,
        addr: MsgAddressInt,
        start_lt: Option<u64>,
        end_lt: Option<u64>,
        limit: u32,
    ) -> TransportResult<Vec<OutboundMessageFull>> {
        #[derive(GraphQLQuery)]
        #[graphql(
            schema_path = "src/transport/graphql_transport/schema.graphql",
            query_path = "src/transport/graphql_transport/query_outbound_messages_full.graphql"
        )]
        struct QueryOutboundMessagesFull;

        Ok(self
            .fetch::<QueryOutboundMessagesFull>(query_outbound_messages_full::Variables {
                address: addr.to_string(),
                start_lt: start_lt.unwrap_or(0).to_string(),
                end_lt: end_lt.unwrap_or_else(u64::max_value).to_string(),
                limit: limit as i64,
            })
            .await?
            .messages
            .map(|messages| messages.into_iter().flatten())
            .ok_or_else(invalid_response)?
            .filter_map(|message| -> Option<_> {
                let src_transaction = message.src_transaction?;
                let transaction_hash = UInt256::from_str(src_transaction.id.as_ref()?).ok()?;
                let transaction_lt =
                    u64::from_str_radix(src_transaction.lt.as_ref()?.trim_start_matches("0x"), 16)
                        .ok()?;
                let timestamp = src_transaction.now? as u32;

                src_transaction.out_messages.map(|messages| {
                    messages.into_iter().flatten().enumerate().filter_map(
                        move |(i, out_message)| {
                            let boc = out_message.boc.as_ref()?;

                            let data =
                                ton_block::Message::construct_from_base64(boc).map_err(|e| {
                                    TransportError::FailedToParseMessage {
                                        reason: e.to_string(),
                                    }
                                });
                            Some(OutboundMessageFull {
                                data,
                                transaction_hash,
                                transaction_lt,
                                event_timestamp: timestamp,
                                event_index: i as u32,
                            })
                        },
                    )
                })
            })
            .flatten()
            .collect())
    }

    #[allow(dead_code)]
    pub async fn send_message(&self, message: &Message) -> TransportResult<()> {
        let cell = message
            .serialize()
            .map_err(|_| TransportError::FailedToSerialize)?;
        let id = base64::encode(&cell.repr_hash());
        let boc = base64::encode(
            &cell
                .write_to_bytes()
                .map_err(|_| TransportError::FailedToSerialize)?,
        );

        let _ = self
            .fetch::<MutationSendMessage>(mutation_send_message::Variables { id, boc })
            .await?;

        Ok(())
    }

    pub async fn send_message_raw(&self, id: &UInt256, boc: &[u8]) -> TransportResult<()> {
        let _ = self
            .fetch::<MutationSendMessage>(mutation_send_message::Variables {
                id: base64::encode(id),
                boc: base64::encode(boc),
            })
            .await?;

        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct OutboundMessage {
    pub lt: u64,
    pub data: TransportResult<ton_block::Message>,
}

#[derive(Debug, Clone)]
pub struct OutboundMessageFull {
    pub data: TransportResult<ton_block::Message>,
    pub transaction_hash: UInt256,
    pub transaction_lt: u64,
    pub event_timestamp: u32,
    pub event_index: u32,
}

#[derive(Debug, Clone)]
pub struct LatestBlock {
    pub id: String,
    pub end_lt: u64,
    pub timestamp: u32,
}

#[derive(GraphQLQuery)]
#[graphql(
    schema_path = "src/transport/graphql_transport/schema.graphql",
    query_path = "src/transport/graphql_transport/mutation_send_message.graphql"
)]
struct MutationSendMessage;

fn check_shard_match(
    workchain_id: i64,
    shard: &str,
    addr: &MsgAddressInt,
) -> TransportResult<bool> {
    let shard = u64::from_str_radix(&shard, 16).map_err(|_| TransportError::NoBlocksFound)?;

    let ident = ShardIdent::with_tagged_prefix(workchain_id as i32, shard).map_err(api_failure)?;

    Ok(ident.contains_full_prefix(&AccountIdPrefixFull::prefix(addr).map_err(api_failure)?))
}

fn api_failure<T>(e: T) -> TransportError
where
    T: std::fmt::Display,
{
    TransportError::ApiFailure {
        reason: e.to_string(),
    }
}

fn invalid_response() -> TransportError {
    TransportError::ApiFailure {
        reason: "invalid response".to_owned(),
    }
}

fn no_blocks_found() -> TransportError {
    TransportError::NoBlocksFound
}

#[cfg(test)]
mod tests {
    use reqwest::header::{self, HeaderMap, HeaderValue};

    use super::*;

    fn make_client() -> NodeClient {
        let mut default_headers = HeaderMap::new();
        default_headers.insert(
            header::CONTENT_TYPE,
            HeaderValue::from_str("application/json").unwrap(),
        );

        let client = reqwest::ClientBuilder::new()
            .default_headers(default_headers)
            .build()
            .unwrap();

        NodeClient {
            client,
            endpoint: "https://main.ton.dev/graphql".to_owned(),
            concurrency_limiter: Arc::new(Semaphore::new(10)),
            fetch_timeout: std::time::Duration::from_secs(10),
        }
    }

    fn elector_addr() -> MsgAddressInt {
        MsgAddressInt::from_str(
            "-1:3333333333333333333333333333333333333333333333333333333333333333",
        )
        .unwrap()
    }

    fn test_addr() -> MsgAddressInt {
        MsgAddressInt::from_str(
            "0:9DAA3ED732451E7A5F7856D8AE443C94BE554330964B02FEC15AC424303A860F",
        )
        .unwrap()
    }

    #[tokio::test]
    async fn get_account_state() {
        let account_state = make_client()
            .get_account_state(&elector_addr())
            .await
            .unwrap();
        println!("Account state: {:?}", account_state);
    }

    #[tokio::test]
    async fn get_latest_block_masterchain() {
        let latest_block = make_client()
            .get_latest_block(&elector_addr())
            .await
            .unwrap();
        println!("Latest masterchain block: {:?}", latest_block);
    }

    #[tokio::test]
    async fn get_latest_block_basechain() {
        let latest_block = make_client().get_latest_block(&test_addr()).await.unwrap();
        println!("Latest basechain block: {:?}", latest_block);
    }

    #[tokio::test]
    async fn get_next_block_masterchain() {
        let client = make_client();

        let latest_block = client.get_latest_block(&elector_addr()).await.unwrap();
        let next_block = client
            .wait_for_next_block(
                &latest_block.id,
                &elector_addr(),
                std::time::Duration::from_secs(60),
            )
            .await
            .unwrap();
        println!("Next block masterchain: {:?}", next_block);
    }

    #[tokio::test]
    async fn get_next_block_basechain() {
        let client = make_client();

        let latest_block = client.get_latest_block(&test_addr()).await.unwrap();
        let next_block = client
            .wait_for_next_block(
                &latest_block.id,
                &test_addr(),
                std::time::Duration::from_secs(60),
            )
            .await
            .unwrap();
        println!("Next block basechain: {:?}", next_block);
    }

    #[tokio::test]
    async fn get_block_by_id() {
        let block = make_client()
            .get_block("27b0882bec89de2e2e1638c1c6151ba56ecb270f8c7990c45ce3e5a812888b11")
            .await
            .unwrap();
        println!("Block: {:?}", block);
    }

    #[tokio::test]
    async fn send_message() {
        let mut message_header = ton_block::ExternalInboundMessageHeader::default();
        message_header.dst = elector_addr();

        let msg = ton_block::Message::with_ext_in_header(message_header);

        make_client().send_message(&msg).await.unwrap();
    }
}
