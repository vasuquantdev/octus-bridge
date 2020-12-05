use graphql_client::*;
use reqwest::Client;
use ton_block::{
    Account, AccountIdPrefixFull, AccountStuff, Block, Deserializable, Message, Serializable,
    ShardIdent, Transaction,
};

use crate::prelude::*;
use crate::transport::errors::*;

#[derive(Clone)]
pub struct NodeClient {
    client: Client,
    endpoint: String,
}

impl NodeClient {
    pub fn new(client: Client, endpoint: String) -> Self {
        Self { client, endpoint }
    }

    async fn fetch<T>(&self, params: T::Variables) -> TransportResult<T::ResponseData>
    where
        T: GraphQLQuery,
    {
        let request_body = T::build_query(params);

        let response = self
            .client
            .post(&self.endpoint)
            .json(&request_body)
            .send()
            .await
            .map_err(api_failure)?;

        let response = response
            .json::<Response<T::ResponseData>>()
            .await
            .map_err(api_failure)?;

        if let Some(errors) = response.errors {
            log::error!("api errors: {:?}", errors);
        }

        response.data.ok_or_else(invalid_response)
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

    pub async fn get_latest_block(&self, addr: &MsgAddressInt) -> TransportResult<String> {
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
        struct QueryNodeSEConditions;

        #[derive(GraphQLQuery)]
        #[graphql(
            schema_path = "src/transport/graphql_transport/schema.graphql",
            query_path = "src/transport/graphql_transport/query_node_se_latest_block.graphql"
        )]
        struct QueryNodeSELatestBlock;

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
                    return block.id.ok_or_else(no_blocks_found);
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
                                    .and_then(|descr| descr.root_hash)
                                    .ok_or_else(no_blocks_found);
                            }
                        }
                        _ => return Err(TransportError::NoBlocksFound),
                    }
                }

                Err(TransportError::NoBlocksFound)
            }
            // Check Node SE case (without masterchain and sharding)
            None => {
                let block = self
                    .fetch::<QueryNodeSEConditions>(query_node_se_conditions::Variables {
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
                    _ => return Err(TransportError::NoBlocksFound),
                }

                self.fetch::<QueryNodeSELatestBlock>(query_node_se_latest_block::Variables {
                    workchain: workchain_id as i64,
                })
                .await?
                .blocks
                .and_then(|blocks| {
                    blocks
                        .into_iter()
                        .flatten()
                        .next()
                        .and_then(|block| block.id)
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
        timeout: u32,
    ) -> TransportResult<String> {
        #[derive(GraphQLQuery)]
        #[graphql(
            schema_path = "src/transport/graphql_transport/schema.graphql",
            query_path = "src/transport/graphql_transport/query_next_block.graphql"
        )]
        struct QueryNextBlock;

        let timeout = (timeout * 1000) as f64; // timeout in ms

        let block = self
            .fetch::<QueryNextBlock>(query_next_block::Variables {
                id: current.to_owned(),
                timeout,
            })
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

                self.fetch::<QueryBlockAfterSplit>(query_block_after_split::Variables {
                    block_id,
                    prev_id: current.to_owned(),
                    timeout,
                })
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
    ) -> TransportResult<Vec<(u64, TransportResult<Message>)>> {
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

                Some((
                    lt,
                    ton_block::Message::construct_from_base64(boc).map_err(|e| {
                        TransportError::FailedToParseMessage {
                            reason: e.to_string(),
                        }
                    }),
                ))
            })
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
            .wait_for_next_block(&latest_block, &elector_addr(), 60)
            .await
            .unwrap();
        println!("Next block masterchain: {:?}", next_block);
    }

    #[tokio::test]
    async fn get_next_block_basechain() {
        let client = make_client();

        let latest_block = client.get_latest_block(&test_addr()).await.unwrap();
        let next_block = client
            .wait_for_next_block(&latest_block, &test_addr(), 60)
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
