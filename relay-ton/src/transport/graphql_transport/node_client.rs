use graphql_client::*;
use reqwest::Client;
use ton_block::{Account, AccountStuff, Block, Deserializable};

use crate::prelude::*;
use crate::transport::errors::*;
use crate::transport::utils::*;

#[derive(Clone)]
pub struct NodeClient {
    client: Client,
    endpoint: String,
}

impl NodeClient {
    pub fn new(client: &Client, endpoint: &str) -> Self {
        Self {
            client: client.clone(),
            endpoint: endpoint.to_owned(),
        }
    }

    async fn fetch<T>(&self, params: &T::Variables) -> TransportResult<T::ResponseData>
    where
        T: GraphQLQuery,
    {
        let response = self
            .client
            .post(&self.endpoint)
            .json(params)
            .send()
            .await
            .map_err(|e| TransportError::ApiFailure {
                reason: e.to_string(),
            })?;

        response
            .json::<T::ResponseData>()
            .await
            .map_err(|e| TransportError::ApiFailure {
                reason: e.to_string(),
            })
    }

    pub async fn get_account_state(&self, addr: &str) -> TransportResult<AccountStuff> {
        #[derive(GraphQLQuery)]
        #[graphql(
            schema_path = "src/transport/graphql_transport/schema.graphql",
            query_path = "src/transport/graphql_transport/query_account_state.graphql"
        )]
        struct AccountStateQuery;

        let account_state = self
            .fetch::<AccountStateQuery>(&account_state_query::Variables {
                address: addr.to_owned(),
            })
            .await?
            .accounts
            .ok_or_else(|| TransportError::ApiFailure {
                reason: "invalid accounts response".to_string(),
            })?
            .into_iter()
            .next()
            .and_then(|item| item.and_then(|account| account.boc))
            .ok_or_else(|| TransportError::AccountNotFound)?;

        match Account::construct_from_base64(&account_state) {
            Ok(Account::Account(account_stuff)) => Ok(account_stuff),
            Ok(_) => Err(TransportError::AccountNotFound),
            Err(e) => Err(TransportError::FailedToParseAccountState {
                reason: e.to_string(),
            }),
        }
    }

    pub async fn get_latest_block(&self, addr: &str) -> TranportResult<Block> {}
}

#[derive(GraphQLQuery)]
#[graphql(
    schema_path = "src/transport/graphql_transport/schema.graphql",
    query_path = "src/transport/graphql_transport/query_account_transactions.graphql"
)]
pub struct AccountTransactionsQuery;

#[derive(GraphQLQuery)]
#[graphql(
    schema_path = "src/transport/graphql_transport/schema.graphql",
    query_path = "src/transport/graphql_transport/query_block.graphql"
)]
pub struct QueryBlock;

#[derive(GraphQLQuery)]
#[graphql(
    schema_path = "src/transport/graphql_transport/schema.graphql",
    query_path = "src/transport/graphql_transport/query_latest_masterchain_block.graphql"
)]
pub struct QueryLatestMasterchainBlock;

#[derive(GraphQLQuery)]
#[graphql(
    schema_path = "src/transport/graphql_transport/schema.graphql",
    query_path = "src/transport/graphql_transport/query_node_se_conditions.graphql"
)]
pub struct QueryNodeSEConditions;

#[derive(GraphQLQuery)]
#[graphql(
    schema_path = "src/transport/graphql_transport/schema.graphql",
    query_path = "src/transport/graphql_transport/query_node_se_latest_block.graphql"
)]
pub struct QueryNodeSELatestBlock;
