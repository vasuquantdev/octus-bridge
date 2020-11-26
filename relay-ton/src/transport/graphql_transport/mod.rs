pub mod config;
mod node_client;

use reqwest::header::{self, HeaderMap, HeaderValue};
use reqwest::{Client, ClientBuilder};
use ton_abi::Function;
use ton_block::{Account, AccountState, Deserializable, ExternalInboundMessageHeader, Message};

use self::config::*;
use super::tvm;
use super::utils::*;
use crate::models::*;
use crate::prelude::*;
use crate::transport::errors::*;
use crate::transport::RunLocal;

pub struct GraphQLTransport {
    db: Db,
    client: Client,
    config: Config,
}

impl GraphQLTransport {
    pub async fn new(config: Config, db: Db) -> TransportResult<Self> {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::CONTENT_TYPE,
            HeaderValue::from_str("application/json").unwrap(),
        );

        let client_builder = ClientBuilder::new().default_headers(headers);
        let client = client_builder
            .build()
            .expect("failed to create graphql client");

        Ok(Self { db, client, config })
    }
}

#[async_trait]
impl RunLocal for GraphQLTransport {
    async fn run_local(
        &self,
        abi: &Function,
        message: ExternalMessage,
    ) -> TransportResult<ContractOutput> {
        let mut message_header = ExternalInboundMessageHeader::default();

        let mut msg = Message::with_ext_in_header(message_header);
        if let Some(body) = message.body {
            msg.set_body(body);
        }
        //
        // let request_body = account_state_query::Variables {
        //     address: message.dest.to_string(),
        // };
        //
        // let response = self
        //     .client
        //     .post(&self.config.addr)
        //     .json(&request_body)
        //     .send()
        //     .await
        //     .map_err(|err| TransportError::ApiFailure {
        //         reason: err.to_string(),
        //     })?;
        //
        // let account_state: String = response
        //     .json::<account_state_query::ResponseData>()
        //     .await
        //     .map_err(|err| TransportError::FailedToParseAccountState {
        //         reason: err.to_string(),
        //     })?
        //     .accounts
        //     .ok_or_else(|| TransportError::ApiFailure {
        //         reason: "invalid accounts response".to_string(),
        //     })?
        //     .into_iter()
        //     .next()
        //     .and_then(|item| item.and_then(|account| account.boc))
        //     .ok_or_else(|| TransportError::AccountNotFound)?;
        //
        // let info = match Account::construct_from_base64(&account_state) {
        //     Ok(Account::Account(account_stuff)) => Ok(account_stuff),
        //     Ok(_) => Err(TransportError::AccountNotFound),
        //     Err(e) => Err(TransportError::FailedToParseAccountState {
        //         reason: e.to_string(),
        //     }),
        // }?;

        //let (messages, _) = tvm::call_msg(info.storage)

        todo!()
    }
}
