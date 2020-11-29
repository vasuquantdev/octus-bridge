pub mod config;
mod node_client;

use reqwest::header::{self, HeaderMap, HeaderValue};
use reqwest::{Client, ClientBuilder};
use ton_abi::Function;
use ton_block::{Account, AccountState, Deserializable, ExternalInboundMessageHeader, Message};

pub use self::config::*;
use self::node_client::*;
use super::tvm;
use super::utils::*;
use crate::models::*;
use crate::prelude::*;
use crate::transport::errors::*;
use crate::transport::{AccountSubscription, RunLocal, Transport};

pub struct GraphQLTransport {
    db: Db,
    client: NodeClient,
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

        let client = NodeClient::new(client, config.addr.clone());

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

        let utime = Utc::now().timestamp() as u32; // TODO: make sure it is not used by contract. Otherwise force tonlabs to add gen_utime for account response

        let account_state = self.client.get_account_state(&message.dest).await?;

        let (messages, _) = tvm::call_msg(
            utime,
            account_state.storage.last_trans_lt,
            account_state,
            &msg,
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
impl Transport for GraphQLTransport {
    async fn subscribe(&self, addr: &str) -> TransportResult<Arc<dyn AccountSubscription>> {
        todo!()
    }
}
