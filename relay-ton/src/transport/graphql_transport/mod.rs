pub mod config;
mod node_client;

use reqwest::header::{self, HeaderMap, HeaderValue};
use reqwest::ClientBuilder;
use ton_abi::Function;
use ton_block::{
    Account, AccountState, BlockId, Deserializable, ExternalInboundMessageHeader, HashmapAugType,
    InRefValue, Message, Serializable, Transaction,
};
use ton_types::HashmapType;

pub use self::config::*;
use self::node_client::*;
use super::tvm;
use super::utils::*;
use crate::models::*;
use crate::prelude::*;
use crate::transport::errors::*;
use crate::transport::{AccountEvent, AccountSubscription, RunLocal, Transport};
use std::collections::hash_map;

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
        run_local(&self.client, abi, message).await
    }
}

#[async_trait]
impl Transport for GraphQLTransport {
    async fn subscribe(&self, addr: &str) -> TransportResult<Arc<dyn AccountSubscription>> {
        let addr = MsgAddressInt::from_str(addr).map_err(|e| TransportError::InvalidAddress)?;

        let subscription: Arc<dyn AccountSubscription> = GraphQLAccountSubscription::new(
            self.db.clone(),
            self.client.clone(),
            None,
            self.config.next_block_timeout_sec,
            addr,
        )
        .await?;

        Ok(subscription)
    }
}

struct GraphQLAccountSubscription {
    db: Db,
    client: NodeClient,
    event_notifier: watch::Receiver<AccountEvent>,
    account: MsgAddressInt,
    account_id: UInt256,
    pending_messages: RwLock<HashMap<UInt256, PendingMessage>>,
}

impl GraphQLAccountSubscription {
    async fn new(
        db: Db,
        client: NodeClient,
        max_initial_rescan_gap: Option<u32>,
        next_block_timeout: u32,
        addr: MsgAddressInt,
    ) -> TransportResult<Arc<Self>> {
        let client = client.clone();
        let known_block_id = client.get_latest_block(&addr).await?;

        // let last_trans_lt = match db.get(addr.address().storage()).map_err(|e| {
        //     TransportError::FailedToInitialize {
        //         reason: e.to_string(),
        //     }
        // })? {
        //     Some(data) => {
        //         let mut lt = 0;
        //         for (i, &byte) in data.iter().take(4).enumerate() {
        //             lt += (byte as u64) << i;
        //         }
        //         lt
        //     }
        //     None => {
        //         let known_block = client.get_block(&known_block_id).await?;
        //         let info = known_block.info.read_struct().map_err(|e| {
        //             TransportError::FailedToInitialize {
        //                 reason: e.to_string(),
        //             }
        //         })?;
        //
        //         info.start_lt()
        //     }
        // };

        let (tx, rx) = watch::channel(AccountEvent::StateChanged);

        let subscription = Arc::new(Self {
            db,
            client,
            event_notifier: rx,
            account: addr.clone(),
            account_id: addr
                .address()
                .get_slice(0, 256)
                .and_then(|mut slice| slice.get_next_bytes(32))
                .map_err(|e| TransportError::FailedToInitialize {
                    reason: e.to_string(),
                })?
                .into(),
            pending_messages: RwLock::new(HashMap::new()),
        });
        subscription.start_loop(
            tx,
            known_block_id,
            max_initial_rescan_gap,
            next_block_timeout,
        );

        Ok(subscription)
    }

    fn start_loop(
        self: &Arc<Self>,
        state_notifier: watch::Sender<AccountEvent>,
        mut last_block_id: String,
        max_interval_rescan_gap: Option<u32>,
        next_block_timeout: u32,
    ) {
        let subscription = Arc::downgrade(self);

        log::debug!("started polling account {}", self.account);

        tokio::spawn(async move {
            'subscription_loop: loop {
                let subscription = match subscription.upgrade() {
                    Some(s) => s,
                    None => return,
                };

                let next_block_id = match subscription
                    .client
                    .wait_for_next_block(&last_block_id, &subscription.account, next_block_timeout)
                    .await
                {
                    Ok(id) => id,
                    Err(e) => {
                        log::error!("failed to get next block id. {}", e);
                        continue 'subscription_loop;
                    }
                };

                log::debug!("current_block: {}", next_block_id);

                let (block, block_info) = match subscription
                    .client
                    .get_block(&next_block_id)
                    .await
                    .and_then(|block| {
                        let info = block.info.read_struct().map_err(|e| {
                            TransportError::FailedToParseBlock {
                                reason: e.to_string(),
                            }
                        })?;
                        Ok((block, info))
                    }) {
                    Ok(block) => block,
                    Err(e) => {
                        log::error!("failed to get next block data. {}", e);
                        continue 'subscription_loop;
                    }
                };

                let mut pending_messages = subscription.pending_messages.write().await;

                match block
                    .extra
                    .read_struct()
                    .and_then(|extra| extra.read_account_blocks())
                    .and_then(|account_blocks| account_blocks.get(&subscription.account_id))
                {
                    Ok(Some(data)) => {
                        let _ = state_notifier.broadcast(AccountEvent::StateChanged);

                        for item in data.transactions().iter() {
                            let transaction = match item.and_then(|(_, mut value)| {
                                InRefValue::<Transaction>::construct_from(&mut value)
                            }) {
                                Ok(transaction) => transaction.0,
                                Err(e) => {
                                    log::error!(
                                        "failed to parse account transaction. {}",
                                        e.to_string()
                                    );
                                    continue 'subscription_loop;
                                }
                            };

                            let out_messages = match parse_transaction_messages(&transaction) {
                                Ok(messages) => messages,
                                Err(e) => {
                                    log::error!("error during transaction processing. {}", e);
                                    continue 'subscription_loop;
                                }
                            };

                            if let Some(in_msg) = &transaction.in_msg {
                                if let Some(pending_message) =
                                    pending_messages.remove(&in_msg.hash())
                                {
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
                        }
                    }
                    Ok(None) => {
                        log::debug!("account state wasn't changed");
                        continue 'subscription_loop;
                    }
                    Err(e) => {
                        log::error!("failed to parse block data. {}", e.to_string());
                        continue 'subscription_loop;
                    }
                };

                pending_messages
                    .retain(|_, message| message.expires_at <= block_info.gen_utime().0);

                last_block_id = next_block_id;
            }
        });
    }
}

#[async_trait]
impl RunLocal for GraphQLAccountSubscription {
    async fn run_local(
        &self,
        abi: &Function,
        message: ExternalMessage,
    ) -> TransportResult<ContractOutput> {
        run_local(&self.client, abi, message).await
    }
}

#[async_trait]
impl AccountSubscription for GraphQLAccountSubscription {
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
            match pending_messages.entry(hash.clone()) {
                hash_map::Entry::Vacant(entry) => {
                    self.client.send_message_raw(&hash, &serialized).await?;

                    entry.insert(PendingMessage {
                        expires_at,
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
    abi: Arc<Function>,
    tx: oneshot::Sender<TransportResult<ContractOutput>>,
}

async fn run_local(
    node_client: &NodeClient,
    abi: &Function,
    message: ExternalMessage,
) -> TransportResult<ContractOutput> {
    let utime = Utc::now().timestamp() as u32; // TODO: make sure it is not used by contract. Otherwise force tonlabs to add gen_utime for account response

    let account_state = node_client.get_account_state(&message.dest).await?;

    let msg = encode_external_message(message);

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

#[cfg(test)]
mod tests {
    use super::*;

    async fn make_transport() -> GraphQLTransport {
        std::env::set_var("RUST_LOG", "relay_ton::transport::graphql_transport=debug");
        env_logger::init();

        let db = sled::Config::new().temporary(true).open().unwrap();

        GraphQLTransport::new(
            Config {
                addr: "https://main.ton.dev/graphql".to_string(),
                next_block_timeout_sec: 60,
            },
            db,
        )
        .await
        .unwrap()
    }

    const ELECTOR_ADDR: &str =
        "-1:3333333333333333333333333333333333333333333333333333333333333333";

    #[tokio::test]
    async fn create_transport() {
        let _transport = make_transport().await;
    }

    #[tokio::test]
    async fn account_subscription() {
        let transport = make_transport().await;

        let _subscription = transport.subscribe(&ELECTOR_ADDR).await.unwrap();

        tokio::time::delay_for(tokio::time::Duration::from_secs(10)).await;
    }
}
