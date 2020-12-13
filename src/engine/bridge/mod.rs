pub(crate) mod event_configurations_listener;

mod event_votes_listener;
pub mod models;
mod prelude;
mod transactions_monitoring;
mod util;

use std::collections::HashSet;
use std::iter::FromIterator;
use std::sync::Arc;

use anyhow::Error;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use num_traits::cast::ToPrimitive;
use sled::Db;
use tokio::sync;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::time::Duration;
use ton_block::MsgAddrStd;

use relay_eth::ws::{EthListener, H256};
use relay_ton::contracts::errors::ContractError;
use relay_ton::contracts::utils::pack_tokens;
use relay_ton::contracts::*;
use relay_ton::prelude::{BigUint, Future, MsgAddressInt};
use relay_ton::transport::Transport;

use crate::crypto::key_managment::EthSigner;
use crate::db_managment::eth_queue::EthQueue;
use crate::db_managment::models::EthTonConfirmationData;
use crate::db_managment::ton_db::TonTree;
use crate::engine::bridge::event_configurations_listener::EventConfigurationsListener;
use crate::engine::bridge::event_votes_listener::EventVotesListener;
use crate::engine::bridge::models::ExtendedEventInfo;
use crate::engine::bridge::util::map_eth_ton;

pub struct Bridge {
    eth_signer: EthSigner,
    eth_client: EthListener,
    eth_queue: EthQueue,

    ton_client: Arc<BridgeContract>,
    ton_transport: Arc<dyn Transport>,
    event_votes_listener: Arc<EventVotesListener>,
    event_configurations_listener: Arc<EventConfigurationsListener>,
    db: Db,
}

impl Bridge {
    pub fn new(
        eth_signer: EthSigner,
        eth_client: EthListener,
        ton_client: Arc<BridgeContract>,
        ton_transport: Arc<dyn Transport>,
        db: Db,
    ) -> Self {
        let eth_queue = EthQueue::new(&db).unwrap();
        let event_votes_listener = EventVotesListener::new(&db, ton_client.pubkey()).unwrap();
        let event_configurations_listener = EventConfigurationsListener::new();

        Self {
            eth_signer,
            eth_client,
            eth_queue,

            ton_client,
            ton_transport,
            event_votes_listener,
            event_configurations_listener,
            db,
        }
    }

    pub async fn run(&self) -> Result<(), anyhow::Error> {
        log::info!("Bridge started");

        let events_rx = self
            .event_configurations_listener
            .start(self.ton_transport.clone(), self.ton_client.clone())
            .await;

        // TODO: subscribe to new configs after they are added
        let (eth_addr, eth_topic): (Vec<_>, Vec<_>) = {
            let state = self.event_configurations_listener.get_state().await;
            (
                state.eth_addr.clone().into_iter().collect(),
                state.topic_abi_map.keys().cloned().collect(),
            )
        };

        log::info!("Got configs for ethereum.");
        log::debug!("Topics: {:?}", eth_addr);
        log::debug!("Ethereum address: {:?}", eth_topic);

        let (confirmed_events_tx, confirmed_events_rx) = sync::broadcast::channel(255);
        {
            let ton_watcher = self.event_votes_listener.clone();
            tokio::spawn(ton_watcher.watch(events_rx, confirmed_events_tx));
        }

        //
        let eth_queue = EthQueue::new(&self.db).unwrap();
        {
            let eth_queue = eth_queue.clone();
            let new_ethereum_blocks_rx = self
                .eth_client
                .get_blocks_stream()
                .await
                .expect("Shouldn't fail");
            let ton_queue = TonTree::new(&self.db).unwrap();
            let bridge = self.ton_client.clone();

            tokio::spawn(async move {
                Self::watch_unsent_eth_ton_transactions(
                    new_ethereum_blocks_rx,
                    eth_queue,
                    ton_queue,
                    bridge,
                )
                .await;
            });
        }

        //
        let mut eth_events_stream = self.eth_client.subscribe(eth_addr, eth_topic).await?;
        log::info!("Subscribed on eth logs");
        dbg!(self.event_configurations_listener.get_state().await);

        while let Some(event) = eth_events_stream.next().await {
            let event: relay_eth::ws::Event = match event {
                Ok(event) => event,
                Err(e) => {
                    log::error!("Failed parsing data from ethereum stream: {:?}", e);
                    continue;
                }
            };

            self.handle_eth_event(event).await
        }

        Ok(())
    }

    async fn handle_eth_event(&self, event: relay_eth::ws::Event) {
        log::info!(
            "Received event from address: {}. Tx hash: {}.",
            &event.address,
            &event.tx_hash
        );

        let (ethereum_event_configuration_address, ethereum_event_blocks_to_confirm, topic_tokens) = {
            let state = self.event_configurations_listener.get_state().await;

            let (config_addr, event_config) = match state.eth_configs_map.get(&event.address) {
                Some(data) => data,
                None => {
                    log::error!("FATAL ERROR. Failed mapping event_configuration with address");
                    return;
                }
            };

            let decoded_data: Option<Result<Vec<ethabi::Token>, _>> = event
                .topics
                .iter()
                .map(|topic_id| state.topic_abi_map.get(topic_id))
                .filter_map(|x| x)
                .map(|x| ethabi::decode(x, &event.data))
                .next();

            //taking first element, cause topics and abi shouldn't overlap more than once
            let topic_tokens = match decoded_data {
                Some(a) => match a {
                    Ok(a) => a,
                    Err(e) => {
                        log::error!("Failed decoding data from event: {}", e);
                        return;
                    }
                },
                None => {
                    log::error!("No data from event could be parsed");
                    return;
                }
            };

            (
                config_addr.clone(),
                event_config
                    .ethereum_event_blocks_to_confirm
                    .to_u64()
                    .unwrap_or_else(u64::max_value),
                topic_tokens,
            )
        };

        let event_transaction = event.tx_hash.0.to_vec();
        let event_index = event.event_index.into();
        let ton_data: Vec<_> = topic_tokens.into_iter().map(map_eth_ton).collect();
        let event_data = match pack_tokens(ton_data) {
            Ok(a) => a,
            Err(e) => {
                log::error!("Failed mapping ton_data to cell: {}", e);
                return;
            }
        };
        let event_block_number: BigUint = event.block_number.into();
        let event_block = event.block_hash;

        // if some relay has already reported transaction
        if ton_watcher
            .get_event_by_hash(&event.tx_hash)
            .unwrap()
            .is_some()
        {
            self.ton_client
                .confirm_ethereum_event(
                    event_transaction,
                    event_index,
                    event_data,
                    event_block_number,
                    event_block,
                    ethereum_event_configuration_address,
                )
                .await
                .unwrap();

            log::info!("Confirmed other relay transaction: {}", event.tx_hash);
            ton_watcher.remove_event_by_hash(&event.tx_hash).unwrap();
            return;
        }

        let prepared_data = EthTonConfirmationData {
            event_transaction,
            event_index,
            event_data,
            event_block_number,
            event_block,
            ethereum_event_configuration_address,
        };

        let queued_block_number = event.block_number + ethereum_event_blocks_to_confirm;

        let transactions_in_block = match eth_queue.get(queued_block_number).unwrap() {
            Some(mut transactions_list) => {
                transactions_list.insert(prepared_data);
                transactions_list
            }
            None => HashSet::from_iter(vec![prepared_data]),
        };

        log::info!(
            "Inserting transaction for block {} with queue number: {}",
            event.block_number,
            queued_block_number
        );
        eth_queue
            .insert(&queued_block_number, &transactions_in_block)
            .unwrap();
    }

    pub async fn get_event_configurations(
        &self,
    ) -> Result<Vec<(MsgAddressInt, EthereumEventConfiguration)>, anyhow::Error> {
        let state = self.event_configurations_listener.get_state().await;
        Ok(state.eth_configs_map.values().cloned().collect())
    }

    pub async fn start_voting_for_new_event_configuration(
        &self,
        new_configuration: NewEventConfiguration,
    ) -> Result<MsgAddrStd, anyhow::Error> {
        let address = self
            .ton_client
            .add_ethereum_event_configuration(new_configuration)
            .await?;

        Ok(address)
    }

    pub async fn vote_for_new_event_configuration(
        &self,
        address: &MsgAddressInt,
        voting: Voting,
    ) -> Result<(), anyhow::Error> {
        match voting {
            Voting::Confirm => {
                self.ton_client
                    .confirm_ethereum_event_configuration(address)
                    .await?
            }
            Voting::Reject => {
                self.ton_client
                    .reject_ethereum_event_configuration(address)
                    .await?
            }
        };
        Ok(())
    }

    async fn watch_unsent_eth_ton_transactions(
        mut new_ethereum_blocks_rx: UnboundedReceiver<u64>,
        eth_queue: EthQueue,
        ton_queue: TonTree,
        bridge: Arc<BridgeContract>,
    ) {
        log::debug!("Started watch_unsent_eth_ton_transactions");

        while let Some(block_number) = new_ethereum_blocks_rx.next().await {
            log::debug!("New block: {}", block_number);

            let prepared_data = match eth_queue.get(block_number) {
                Ok(value) => match value {
                    Some(data) => {
                        log::debug!("Found unconfirmed data in block {}", block_number);
                        data
                    }
                    None => {
                        log::debug!("No data found for block {}", block_number);
                        continue;
                    }
                },
                Err(e) => {
                    log::error!("CRITICAL ERROR. Failed getting data from eth_queue: {}", e);
                    continue;
                }
            };

            for fake_event in ton_queue.scan_for_block_lower_bound(BigUint::from(block_number)) {
                let event = fake_event.data;
                let bridge = bridge.clone();
                let ethereum_event_transaction =
                    H256::from_slice(event.ethereum_event_transaction.as_slice());

                tokio::spawn(async move {
                    bridge
                        .reject_ethereum_event(
                            event.ethereum_event_transaction, //not in other order on case of shutdown
                            event.event_index,
                            event.event_data,
                            event.event_block_number,
                            event.event_block,
                            MsgAddressInt::AddrStd(event.event_configuration_address),
                        )
                        .await
                    //todo error handling
                });

                ton_queue
                    .remove_event_by_hash(&ethereum_event_transaction)
                    .unwrap();
            }

            tokio::spawn({
                let bridge = bridge.clone();
                let eth_queue = eth_queue.clone();
                async move {
                    let mut confirmations =
                        FuturesUnordered::from_iter(prepared_data.into_iter().map(|event| async {
                            let hash = H256::from_slice(&*event.event_transaction);

                            let result = bridge
                                .confirm_ethereum_event(
                                    event.event_transaction,
                                    event.event_index,
                                    event.event_data,
                                    event.event_block_number,
                                    event.event_block,
                                    event.ethereum_event_configuration_address,
                                )
                                .await;

                            (hash, result)
                        }));

                    while let Some((hash, result)) = confirmations.next().await {
                        match result {
                            Ok(_) => {
                                log::info!("Confirmed tx in ton with hash {}", hash);
                                eth_queue.remove(block_number).unwrap()
                            }
                            Err(e) => {
                                log::error!(
                                    "Failed confirming tx with hash: {} in ton: {:?}",
                                    hash,
                                    e
                                )
                            }
                        }
                    }
                }
            });
        }
    }
}
