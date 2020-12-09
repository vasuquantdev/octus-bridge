use std::sync::Arc;

use anyhow::Error;
use futures::StreamExt;
use log::info;
use num_traits::cast::ToPrimitive;
use serde::{Deserialize, Serialize};
use sled::{Db, Tree};
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::time::Duration;

use relay_eth::ws::EthListener;
use relay_ton::contracts::utils::pack_tokens;
use relay_ton::contracts::*;
use relay_ton::prelude::{serde_cells, serde_int_addr, BigUint, Cell, MsgAddressInt};
use relay_ton::transport::Transport;

use crate::crypto::key_managment::EthSigner;
use crate::engine::bridge::persistent_state::TonWatcher;
use crate::engine::bridge::ton_config_listener::{ConfigListener, ConfigsState};
use crate::engine::bridge::util::map_eth_ton;

mod ton_config_listener;

mod models;
mod persistent_state;
mod util;

const ETH_UNCONFIRMED_TRANSACTIONS_TREE_NAME: &str = "eth_unconfirmed";

pub struct Bridge {
    eth_signer: EthSigner,
    ton_client: BridgeContract,
    eth_client: EthListener,
    ton_transport: Arc<dyn Transport>,
    db: Db,
}

#[derive(Deserialize, Serialize, Debug)]
struct EthTonConfirmationData {
    event_transaction: Vec<u8>,
    event_index: BigUint,
    #[serde(with = "serde_cells")]
    event_data: Cell,
    event_block_number: BigUint,
    event_block: Vec<u8>,
    #[serde(with = "serde_int_addr")]
    ethereum_event_configuration_address: MsgAddressInt,
}

impl Bridge {
    pub fn new(
        eth_signer: EthSigner,
        eth_client: EthListener,
        ton_client: BridgeContract,
        ton_transport: Arc<dyn Transport>,
        db: Db,
    ) -> Self {
        Self {
            eth_signer,
            ton_client,
            eth_client,
            ton_transport,
            db,
        }
    }

    async fn watch_unsent_eth_ton_transactions(
        mut new_ethereum_blocks_rx: UnboundedReceiver<u64>,
        eth_queue: Tree,
        ton_queue: Tree,
        bridge: Arc<BridgeContract>,
    ) {
        log::debug!("Started watch_unsent_eth_ton_transactions");

        while let Some(block_number) = new_ethereum_blocks_rx.next().await {
            log::debug!("New block: {}", block_number);
            let key = bincode::serialize(&block_number).expect("Shouldn't fail");

            let prepared_data = match eth_queue.get(key) {
                Ok(value) => match value {
                    Some(data) => {
                        let confirmation =
                            bincode::deserialize::<Vec<EthTonConfirmationData>>(&data)
                                .expect("Shouldn't fail");

                        log::debug!("Found unconfirmed data in block {}", block_number);
                        confirmation
                    }
                    None => {
                        log::debug!("No data found for block {}", block_number);
                        continue;
                    }
                },
                Err(e) => {
                    log::error!(
                        "CRITICAL ERROR. Failed getting data from {}: {}",
                        ETH_UNCONFIRMED_TRANSACTIONS_TREE_NAME,
                        e
                    );
                    continue;
                }
            };

            for fake_event in
                TonWatcher::scan_for_block_lower_bound(&ton_queue, block_number.into())
            {
                let event = fake_event.data;
                let bridge = bridge.clone();
                let ethereum_event_transaction = event.ethereum_event_transaction.clone();
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
                ton_queue.remove(&ethereum_event_transaction).unwrap();
            }

            for event in prepared_data {
                let bridge = bridge.clone();
                tokio::spawn(async move {
                    let hash = hex::encode(&event.event_transaction);
                    match bridge
                        .confirm_ethereum_event(
                            event.event_transaction,
                            event.event_index,
                            event.event_data,
                            event.event_block_number,
                            event.event_block,
                            event.ethereum_event_configuration_address,
                        )
                        .await
                    {
                        Ok(_) => log::info!("Confirmed tx in ton with hash {}", hash),
                        Err(e) => {
                            log::error!("Failed confirming tx with hash: {} in ton: {:?}", hash, e)
                        }
                    }
                });
                //todo error handling
            }
        }
    }

    async fn eth_listener(
        eth_client: EthListener,
        ton_client: BridgeContract,
        transport: Arc<dyn Transport>,
        db: Db,
    ) -> Result<(), Error> {
        let ton_client = Arc::new(ton_client);

        let listener = ConfigListener::new();
        let events_rx = listener.start(transport, ton_client.clone()).await;

        let (eth_addr, eth_topic): (Vec<_>, Vec<_>) = {
            let state = listener.get_state().await;
            (
                state.eth_addr.clone().into_iter().collect(),
                state.topic_abi_map.keys().cloned().collect(),
            )
        };

        log::info!("Got config for ethereum.");
        log::debug!("Topics: {:?}", eth_addr);
        log::debug!("Ethereum address: {:?}", eth_topic);

        //
        let ton_watcher = Arc::new(TonWatcher::new(db.clone()).unwrap());
        {
            let ton_watcher = ton_watcher.clone();
            tokio::spawn(async move { ton_watcher.watch(events_rx).await });
        }

        //
        let eth_queue = db
            .open_tree(ETH_UNCONFIRMED_TRANSACTIONS_TREE_NAME)
            .unwrap();

        //
        {
            let eth_queue = eth_queue.clone();
            let new_ethereum_blocks_rx = eth_client
                .get_blocks_stream()
                .await
                .expect("Shouldn't fail");
            let ton_queue = db
                .open_tree(persistent_state::PERSISTENT_TREE_NAME)
                .unwrap();
            let bridge = ton_client.clone();

            tokio::spawn(async move {
                Bridge::watch_unsent_eth_ton_transactions(
                    new_ethereum_blocks_rx,
                    eth_queue,
                    ton_queue,
                    bridge,
                )
                .await;
            });
        }

        //
        let mut stream = eth_client.subscribe(eth_addr, eth_topic).await?;
        log::info!("Subscribed on eth logs");
        dbg!(listener.get_state().await);

        while let Some(event) = stream.next().await {
            let event: relay_eth::ws::Event = match event {
                Ok(event) => event,
                Err(e) => {
                    log::error!("Failed parsing data from ethereum stream: {}", e);
                    continue;
                }
            };
            log::info!(
                "Received event from address: {}. Tx hash: {}.",
                &event.address,
                &event.tx_hash
            );

            let state = listener.get_state().await;

            let (config_addr, event_config) = match state.eth_configs_map.get(&event.address) {
                Some(data) => data,
                None => {
                    log::error!("FATAL ERROR. Failed mapping event_configuration with address");
                    continue;
                }
            };

            let decoded_data: Option<Result<Vec<ethabi::Token>, _>> = event
                .topics
                .iter()
                .map(|x| state.topic_abi_map.get(x))
                .filter_map(|x| x)
                .map(|x| ethabi::decode(x, &event.data))
                .next();

            //taking first element, cause topics and abi shouldn't overlap more than once
            let tokens = match decoded_data {
                Some(a) => match a {
                    Ok(a) => a,
                    Err(e) => {
                        log::error!("Failed decoding data from event: {}", e);
                        continue;
                    }
                },
                None => {
                    log::error!("No data from event could be parsed");
                    continue;
                }
            };

            let event_transaction = Vec::from(event.tx_hash.0);
            let event_index = event.event_index.into();
            let ton_data: Vec<_> = tokens.into_iter().map(map_eth_ton).collect();
            let event_data = match pack_tokens(ton_data) {
                Ok(a) => a,
                Err(e) => {
                    log::error!("Failed mapping ton_data to cell: {}", e);
                    continue;
                }
            };
            let event_block_number: BigUint = event.block_number.into();
            let event_block = event.block_hash;
            let ethereum_event_configuration_address = config_addr.clone();

            // if some relay has already reported transaction
            if ton_watcher
                .get_event_by_hash(event.tx_hash.as_ref())
                .unwrap()
                .is_some()
            {
                ton_client
                    .confirm_ethereum_event(
                        event_transaction,
                        event_index,
                        event_data,
                        event_block_number.clone(),
                        event_block,
                        ethereum_event_configuration_address,
                    )
                    .await
                    .unwrap();

                log::info!("Confirmed other relay transaction: {}", event.tx_hash);
                ton_watcher
                    .remove_event_by_hash(event.tx_hash.as_ref())
                    .unwrap();
                continue;
            }

            let prepared_data = EthTonConfirmationData {
                event_transaction,
                event_index,
                event_data,
                event_block_number: event_block_number.clone(),
                event_block,
                ethereum_event_configuration_address,
            };

            let queued_block_number = bincode::serialize(
                &(event.block_number
                    + event_config
                        .ethereum_event_blocks_to_confirm
                        .clone()
                        .to_u64()
                        .unwrap()),
            )
            .expect("Shouldn't fail");

            let queued_block_number_display =
                event.block_number + event_config.ethereum_event_blocks_to_confirm.clone();
            let transactions_in_block =
                bincode::serialize(&match eth_queue.get(&queued_block_number).unwrap() {
                    Some(a) => {
                        let mut transactions_list: Vec<EthTonConfirmationData> =
                            bincode::deserialize(&*a).expect("Shouldn't fail");
                        transactions_list.push(prepared_data);
                        transactions_list
                    }
                    None => vec![prepared_data],
                })
                .expect("Shouldn't fail");

            log::info!(
                "Inserting transaction for block {} with queue number: {}",
                event.block_number,
                queued_block_number_display
            );
            eth_queue
                .insert(queued_block_number, transactions_in_block)
                .unwrap();
        }
        Ok(())
    }

    pub async fn run(&self) -> Result<(), anyhow::Error> {
        info!("Bridge started");
        tokio::spawn(Self::eth_listener(
            self.eth_client.clone(),
            self.ton_client.clone(),
            self.ton_transport.clone(),
            self.db.clone(),
        ));
        tokio::time::delay_for(Duration::from_secs(8640000)).await;
        Ok(())
    }

    fn start_voting_for_update_config() {
        todo!()
    }
    fn update_config() {
        todo!()
    }
    fn start_voting_for_remove_event_type() {
        todo!()
    }
}
