use std::sync::Arc;

use anyhow::Error;
use bincode::{deserialize, serialize};
use futures::StreamExt;
use log::info;
use serde::{Deserialize, Serialize};
use sled::{Db, Tree};
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::time::Duration;
use relay_eth::ws::{EthListener};

use relay_ton::contracts::utils::pack_tokens;
use relay_ton::contracts::*;
use relay_ton::prelude::{
    serde_cells, serde_int_addr, serde_std_addr, BigUint, Cell, HashMap, MsgAddrStd, MsgAddressInt,
};
use relay_ton::transport::Transport;

use crate::crypto::key_managment::EthSigner;
use crate::engine::bridge::persistent_state::TonWatcher;
use crate::engine::bridge::ton_config_listener::{ConfigListener, MappedData};
use crate::engine::bridge::util::{map_eth_ton};

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
        stream: UnboundedReceiver<u64>,
        eth_tree: Tree,
        ton_tree: Tree,
        bridge: Arc<BridgeContract>,
    ) {
        let mut stream = stream;
        while let Some(block_number) = stream.next().await {
            let prepared_data =
                match eth_tree.get(serialize(&block_number).expect("Shouldn't fail")) {
                    Ok(a) => match a {
                        Some(a) => {
                            let data = deserialize::<Vec<EthTonConfirmationData>>(&a)
                                .expect("Shouldn't fail");
                            log::debug!(
                                "Found unconfirmed data in block {}: {:#?}",
                                block_number,
                                &data
                            );
                            data
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

            for fake_event in TonWatcher::scan_for_block_lower_bound(&ton_tree, block_number.into())
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
                ton_tree.remove(&ethereum_event_transaction).unwrap();
            }

            for event in prepared_data {
                let bridge = bridge.clone();
                tokio::spawn(async move {
                    bridge
                        .confirm_ethereum_event(
                            event.event_transaction,
                            event.event_index,
                            event.event_data,
                            event.event_block_number,
                            event.event_block,
                            event.ethereum_event_configuration_address,
                        )
                        .await
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
        let listener = ConfigListener::new();
        {
            let listener = listener.clone();
            tokio::spawn(listener.run(transport.clone(), ton_client.clone()));
        }

        let MappedData {
            eth_addr,
            eth_topic,
            address_topic_map: _,
            topic_abi_map,
            eth_proxy_map,
        } = listener.get_initial_config_map().await;

        log::info!("Got config for ethereum.");
        log::debug!("Topics: {:?}", &eth_topic);
        log::debug!("Ethereum address: {:?}", &eth_addr);

        let ton_watcher = Arc::new(TonWatcher::new(db.clone(), transport.clone()).unwrap());
        {
            let ton_watcher = ton_watcher.clone();
            let receiver = listener.get_events_stream().await.unwrap();
            tokio::spawn(async move { ton_watcher.watch(receiver) });
        }

        let eth_queue = db
            .open_tree(ETH_UNCONFIRMED_TRANSACTIONS_TREE_NAME)
            .unwrap();

        let mut stream = eth_client
            .subscribe(
                eth_addr.into_iter().collect(),
                eth_topic.into_iter().collect(),
            )
            .await?;

        while let Some(a) = stream.next().await {
            let event: relay_eth::ws::Event = match a {
                Ok(a) => a,
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

            let event_config = match listener
                .get_event_configuration()
                .await
                .get(event.address.as_bytes())
            {
                Some(a) => a.clone(),
                None => {
                    log::error!("FATAL ERROR. Failed mapping event_configuration with address");
                    continue;
                }
            };

            let decoded_data: Option<Result<Vec<ethabi::Token>, _>> = event
                .topics
                .iter()
                .map(|x| topic_abi_map.get(x))
                .filter_map(|x| x)
                .map(|x| ethabi::decode(x, &event.data))
                .next();

            //taking first element, cause topics and abi shouldn't overlap more than once
            let tokens = match decoded_data {
                None => {
                    log::error!("No data from event could be parsed");
                    continue;
                }
                Some(a) => match a {
                    Ok(a) => a,
                    Err(e) => {
                        log::error!("Failed decoding data from event: {}", e);
                        continue;
                    }
                },
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
            let event_block_number = event.block_number.into();
            let event_block = event.block_hash;
            let ethereum_event_configuration_address =
                MsgAddressInt::AddrStd(match eth_proxy_map.get(&event.address) {
                    Some(a) => a.clone(),
                    None => {
                        log::error!("Can't map eth address into ton proxy contract address");
                        continue;
                    }
                });
            //if some relay already reported transaction
            match ton_watcher
                .get_event_by_hash(event.tx_hash.as_ref())
                .unwrap() // db  error
            {
                Some(_) => {
                    ton_client.confirm_ethereum_event(
                        event_transaction,
                        event_index,
                        event_data,
                        event_block_number,
                        event_block,
                        ethereum_event_configuration_address
                    ).await.unwrap();
                    log::info!("Confirmed met transaction: {}", event.tx_hash);
                    ton_watcher.drop_event_by_hash(event.tx_hash.as_ref()).unwrap();
                    continue
                },
                None=>()
            }

            let prepared_data = EthTonConfirmationData {
                event_transaction,
                event_index,
                event_data,
                event_block_number,
                event_block,
                ethereum_event_configuration_address,
            };

            let queued_block_number =
                serialize(&(event.block_number + event_config.ethereum_event_blocks_to_confirm))
                    .expect("Shouldn't fail");
            let transactions_in_block =
                serialize(&match eth_queue.get(&queued_block_number).unwrap() {
                    Some(a) => {
                        let mut transactions_list: Vec<EthTonConfirmationData> =
                            deserialize(&*a).expect("Shouldn't fail");
                        transactions_list.push(prepared_data);
                        transactions_list
                    }
                    None => vec![prepared_data],
                })
                .expect("Shouldn't fail");
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
