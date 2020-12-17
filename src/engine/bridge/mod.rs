use std::sync::Arc;

use anyhow::{anyhow, Error};
use ethabi::Address;
use futures::stream::Stream;
use futures::StreamExt;
use num_traits::cast::ToPrimitive;
use secp256k1::PublicKey;
use sled::Db;
use ton_block::MsgAddrStd;

use relay_eth::ws::{EthListener, H256};
use relay_ton::contracts::utils::pack_tokens;
use relay_ton::contracts::*;
use relay_ton::prelude::{MsgAddressInt, UInt256};
use relay_ton::transport::Transport;

use crate::crypto::key_managment::EthSigner;
use crate::db_managment::{EthQueue, EthTonConfirmationData, EthTonTransaction, StatsDb, TonQueue};
use crate::engine::bridge::event_configurations_listener::{
    ConfigsState, EventConfigurationsListener,
};
use crate::engine::bridge::event_votes_listener::EventVotesListener;
use crate::engine::bridge::util::map_eth_ton;

pub(crate) mod event_configurations_listener;

mod event_votes_listener;
pub mod models;
mod prelude;
mod util;

pub struct Bridge {
    eth_signer: EthSigner,
    eth_listener: Arc<EthListener>,
    ton_transport: Arc<dyn Transport>,
    event_votes_listener: Arc<EventVotesListener>,
    event_configurations_listener: Arc<EventConfigurationsListener>,

    ton_client: Arc<BridgeContract>,
    eth_queue: EthQueue,
}

impl Bridge {
    pub fn new(
        eth_signer: EthSigner,
        eth_client: Arc<EthListener>,
        ton_client: Arc<BridgeContract>,
        ton_transport: Arc<dyn Transport>,
        db: Db,
    ) -> Result<Self, Error> {
        let eth_queue = EthQueue::new(&db)?;
        let ton_queue = TonQueue::new(&db)?;
        let stats_db = StatsDb::new(&db)?;

        let event_votes_listener = EventVotesListener::new(
            ton_client.clone(),
            eth_queue.clone(),
            ton_queue.clone(),
            stats_db.clone(),
        );
        let event_configurations_listener = EventConfigurationsListener::new();

        Ok(Self {
            eth_signer: eth_signer,
            eth_listener: eth_client,

            ton_transport,
            event_votes_listener,
            event_configurations_listener,

            ton_client,
            eth_queue,
        })
    }

    pub async fn run(self: Arc<Self>) -> Result<(), Error> {
        log::info!(
            "Bridge started. Pubkey: {}",
            self.ton_client.pubkey().to_hex_string()
        );

        // Subscribe for new event configuration contracts
        let (subscriptions, ton_events) = self
            .event_configurations_listener
            .start(self.ton_transport.clone(), self.ton_client.clone())
            .await;

        // Start event votes listener
        tokio::spawn(self.event_votes_listener.clone().watch(ton_events));

        // Subscribe for ETH blocks and events
        let (blocks_rx, mut eth_events_rx) = self.eth_listener.start(subscriptions).await?;

        // Spawn pending confirmations queue processing
        tokio::spawn(self.clone().watch_pending_confirmations(blocks_rx));

        // Enqueue new events from ETH
        while let Some(event) = eth_events_rx.next().await {
            let event: relay_eth::ws::Event = match event {
                Ok(event) => event,
                Err(e) => {
                    log::error!("Failed parsing data from ethereum stream: {:?}", e);
                    continue;
                }
            };

            tokio::spawn(self.clone().process_eth_event(event));
        }

        // Done
        Ok(())
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

    fn check_suspicious_event(self: Arc<Self>, event: EthTonConfirmationData) {
        async fn check_event(
            configs: &ConfigsState,
            check_result: Result<(Address, Vec<u8>), Error>,
            event: &EthTonConfirmationData,
        ) -> Result<(), Error> {
            let (address, data) = check_result?;
            match configs.address_topic_map.get(&address) {
                None => Err(anyhow!(
                    "We have no info about {} to get abi. Rejecting transaction",
                    address
                )),
                Some((_, eth_abi, ton_abi)) => {
                    // Decode event data
                    let got_tokens: Vec<ethabi::Token> =
                        util::parse_ton_event_data(&eth_abi, &ton_abi, event.event_data.clone())
                            .map_err(|e| {
                                e.context("Failed decoding other relay data as eth types")
                            })?;

                    let expected_tokens = ethabi::decode(eth_abi, &data).map_err(|e| {
                        Error::from(e).context(
                            "Can not verify data, that other relay sent. Assuming it's fake.",
                        )
                    })?;

                    if got_tokens == expected_tokens {
                        Ok(())
                    } else {
                        Err(anyhow!(
                            "Decoded tokens are not equal with that other relay "
                        ))
                    }
                }
            }
        }

        tokio::spawn(async {
            let configs = self.event_configurations_listener.get_state().await.clone();
            let eth_listener = self.eth_listener.clone();
            async move {
                let hash = H256::from_slice(&event.event_transaction); //FIXME !!!!!!!
                let check_result = eth_listener.check_transaction(hash).await;
                if let Err(e) = match check_event(&configs, check_result, &event).await {
                    Ok(_) => {
                        log::info!("Confirming tranaction. Hash: {}", hash);
                        self.event_votes_listener
                            .spawn_vote(EthTonTransaction::Confirm(event))
                    }
                    Err(e) => {
                        log::warn!("Rejection: {:?}", e);
                        self.event_votes_listener
                            .spawn_vote(EthTonTransaction::Reject(event))
                    }
                } {
                    log::error!("Critical error while spawning vote: {:?}", e)
                }
            }
            .await
        });
    }

    async fn watch_pending_confirmations<S>(self: Arc<Self>, mut blocks_rx: S)
    where
        S: Stream<Item = u64> + Unpin,
    {
        log::debug!("Started watch_unsent_eth_ton_transactions");

        while let Some(block_number) = blocks_rx.next().await {
            log::debug!("New block: {}", block_number);
            let prepared_blocks = self.eth_queue.get_prepared_blocks(block_number).await;

            for (entry, event) in prepared_blocks {
                log::debug!(
                    "Found unconfirmed data in block {}: {}",
                    block_number,
                    hex::encode(&event.event_transaction)
                );
                self.clone().check_suspicious_event(event);
                entry.remove().unwrap();
            }
        }
    }

    async fn process_eth_event(self: Arc<Self>, event: relay_eth::ws::Event) {
        log::info!(
            "Received event from address: {}. Tx hash: {}.",
            &event.address,
            &event.tx_hash
        );

        // Extend event info
        let (ethereum_event_configuration_address, ethereum_event_blocks_to_confirm, topic_tokens) = {
            let state = self.event_configurations_listener.get_state().await;

            // Find suitable event configuration
            let (config_addr, event_config) = match state.eth_configs_map.get(&event.address) {
                Some(data) => data,
                None => {
                    log::error!("FATAL ERROR. Failed mapping event_configuration with address");
                    return;
                }
            };

            // Decode event data
            let decoded_data: Option<Result<Vec<ethabi::Token>, _>> = event
                .topics
                .iter()
                .map(|topic_id| state.topic_abi_map.get(topic_id))
                .filter_map(|x| x)
                .map(|x| ethabi::decode(x, &event.data))
                // Taking first element, cause topics and abi shouldn't overlap more than once
                .next();

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

        // Prepare confirmation

        let ton_data: Vec<_> = topic_tokens.into_iter().map(map_eth_ton).collect();
        let event_data = match pack_tokens(ton_data) {
            Ok(a) => a,
            Err(e) => {
                log::error!("Failed mapping ton_data to cell: {}", e);
                return;
            }
        };

        let prepared_data = EthTonConfirmationData {
            event_transaction: event.tx_hash.0.to_vec(),
            event_index: event.event_index,
            event_data,
            event_block_number: event.block_number,
            event_block: event.block_hash,
            ethereum_event_configuration_address,
        };

        let target_block_number = event.block_number + ethereum_event_blocks_to_confirm;

        log::info!(
            "Inserting transaction for block {} with queue number: {}",
            event.block_number,
            target_block_number
        );
        self.eth_queue
            .insert(target_block_number, &prepared_data)
            .await
            .unwrap();
    }

    pub fn ton_pubkey(&self) -> UInt256 {
        self.ton_client.pubkey()
    }

    pub fn eth_pubkey(&self) -> PublicKey {
        self.eth_signer.pubkey()
    }
}
