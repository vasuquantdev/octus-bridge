use ethabi::Address;
use secp256k1::PublicKey;
use sled::Db;
use url::Url;

use relay_eth::ws::EthListener;
use relay_ton::contracts::utils::pack_tokens;
use relay_ton::contracts::*;
use relay_ton::prelude::{MsgAddressInt, UInt256};

use crate::config::RelayConfig;
use crate::crypto::key_managment::{EthSigner, KeyData};
use crate::db_management::{
    EthQueue, EthTonConfirmationData, EthTonTransaction, StatsDb, TonQueue,
};
use crate::engine::bridge::event_configurations_listener::{
    ConfigsState, EventConfigurationsListener,
};
use crate::engine::bridge::util::map_eth_ton;
use crate::prelude::*;

pub(crate) mod event_configurations_listener;
pub mod models;
mod prelude;
mod util;

pub async fn make_bridge(
    state_manager: Db,
    config: RelayConfig,
    key_data: KeyData,
) -> Result<Arc<Bridge>, Error> {
    let transport = config.ton_config.make_transport().await?;

    let ton_contract_address = MsgAddressInt::from_str(&*config.ton_contract_address.0)
        .map_err(|e| Error::msg(e.to_string()))?;

    let (bridge_contract, bridge_contract_events) = make_bridge_contract(
        transport.clone(),
        ton_contract_address,
        key_data.ton.keypair(),
    )
    .await?;

    let eth_listener = Arc::new(
        EthListener::new(
            Url::parse(config.eth_node_address.as_str())
                .map_err(|e| Error::new(e).context("Bad url for eth_config provided"))?,
            state_manager.clone(),
            100, //todo move to config
        )
        .await?,
    );

    let eth_queue = EthQueue::new(&state_manager)?;
    let ton_queue = TonQueue::new(&state_manager)?;
    let stats_db = StatsDb::new(&state_manager)?;

    let event_configurations_listener = EventConfigurationsListener::new(
        transport,
        bridge_contract.clone(),
        eth_queue.clone(),
        ton_queue,
        stats_db,
        config.ton_operation_timeouts.clone(),
    )
    .await;

    let bridge = Arc::new(Bridge {
        eth_signer: key_data.eth,
        eth_listener,
        event_configurations_listener,
        bridge_contract,
        eth_queue,
    });

    tokio::spawn({
        let bridge = bridge.clone();
        async move { bridge.run(bridge_contract_events).await }
    });

    Ok(bridge)
}

pub struct Bridge {
    eth_signer: EthSigner,
    eth_listener: Arc<EthListener>,
    event_configurations_listener: Arc<EventConfigurationsListener>,

    bridge_contract: Arc<BridgeContract>,
    eth_queue: EthQueue,
}

impl Bridge {
    async fn run<T>(self: Arc<Self>, bridge_contract_events: T) -> Result<(), Error>
    where
        T: Stream<Item = BridgeContractEvent> + Send + Unpin + 'static,
    {
        log::info!(
            "Bridge started. Pubkey: {}",
            self.bridge_contract.pubkey().to_hex_string()
        );

        // Subscribe for new event configuration contracts
        let subscriptions = self
            .event_configurations_listener
            .start(bridge_contract_events)
            .await;

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

    /// Restart voting for failed transactions
    pub fn retry_failed(&self) {
        self.event_configurations_listener.retry_failed()
    }

    ///Sets eth height
    pub fn change_eth_height(&self, height: u64) -> Result<(), Error> {
        self.eth_listener.change_eth_height(height)?;
        Ok(())
    }

    pub async fn get_event_configurations(
        &self,
    ) -> Result<Vec<(MsgAddressInt, EthereumEventConfiguration)>, anyhow::Error> {
        let state = self.event_configurations_listener.get_state().await;
        Ok(state.eth_configs_map.values().cloned().collect())
    }

    pub async fn vote_for_ethereum_event_configuration(
        &self,
        event_configuration: &MsgAddressInt,
        voting: Voting,
    ) -> Result<(), anyhow::Error> {
        self.bridge_contract
            .update_event_configuration(event_configuration, voting)
            .await?;
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
                let check_result = eth_listener
                    .check_transaction(event.event_transaction)
                    .await;
                if let Err(e) = match check_event(&configs, check_result, &event).await {
                    Ok(_) => {
                        log::info!("Confirming transaction. Hash: {}", event.event_transaction);
                        self.event_configurations_listener
                            .enqueue_vote(EthTonTransaction::Confirm(event))
                            .await
                    }
                    Err(e) => {
                        log::warn!("Rejection: {:?}", e);
                        self.event_configurations_listener
                            .enqueue_vote(EthTonTransaction::Reject(event))
                            .await
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
            event_transaction: event.tx_hash,
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
            .expect("Fatal db error");
    }

    pub fn ton_pubkey(&self) -> UInt256 {
        self.bridge_contract.pubkey()
    }

    pub fn eth_pubkey(&self) -> PublicKey {
        self.eth_signer.pubkey()
    }
}
