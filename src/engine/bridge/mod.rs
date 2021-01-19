use ethabi::Address;
use secp256k1::PublicKey;
use sled::Db;
use tokio::time::Duration;
use url::Url;

use relay_eth::ws::{EthListener, SyncedHeight};
use relay_ton::contracts::*;
use relay_ton::prelude::MsgAddressInt;

use crate::config::RelayConfig;
use crate::crypto::key_managment::{EthSigner, KeyData};
use crate::db::*;
use crate::engine::bridge::ton_listener::{make_ton_listener, ConfigsState, TonListener};
use crate::models::*;
use crate::prelude::*;

mod prelude;
pub(crate) mod ton_listener;
mod utils;

pub async fn make_bridge(
    db: Db,
    config: RelayConfig,
    key_data: KeyData,
) -> Result<Arc<Bridge>, Error> {
    let transport = config.ton_settings.transport.make_transport().await?;

    let ton_contract_address =
        MsgAddressInt::from_str(&*config.ton_settings.bridge_contract_address.0)
            .map_err(|e| Error::msg(e.to_string()))?;

    let relay_contract_address =
        MsgAddressInt::from_str(&*config.ton_settings.relay_account_address.0)
            .map_err(|e| Error::msg(e.to_string()))
            .and_then(|address| match address {
                MsgAddressInt::AddrStd(addr) => Ok(addr),
                MsgAddressInt::AddrVar(_) => Err(anyhow!("Unsupported relay address")),
            })?;

    let (bridge_contract, bridge_contract_events) =
        make_bridge_contract(transport.clone(), ton_contract_address).await?;

    let relay_contract = make_relay_contract(
        transport.clone(),
        relay_contract_address,
        key_data.ton.keypair(),
        bridge_contract,
    )
    .await?;

    let eth_listener = Arc::new(
        EthListener::new(
            Url::parse(&config.eth_settings.node_address)
                .map_err(|e| Error::new(e).context("Bad url for eth_config provided"))?,
            db.clone(),
            config.eth_settings.tcp_connection_count,
        )
        .await?,
    );

    let eth_queue = EthQueue::new(&db)?;

    let ton_listener = make_ton_listener(
        &db,
        transport,
        relay_contract.clone(),
        eth_queue.clone(),
        key_data.eth.clone(),
        config.ton_settings.clone(),
    )
    .await?;

    let bridge = Arc::new(Bridge {
        eth_signer: key_data.eth,
        eth_listener,
        ton_listener,
        relay_contract,
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
    ton_listener: Arc<TonListener>,

    relay_contract: Arc<RelayContract>,
    eth_queue: EthQueue,
}

impl Bridge {
    async fn run<T>(self: Arc<Self>, bridge_contract_events: T) -> Result<(), Error>
    where
        T: Stream<Item = BridgeContractEvent> + Send + Unpin + 'static,
    {
        log::info!(
            "Bridge started. Relay account: {}",
            self.relay_contract.address()
        );

        // Subscribe for new event configuration contracts
        let subscriptions = self.ton_listener.start(bridge_contract_events).await;

        // Subscribe for ETH blocks and events
        let mut eth_events_rx = self.eth_listener.start(subscriptions).await?;

        // Spawn pending confirmations queue processing
        tokio::spawn(self.clone().watch_pending_confirmations());

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
        self.ton_listener.retry_failed()
    }

    ///Sets eth height
    pub async fn change_eth_height(&self, height: u64) -> Result<(), Error> {
        let actual_height = self.eth_listener.get_synced_height().await?.as_u64();
        if actual_height < height {
            return Err(anyhow::anyhow!(
                "Height provided by user is higher, then actual eth height. Cowardly refusing"
            ));
        }
        self.eth_listener.change_eth_height(height)?;
        Ok(())
    }

    pub async fn get_event_configurations(
        &self,
    ) -> Result<Vec<(BigUint, EthEventConfiguration)>, anyhow::Error> {
        let state = self.ton_listener.get_state().await;
        Ok(state.eth_configs_map.values().cloned().collect())
    }

    pub async fn vote_for_ethereum_event_configuration(
        &self,
        configuration_id: BigUint,
        voting: Voting,
    ) -> Result<(), anyhow::Error> {
        self.relay_contract
            .vote_for_event_configuration_creation(configuration_id, voting)
            .await?;
        Ok(())
    }

    fn check_suspicious_event(self: Arc<Self>, event: EthEventVotingData) {
        async fn check_event(
            configs: &ConfigsState,
            check_result: Result<(Address, Vec<u8>), Error>,
            event: &EthEventVotingData,
        ) -> Result<(), Error> {
            let (address, data) = check_result?;
            match configs.address_topic_map.get(&address) {
                None => Err(anyhow!(
                    "We have no info about {} to get abi. Rejecting transaction",
                    address
                )),
                Some((_, eth_abi, ton_abi)) => {
                    // Decode event data
                    let got_tokens: Vec<ethabi::Token> = utils::parse_eth_event_data(
                        &eth_abi,
                        &ton_abi,
                        event.event_data.clone(),
                    )
                    .map_err(|e| e.context("Failed decoding other relay data as eth types"))?;

                    let expected_tokens = ethabi::decode(eth_abi, &data).map_err(|e| {
                        Error::from(e).context(
                            "Can not verify data, that other relay sent. Assuming it's fake.",
                        )
                    })?;

                    if got_tokens == expected_tokens {
                        Ok(())
                    } else {
                        Err(anyhow!(
                            "Decoded tokens are not equal with that other relay sent"
                        ))
                    }
                }
            }
        }

        tokio::spawn(async {
            let configs = self.ton_listener.get_state().await.clone();
            let eth_listener = self.eth_listener.clone();
            async move {
                let check_result = eth_listener
                    .check_transaction(event.event_transaction)
                    .await;
                if let Err(e) = match check_event(&configs, check_result, &event).await {
                    Ok(_) => {
                        log::info!("Confirming transaction. Hash: {}", event.event_transaction);
                        self.ton_listener
                            .enqueue_vote(EventTransaction::Confirm(event))
                            .await
                    }
                    Err(e) => {
                        log::warn!("Rejection: {:?}", e);
                        self.ton_listener
                            .enqueue_vote(EventTransaction::Reject(event))
                            .await
                    }
                } {
                    log::error!("Critical error while spawning vote: {:?}", e)
                }
            }
            .await
        });
    }

    async fn watch_pending_confirmations(self: Arc<Self>) {
        log::debug!("Started watch_unsent_eth_ton_transactions");
        loop {
            let synced_block = match self.eth_listener.get_synced_height().await {
                Ok(a) => a,
                Err(e) => {
                    log::error!("CRITICAL error: {}", e);
                    continue;
                }
            };
            log::debug!("New block: {:?}", synced_block);

            let prepared_blocks = self
                .eth_queue
                .get_prepared_blocks(synced_block.as_u64())
                .await;
            for (entry, event) in prepared_blocks {
                let block_number = event.event_block_number;
                log::debug!(
                    "Found unconfirmed data in block {}: {}",
                    block_number,
                    hex::encode(&event.event_transaction)
                );
                self.clone().check_suspicious_event(event);
                entry.remove().expect("Fatal db error");
            }
            if let SyncedHeight::Synced(a) = synced_block {
                let bad_blocks = self.eth_queue.get_bad_blocks(a).await;
                for (entry, event) in bad_blocks {
                    let block_number = event.event_block_number;
                    log::debug!(
                        "Found suspicious data in block {}: {}",
                        block_number,
                        hex::encode(&event.event_transaction)
                    );
                    self.clone().check_suspicious_event(event);
                    entry.remove().expect("Fatal db error");
                }
            }
            tokio::time::delay_for(Duration::from_secs(10)).await;
        }
    }

    async fn process_eth_event(self: Arc<Self>, event: relay_eth::ws::Event) {
        log::info!(
            "Received event from address: {}. Tx hash: {}.",
            &event.address,
            &event.tx_hash
        );

        // Extend event info
        let (configuration_id, ethereum_event_blocks_to_confirm, topic_tokens) = {
            let state = self.ton_listener.get_state().await;

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
                    .event_blocks_to_confirm
                    .to_u64()
                    .unwrap_or_else(u64::max_value),
                topic_tokens,
            )
        };

        // Prepare confirmation

        let ton_data: Vec<_> = topic_tokens
            .into_iter()
            .map(utils::map_eth_to_ton)
            .collect();
        let event_data = match utils::pack_token_values(ton_data) {
            Ok(a) => a,
            Err(e) => {
                log::error!("Failed mapping ton_data to cell: {:?}", e);
                return;
            }
        };

        let prepared_data = EthEventVotingData {
            event_transaction: event.tx_hash,
            event_index: event.event_index,
            event_data,
            event_block_number: event.block_number,
            event_block: event.block_hash,
            configuration_id,
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

    pub fn ton_relay_address(&self) -> MsgAddrStd {
        self.relay_contract.address().clone()
    }

    pub fn eth_pubkey(&self) -> PublicKey {
        self.eth_signer.pubkey()
    }
}
