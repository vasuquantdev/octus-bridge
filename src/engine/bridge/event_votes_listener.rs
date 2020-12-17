use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{anyhow, Error};
use ethereum_types::H256;
use futures::{future, Stream, StreamExt};

use tokio::sync::{oneshot, Mutex};

use relay_ton::contracts::{BridgeContract, ContractResult};
use relay_ton::prelude::UInt256;

use crate::db_managment::{EthQueue, EthTonTransaction, StatsDb, TonQueue};
use crate::engine::bridge::models::{EventVote, ExtendedEventInfo, ValidatedEventStructure};

pub struct EventVotesListener {
    bridge: Arc<BridgeContract>,
    eth_queue: EthQueue,
    ton_queue: TonQueue,
    stats_db: StatsDb,

    relay_key: UInt256,
    confirmations: Mutex<HashMap<Vec<u8>, oneshot::Sender<()>>>,
    rejections: Mutex<HashMap<Vec<u8>, oneshot::Sender<()>>>,
}

impl EventVotesListener {
    pub fn new(
        bridge: Arc<BridgeContract>,
        eth_queue: EthQueue,
        ton_queue: TonQueue,
        stats_db: StatsDb,
    ) -> Arc<Self> {
        let relay_key = bridge.pubkey();

        Arc::new(Self {
            bridge,
            eth_queue,
            ton_queue,
            stats_db,

            relay_key,
            confirmations: Default::default(),
            rejections: Default::default(),
        })
    }

    pub async fn watch<S>(self: Arc<Self>, mut events_rx: S)
    where
        S: Stream<Item = ExtendedEventInfo> + Unpin,
    {
        log::info!("Started watching relay events");

        while let Some(event) = events_rx.next().await {
            let new_event = !self.stats_db.has_event(&event.event_addr).expect("Fatal db error");
            let should_check = event.vote == EventVote::Confirm
                && new_event
                && !event.data.proxy_callback_executed
                && !event.data.event_rejected;

            let validated_structure = event.validate_structure();
            log::info!("Received {}", validated_structure);

            self.stats_db
                .update_relay_stats(&validated_structure)
                .expect("Fatal db error");

            match validated_structure {
                ValidatedEventStructure::Valid(event) => {
                    if event.relay_key == self.relay_key {
                        // Stop retrying after our event response was found
                        let hash = H256::from_slice(&event.data.ethereum_event_transaction);
                        if let Err(e) = self.ton_queue.mark_complete(&hash) {
                            log::error!("Failed to mark transaction completed. {:?}", e);
                        }

                        self.notify_found(&event).await;
                    } else if should_check {
                        let target_block_number = event.target_block_number();

                        if let Err(e) = self
                            .eth_queue
                            .insert(target_block_number, &event.into())
                            .await
                        {
                            log::error!("Failed to insert event confirmation. {:?}", e);
                        }
                    }
                }
                ValidatedEventStructure::Invalid(event, _) => {
                    if event.relay_key == self.relay_key {
                        log::error!("Found invalid data for our event");

                        // TODO: mark complete in ton_queue?
                        self.notify_found(&event).await;
                    } else if should_check {
                        // If found event with invalid structure, that we should check - reject it immediately
                        let data = EthTonTransaction::Reject(event.into());

                        let bridge = self.bridge.clone();
                        tokio::spawn(async move {
                            if let Err(e) = data.send(bridge.as_ref()).await {
                                log::error!("Failed to send rejection for invalid event. {:?}", e);
                            }
                        });
                    }
                }
            }
        }
    }

    pub fn spawn_vote(self: &Arc<Self>, data: EthTonTransaction) -> Result<(), Error> {
        let hash = data.get_event_transaction();
        self.ton_queue.insert_pending(&hash, &data)?;

        tokio::spawn(self.clone().ensure_sent(hash, data));

        Ok(())
    }

    async fn ensure_sent(self: Arc<Self>, hash: H256, data: EthTonTransaction) {
        let (tx, rx) = oneshot::channel();

        let vote = match &data {
            EthTonTransaction::Confirm(_) => {
                self.confirmations.lock().await.insert(hash.0.to_vec(), tx);
                EventVote::Confirm
            }
            EthTonTransaction::Reject(_) => {
                self.rejections.lock().await.insert(hash.0.to_vec(), tx);
                EventVote::Reject
            }
        };

        let mut rx = Some(rx);
        let mut retries_count = 3;
        let retries_interval = tokio::time::Duration::from_secs(60);

        let result = loop {
            let delay = tokio::time::delay_for(retries_interval);

            if let Err(e) = data.send(&self.bridge).await {
                log::error!(
                    "Failed to vote for event: {:?}. Retrying ({} left)",
                    e,
                    retries_count
                );

                retries_count -= 1;
                if retries_count < 0 {
                    break Err(e.into());
                }

                delay.await;
            } else if let Some(rx_fut) = rx.take() {
                match future::select(rx_fut, delay).await {
                    future::Either::Left((Ok(()), _)) => {
                        log::info!("Got response for voting for {:?} {}", vote, hash);
                        break Ok(());
                    }
                    future::Either::Left((Err(e), _)) => {
                        break Err(e.into());
                    }
                    future::Either::Right((_, new_rx)) => {
                        log::error!(
                            "Failed to get voting event response: timeout reached. Retrying ({} left)",
                            retries_count
                        );

                        retries_count -= 1;
                        if retries_count < 0 {
                            break Err(anyhow!("Failed to vote for event, no retries left"));
                        }

                        rx = Some(new_rx);
                    }
                }
            } else {
                unreachable!()
            }
        };

        self.cancel(&hash.0, vote).await;

        match result {
            Ok(_) => {
                log::info!(
                    "Stopped waiting for transaction: {}",
                    hex::encode(hash.as_bytes())
                );
            }
            Err(e) => {
                log::error!(
                    "Stopped waiting for transaction: {}. Reason: {:?}",
                    hex::encode(hash.as_bytes()),
                    e
                );
                if let Err(e) = self.ton_queue.mark_failed(&hash) {
                    log::error!("failed to mark transaction: {:?}", e);
                }
            }
        }
    }

    async fn notify_found(&self, event: &ExtendedEventInfo) {
        let mut table = match event.vote {
            EventVote::Confirm => self.confirmations.lock().await,
            EventVote::Reject => self.rejections.lock().await,
        };

        if let Some(tx) = table.remove(&event.data.ethereum_event_transaction) {
            if tx.send(()).is_err() {
                log::error!("Failed sending event notification");
            }
        }
    }

    async fn cancel(&self, hash: &[u8], vote: EventVote) {
        match vote {
            EventVote::Confirm => self.confirmations.lock().await.remove(hash),
            EventVote::Reject => self.rejections.lock().await.remove(hash),
        };
    }
}

impl EthTonTransaction {
    async fn send(&self, bridge: &BridgeContract) -> ContractResult<()> {
        match self.clone() {
            Self::Confirm(a) => {
                bridge
                    .confirm_ethereum_event(
                        a.event_transaction,
                        a.event_index.into(),
                        a.event_data,
                        a.event_block_number.into(),
                        a.event_block,
                        a.ethereum_event_configuration_address,
                    )
                    .await
            }
            Self::Reject(a) => {
                bridge
                    .reject_ethereum_event(
                        a.event_transaction,
                        a.event_index.into(),
                        a.event_data,
                        a.event_block_number.into(),
                        a.event_block,
                        a.ethereum_event_configuration_address,
                    )
                    .await
            }
        }
    }
}
