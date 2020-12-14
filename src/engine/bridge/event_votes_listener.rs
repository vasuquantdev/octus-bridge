use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{anyhow, Error};
use ethereum_types::H256;
use futures::{future, StreamExt};
use sled::Db;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::{oneshot, Mutex};

use relay_ton::contracts::{BridgeContract, ContractResult};
use relay_ton::prelude::UInt256;

use crate::db_managment::models::EthTonTransaction;
use crate::db_managment::stats::StatsProvider;
use crate::db_managment::ton_db::TonTree;
use crate::db_managment::tx_monitor::TxTable;
use crate::engine::bridge::models::{EventVote, ExtendedEventInfo};

pub struct EventVotesListener {
    bridge: Arc<BridgeContract>,
    db: TonTree,
    tx_table: TxTable,
    stats: StatsProvider,
    relay_key: UInt256,
    confirmations: Mutex<HashMap<Vec<u8>, oneshot::Sender<ExtendedEventInfo>>>,
    rejections: Mutex<HashMap<Vec<u8>, oneshot::Sender<ExtendedEventInfo>>>,
}

impl EventVotesListener {
    pub fn new(db: &Db, bridge: Arc<BridgeContract>) -> Result<Arc<Self>, Error> {
        let relay_key = bridge.pubkey();

        Ok(Arc::new(Self {
            bridge,
            db: TonTree::new(&db)?,
            tx_table: TxTable::new(db)?,
            stats: StatsProvider::new(&db)?,
            relay_key,
            confirmations: Default::default(),
            rejections: Default::default(),
        }))
    }

    pub async fn watch(
        self: Arc<Self>,
        mut events_rx: UnboundedReceiver<(EventVote, ExtendedEventInfo)>,
    ) {
        log::info!("Started watching other relay events");
        while let Some((vote, event)) = events_rx.next().await {
            self.stats
                .update_relay_stats(
                    &event.relay_key,
                    H256::from_slice(&event.data.ethereum_event_transaction),
                )
                .unwrap();

            log::info!("Received event {:?}", vote);

            if event.relay_key == self.relay_key {
                log::info!(
                    "Received event for our relay. Eth tx: {}",
                    hex::encode(&event.data.ethereum_event_transaction)
                );

                self.notify(vote, event).await;
            } else {
                log::info!(
                    "Received event for other relay. Eth tx: {}, relay key: {}",
                    hex::encode(&event.data.ethereum_event_transaction),
                    hex::encode(&event.relay_key)
                );

                // TODO: check how to handle events from multiple relays
                self.db
                    .insert(
                        H256::from_slice(&event.data.ethereum_event_transaction),
                        &event,
                    )
                    .unwrap();
            }
        }
    }

    pub fn remove_event_by_hash(&self, hash: &H256) -> Result<(), Error> {
        self.db.remove_event_by_hash(hash)?;
        Ok(())
    }

    pub fn get_event_by_hash(&self, hash: &H256) -> Result<Option<ExtendedEventInfo>, Error> {
        Ok(self.db.get_event_by_hash(hash)?)
    }

    pub async fn vote(self: Arc<Self>, data: EthTonTransaction) -> Result<(), Error> {
        let hash = data.get_event_transaction();
        self.tx_table.insert(&hash, &data).unwrap();

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
                if retries_count <= 0 {
                    break Err(e.into());
                }

                delay.await;
            } else if let Some(rx_fut) = rx.take() {
                match future::select(rx_fut, delay).await {
                    future::Either::Left((Ok(event), _)) => {
                        log::info!(
                            "Got response for voting for {}. Event: {:?}",
                            data.get_event_transaction(),
                            event
                        );
                        self.remove_event_by_hash(&hash).unwrap();
                        self.tx_table.remove(&hash).unwrap();
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
                        if retries_count <= 0 {
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
        result
    }

    async fn cancel(&self, hash: &[u8], vote: EventVote) {
        match vote {
            EventVote::Confirm => self.confirmations.lock().await.remove(hash),
            EventVote::Reject => self.rejections.lock().await.remove(hash),
        };
    }

    async fn notify(&self, vote: EventVote, event: ExtendedEventInfo) {
        let mut table = match vote {
            EventVote::Confirm => self.confirmations.lock().await,
            EventVote::Reject => self.rejections.lock().await,
        };

        if let Some(tx) = table.remove(&event.data.ethereum_event_transaction) {
            if tx.send(event).is_err() {
                log::error!("Failed sending event notification");
            }
        }
    }
}

impl EthTonTransaction {
    async fn send(&self, bridge: &BridgeContract) -> ContractResult<()> {
        match self.clone() {
            Self::Confirm(a) => {
                bridge
                    .confirm_ethereum_event(
                        a.event_transaction,
                        a.event_index,
                        a.event_data,
                        a.event_block_number,
                        a.event_block,
                        a.ethereum_event_configuration_address,
                    )
                    .await
            }
            Self::Reject(a) => {
                bridge
                    .reject_ethereum_event(
                        a.event_transaction,
                        a.event_index,
                        a.event_data,
                        a.event_block_number,
                        a.event_block,
                        a.ethereum_event_configuration_address,
                    )
                    .await
            }
        }
    }
}
