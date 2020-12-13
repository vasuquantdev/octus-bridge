use std::time::Duration;

use anyhow::Error;
use ethereum_types::H256;
use futures::future::Either;
use futures::StreamExt;
use sled::Db;
use tokio::sync;

use relay_ton::contracts::errors::ContractError;
use relay_ton::contracts::BridgeContract;
use relay_ton::prelude::Future;

use crate::db_managment::eth_queue::EthQueue;
use crate::db_managment::models::EthTonTransaction;
use crate::db_managment::tx_monitor::TxTable;
use crate::engine::bridge::models::{ExtendedEventInfo, Status};
use crate::engine::bridge::prelude::Arc;

struct TxMonitor {
    tx_table: TxTable,
    notifier_tx: sync::mpsc::Sender<Status>,
    notifier_rx: sync::mpsc::Receiver<Status>,
    confirmed_events_rx: Arc<sync::broadcast::Receiver<ExtendedEventInfo>>,
}

impl TxMonitor {
    pub fn new(
        db: &Db,
        confirmed_events_rx: sync::broadcast::Receiver<ExtendedEventInfo>,
    ) -> Result<Self, Error> {
        let (notifier_tx, notifier_rx) = sync::mpsc::channel(255);
        Ok(Self {
            tx_table: TxTable::new(db)?,
            notifier_rx,
            notifier_tx,
            confirmed_events_rx: Arc::new(confirmed_events_rx),
        })
    }

    fn send_ton_tx(
        bridge: Arc<BridgeContract>,
        hash: H256,
        data: EthTonTransaction,
        mut notify: sync::mpsc::Sender<Status>,
        retries_number: usize,
        retry_sleep_time: Duration,
    ) {
        tokio::spawn(async move {
            let bridge_future = move || match data {
                EthTonTransaction::Confirm(a) => bridge.confirm_ethereum_event(
                    a.event_transaction,
                    a.event_index,
                    a.event_data,
                    a.event_block_number,
                    a.event_block,
                    a.ethereum_event_configuration_address,
                ),
                EthTonTransaction::Reject(a) => bridge.reject_ethereum_event(
                    a.event_transaction,
                    a.event_index,
                    a.event_data,
                    a.event_block_number,
                    a.event_block,
                    a.ethereum_event_configuration_address,
                ),
            };

            for _ in 0..retries_number {
                match bridge_future().await {
                    Err(e) => log::error!("Failed sending tx to ton: {}. Retrying", e),
                    Ok(_) => {
                        while let Some(event) = tx.subscribe().next().await {
                            let event = match event {
                                Ok(a) => a,
                                Err(e) => {
                                    log::error!("Failed receiving via channel: {:?}", e);
                                    return;
                                }
                            };
                            if &H256::from_slice(&*event.data.ethereum_event_transaction) == &hash {
                                log::info!("Met confirmation of our transaction. Hash: {}", hash);
                                if let Err(e) = notify.send(Status { hash, sucess: true }).await {
                                    log::error!("Failed sending confirmation: {}", e)
                                }
                            }
                        }
                    }
                }
                tokio::time::delay_for(retry_sleep_time).await;
            }

            log::error!("Retries are exhausted. Confirming fail. Tx: {}", hash);
            if let Err(e) = notify
                .send(Status {
                    hash,
                    sucess: false,
                })
                .await
            {
                log::error!("Failed sending confirmation: {}", e)
            };
        });
    }

    pub fn enqueue(&self, data: EthTonTransaction, bridge: Arc<BridgeContract>) {
        let hash = data.get_hash();
        self.tx_table.insert(&hash, &data).unwrap();

        Self::send_ton_tx(
            bridge,
            hash,
            data,
            self.notifier_tx.clone(),
            5,
            Duration::from_secs(10), //fixme
        );
    }

    async fn monitor_transactions_status(self) {
        let mut confirmed_events_rx = self.notifier_rx;
        while let Some(status) = confirmed_events_rx.next().await {
            match status.sucess {
                true => {
                    log::info!(
                        "Received successfully confirmation. Removing from queue. Tx hash: {}",
                        status.hash
                    );
                    self.tx_table.remove(&status.hash).unwrap();
                }
                false => {
                    log::error!("Received failed confirmation. Hash: {}", status.hash)
                }
            }
        }
    }
}
