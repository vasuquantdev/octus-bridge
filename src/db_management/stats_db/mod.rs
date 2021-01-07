use chrono::{DateTime, Utc};

use relay_models::models::{EventVote, TxStatView};
use relay_ton::prelude::UInt256;

use super::prelude::{Error, Tree};
use crate::db_management::{constants::STATS_TREE_NAME, constants::TON_LATEST_SCANNED_LT, Table};
use crate::engine::bridge::models::ExtendedEventInfo;
use crate::prelude::*;

#[derive(Clone)]
pub struct StatsDb {
    stats: Tree,
    latest_scanned_lt: Tree,
}

impl StatsDb {
    pub fn new(db: &Db) -> Result<Self, Error> {
        Ok(Self {
            stats: db.open_tree(STATS_TREE_NAME)?,
            latest_scanned_lt: db.open_tree(TON_LATEST_SCANNED_LT)?,
        })
    }

    pub async fn update_relay_stats(&self, event: &ExtendedEventInfo) -> Result<(), Error> {
        log::debug!("Inserting stats");

        let event_addr = event.event_addr.address.get_bytestring(0);

        let mut key = [0; 65];
        key[0..32].copy_from_slice(&event_addr);
        key[32..64].copy_from_slice(event.relay_key.as_slice());
        key[64] = (event.vote == EventVote::Confirm) as u8;

        self.stats.insert(
            key,
            bincode::serialize(&StoredTxStat {
                tx_hash: event.data.ethereum_event_transaction,
                met: chrono::Utc::now(),
            })
            .expect("Db error fail"),
        )?;
        #[cfg(feature = "paranoid")]
        self.stats.flush()?;
        Ok(())
    }

    pub fn has_already_voted(
        &self,
        event_addr: &MsgAddrStd,
        relay_key: &UInt256,
    ) -> Result<bool, Error> {
        let mut key = Vec::with_capacity(64);
        key.extend_from_slice(&event_addr.address.get_bytestring(0));
        key.extend_from_slice(relay_key.as_slice());
        Ok(self.stats.scan_prefix(&key).keys().next().is_some())
    }

    pub fn update_latest_scanned_lt(&self, addr: &MsgAddressInt, lt: u64) -> Result<(), Error> {
        self.latest_scanned_lt
            .insert(&addr.address().get_bytestring(0), &lt.to_be_bytes())?;

        #[cfg(feature = "paranoid")]
        self.tx_stats.flush()?;
        Ok(())
    }

    pub fn get_latest_scanned_lt(&self, addr: &MsgAddressInt) -> Result<Option<u64>, Error> {
        Ok(self
            .latest_scanned_lt
            .get(&addr.address().get_bytestring(0))?
            .map(|value| {
                let mut buf = [0u8; 8];
                buf.copy_from_slice(&value[0..8]);
                u64::from_be_bytes(buf)
            }))
    }
}

impl Table for StatsDb {
    type Key = String;
    type Value = Vec<TxStatView>;

    fn dump_elements(&self) -> HashMap<Self::Key, Self::Value> {
        self.stats
            .iter()
            .filter_map(|x| match x {
                Ok(a) => Some(a),
                Err(e) => {
                    log::error!("Failed getting stats from db. Db corruption?: {}", e);
                    None
                }
            })
            .fold(HashMap::new(), |mut result, (key, value)| {
                let event_addr = MsgAddrStd {
                    anycast: None,
                    workchain_id: 0,
                    address: UInt256::from(&key[0..32]).into(),
                };
                let relay_key = H256::from_slice(&key[32..64]);
                let vote = if key[64] == 0 {
                    EventVote::Reject
                } else {
                    EventVote::Confirm
                };

                let stats: StoredTxStat = bincode::deserialize(&value).expect("Shouldn't fail");

                result
                    .entry(hex::encode(&relay_key))
                    .or_insert_with(Vec::new)
                    .push(TxStatView {
                        tx_hash: hex::encode(stats.tx_hash),
                        met: stats.met.timestamp(),
                        event_addr: event_addr.to_string(),
                        vote,
                    });

                result
            })
    }
}

#[derive(Deserialize, Serialize)]
pub struct StoredTxStat {
    pub tx_hash: H256,
    pub met: DateTime<Utc>,
}
