use chrono::{DateTime, Utc};

use relay_models::models::{EventVote, TxStatView};
use relay_ton::prelude::UInt256;

use super::prelude::{Error, Tree};
use crate::db_management::{constants::*, Table};
use crate::engine::bridge::models::{EthEventReceivedVote, TonEventReceivedVote};
use crate::prelude::*;

#[derive(Clone)]
pub struct StatsDb {
    eth_event_votes: Stats,
    ton_event_votes: Stats,
    latest_scanned_lt: Tree,
}

impl StatsDb {
    pub fn new(db: &Db) -> Result<Self, Error> {
        Ok(Self {
            eth_event_votes: Stats::new(db, ETH_EVENT_VOTES)?,
            ton_event_votes: Stats::new(db, TON_EVENT_VOTES)?,
            latest_scanned_lt: db.open_tree(TON_LATEST_SCANNED_LT)?,
        })
    }

    #[inline]
    pub fn eth_event_votes(&self) -> &Stats {
        &self.eth_event_votes
    }

    #[inline]
    pub fn ton_event_votes(&self) -> &Stats {
        &self.ton_event_votes
    }

    pub fn update_latest_scanned_lt(&self, addr: &MsgAddressInt, lt: u64) -> Result<(), Error> {
        self.latest_scanned_lt
            .insert(&addr.address().get_bytestring(0), &lt.to_be_bytes())?;

        #[cfg(feature = "paranoid")]
        self.eth_event_votes.flush()?;
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
        self.eth_event_votes
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

#[derive(Clone)]
struct Stats {
    tree: Tree,
}

impl Stats {
    fn new(db: &Db, name: &str) -> Result<Self, Error> {
        Ok(Self {
            tree: db.open_tree(name)?,
        })
    }

    pub async fn insert_vote<T>(&self, event: T) -> Result<(), Error>
    where
        T: ReceivedVote,
    {
        log::debug!("Inserting stats");

        let event_addr = event.event_addr().address.get_bytestring(0);

        let mut key = [0; 65];
        key[0..32].copy_from_slice(&event_addr);
        key[32..64].copy_from_slice(event.relay_key());
        key[64] = (event.vote() == EventVote::Confirm) as u8;

        let stored = event.into_stored();
        self.eth_event_votes
            .insert(key, bincode::serialize(&stored).expect("Fatal db error"))?;

        #[cfg(feature = "paranoid")]
        self.eth_event_votes.flush()?;
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
        Ok(self
            .eth_event_votes
            .scan_prefix(&key)
            .keys()
            .next()
            .is_some())
    }
}

pub trait ReceivedVote {
    type Stored;

    fn event_addr(&self) -> &MsgAddrStd;
    fn relay_key(&self) -> &[u8; 32];
    fn vote(&self) -> EventVote;
    fn into_stored(self) -> Self::Stored;
}

impl ReceivedVote for EthEventReceivedVote<'_> {
    type Stored = StoredTxStat;

    #[inline]
    fn event_addr(&self) -> &MsgAddrStd {
        self.event_addr
    }

    #[inline]
    fn relay_key(&self) -> &[u8; 32] {
        self.relay_key.as_slice()
    }

    #[inline]
    fn vote(&self) -> EventVote {
        self.vote
    }

    #[inline]
    fn into_stored(self) -> Self::Stored {
        StoredTxStat {
            tx_hash: self.data.init_data.event_transaction,
            met: chrono::Utc::now(),
        }
    }
}

impl ReceivedVote for TonEventReceivedVote<'_> {
    type Stored = StoredTxStat;

    #[inline]
    fn event_addr(&self) -> &MsgAddrStd {
        self.event_addr
    }

    #[inline]
    fn relay_key(&self) -> &[u8; 32] {
        self.relay_key.as_slice()
    }

    #[inline]
    fn vote(&self) -> EventVote {
        self.vote
    }

    #[inline]
    fn into_stored(self) -> Self::Stored {
        StoredTxStat {
            tx_hash: self.data.init_data.event_transaction,
            met: chrono::Utc::now(),
        }
    }
}

#[derive(Deserialize, Serialize)]
pub struct StoredTxStat {
    pub tx_hash: H256,
    pub met: DateTime<Utc>,
}
