use chrono::{DateTime, Utc};
use ethereum_types::H256;
use serde::de::DeserializeOwned;

use relay_models::models::{EventVote, TxStatView};
use relay_ton::contracts::Voting;
use relay_ton::prelude::UInt256;

use super::prelude::{Error, Tree};
use crate::db_management::{constants::*, prelude::Table};
use crate::models::*;
use crate::prelude::*;

#[derive(Clone)]
pub struct ScanningState {
    latest_scanned_lt: Tree,
}

impl ScanningState {
    pub fn new(db: &Db) -> Result<Self, Error> {
        Ok(Self {
            latest_scanned_lt: db.open_tree(TON_LATEST_SCANNED_LT)?,
        })
    }

    pub fn update_latest_scanned_lt(&self, addr: &MsgAddressInt, lt: u64) -> Result<(), Error> {
        self.latest_scanned_lt
            .insert(&addr.address().get_bytestring(0), &lt.to_be_bytes())?;

        #[cfg(feature = "paranoid")]
        self.latest_scanned_lt.flush()?;
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

pub type TonVotingStats = VotingStats<TonEventReceivedVoteWithData>;

impl TonVotingStats {
    pub fn new_ton_voting_stats(db: &Db) -> Result<Self, Error> {
        Ok(Self {
            tree: db.open_tree(TON_EVENT_VOTES)?,
            _marker: Default::default(),
        })
    }
}

pub type EthVotingStats = VotingStats<EthEventReceivedVoteWithData>;

impl EthVotingStats {
    pub fn new_eth_voting_stats(db: &Db) -> Result<Self, Error> {
        Ok(Self {
            tree: db.open_tree(ETH_EVENT_VOTES)?,
            _marker: Default::default(),
        })
    }
}

#[derive(Clone)]
pub struct VotingStats<T> {
    tree: Tree,
    _marker: std::marker::PhantomData<T>,
}

impl<T> VotingStats<T>
where
    T: ReceivedVoteWithData + GetStoredData,
{
    pub async fn insert_vote(&self, event: &T) -> Result<(), Error> {
        log::debug!("Inserting stats");

        let info = event.info();

        let event_addr = info.event_address().address.get_bytestring(0);

        let mut key = [0; 65];
        key[0..32].copy_from_slice(&event_addr);
        key[32..64].copy_from_slice(info.relay_key().as_slice());
        key[64] = (info.kind() == Voting::Confirm) as u8;

        let stored = event.get_stored_data();
        self.tree
            .insert(key, bincode::serialize(&stored).expect("Fatal db error"))?;

        #[cfg(feature = "paranoid")]
        self.tree.flush()?;
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
        Ok(self.tree.scan_prefix(&key).keys().next().is_some())
    }
}

impl<T> Table for VotingStats<T>
where
    T: GetStoredData,
{
    type Key = String;
    type Value = Vec<<T as GetStoredData>::View>;

    fn dump_elements(&self) -> HashMap<Self::Key, Self::Value> {
        self.tree
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
                    Voting::Reject
                } else {
                    Voting::Confirm
                };

                let stored = bincode::deserialize::<<T as GetStoredData>::Stored>(&value)
                    .expect("Shouldn't fail");

                result
                    .entry(hex::encode(&relay_key))
                    .or_insert_with(Vec::new)
                    .push(<T as GetStoredData>::create_view(&event_addr, vote, stored));

                result
            })
    }
}

pub trait GetStoredData {
    type Stored: Serialize + DeserializeOwned;
    type View: Serialize;

    fn get_stored_data(&self) -> Self::Stored;

    fn create_view(event_addr: &MsgAddrStd, vote: Voting, stored: Self::Stored) -> Self::View;
}

impl GetStoredData for EthEventReceivedVoteWithData {
    type Stored = StoredTxStat;
    type View = TxStatView;

    #[inline]
    fn get_stored_data(&self) -> Self::Stored {
        StoredTxStat {
            tx_hash: self.data().init_data.event_transaction.clone(),
            met: chrono::Utc::now(),
        }
    }

    #[inline]
    fn create_view(event_addr: &MsgAddrStd, vote: Voting, stored: Self::Stored) -> Self::View {
        TxStatView {
            tx_hash: hex::encode(stored.tx_hash),
            met: stored.met.timestamp(),
            event_addr: event_addr.to_string(),
            vote: into_view(vote),
        }
    }
}

impl GetStoredData for TonEventReceivedVoteWithData {
    type Stored = StoredTxStat;
    type View = TxStatView;

    #[inline]
    fn get_stored_data(&self) -> Self::Stored {
        StoredTxStat {
            tx_hash: self.data().init_data.event_transaction.clone(),
            met: chrono::Utc::now(),
        }
    }

    #[inline]
    fn create_view(event_addr: &MsgAddrStd, vote: Voting, stored: Self::Stored) -> Self::View {
        TxStatView {
            tx_hash: hex::encode(stored.tx_hash),
            met: stored.met.timestamp(),
            event_addr: event_addr.to_string(),
            vote: into_view(vote),
        }
    }
}

#[derive(Deserialize, Serialize)]
pub struct StoredTxStat {
    pub tx_hash: H256,
    pub met: DateTime<Utc>,
}

fn into_view(vote: Voting) -> EventVote {
    match vote {
        Voting::Confirm => EventVote::Confirm,
        Voting::Reject => EventVote::Reject,
    }
}
