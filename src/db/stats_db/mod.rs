use relay_models::models::{EthTxStatView, EventVote, TonTxStatView};
use relay_ton::contracts::Voting;

use super::constants::*;
use super::Table;
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
    pub fn new(db: &Db) -> Result<Self, Error> {
        Ok(Self {
            tree: db.open_tree(TON_EVENT_VOTES)?,
            _marker: Default::default(),
        })
    }
}

pub type EthVotingStats = VotingStats<EthEventReceivedVoteWithData>;

impl EthVotingStats {
    pub fn new(db: &Db) -> Result<Self, Error> {
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
        let relay_addr = info.relay().address.get_bytestring(0);

        let mut key = [0; 65];
        key[0..32].copy_from_slice(&event_addr);
        key[32..64].copy_from_slice(&relay_addr);
        key[64] = (info.kind() == Voting::Confirm) as u8;

        let stored = event.get_stored_data();
        self.tree
            .insert(key, stored.try_to_vec().expect("Fatal db error"))?;

        #[cfg(feature = "paranoid")]
        self.tree.flush()?;
        Ok(())
    }

    pub fn has_already_voted(
        &self,
        event_addr: &MsgAddrStd,
        relay_key: &MsgAddrStd,
    ) -> Result<bool, Error> {
        let mut key = Vec::with_capacity(64);
        key.extend_from_slice(&event_addr.address.get_bytestring(0));
        key.extend_from_slice(&relay_key.address.get_bytestring(0));
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

                let stored =
                    <<T as GetStoredData>::Stored as BorshDeserialize>::try_from_slice(&value)
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
    type Stored: BorshSerialize + BorshDeserialize;
    type View: Serialize;

    fn get_stored_data(&self) -> Self::Stored;

    fn create_view(event_addr: &MsgAddrStd, vote: Voting, stored: Self::Stored) -> Self::View;
}

impl GetStoredData for EthEventReceivedVoteWithData {
    type Stored = EthStoredTxStat;
    type View = EthTxStatView;

    #[inline]
    fn get_stored_data(&self) -> Self::Stored {
        EthStoredTxStat {
            tx_hash: self.data().init_data.event_transaction.as_bytes().to_vec(),
            met: chrono::Utc::now().timestamp(),
        }
    }

    #[inline]
    fn create_view(event_addr: &MsgAddrStd, vote: Voting, stored: Self::Stored) -> Self::View {
        EthTxStatView {
            tx_hash: hex::encode(stored.tx_hash),
            met: stored.met.to_string(),
            event_addr: event_addr.to_string(),
            vote: vote.into_view(),
        }
    }
}

impl GetStoredData for TonEventReceivedVoteWithData {
    type Stored = TonStoredTxStat;
    type View = TonTxStatView;

    #[inline]
    fn get_stored_data(&self) -> Self::Stored {
        TonStoredTxStat {
            tx_hash: self.data().init_data.event_transaction.as_slice().to_vec(),
            tx_lt: self.data().init_data.event_transaction_lt,
            met: chrono::Utc::now().timestamp(),
        }
    }

    #[inline]
    fn create_view(event_addr: &MsgAddrStd, vote: Voting, stored: Self::Stored) -> Self::View {
        TonTxStatView {
            tx_hash: hex::encode(stored.tx_hash.as_slice()),
            tx_lt: stored.tx_lt.to_string(),
            met: stored.met.to_string(),
            event_addr: event_addr.to_string(),
            vote: vote.into_view(),
        }
    }
}

#[derive(BorshSerialize, BorshDeserialize)]
pub struct EthStoredTxStat {
    pub tx_hash: Vec<u8>,
    pub met: i64,
}

#[derive(BorshSerialize, BorshDeserialize)]
pub struct TonStoredTxStat {
    pub tx_hash: Vec<u8>,
    pub tx_lt: u64,
    pub met: i64,
}

impl IntoView for Voting {
    type View = EventVote;

    fn into_view(self) -> Self::View {
        match self {
            Voting::Confirm => EventVote::Confirm,
            Voting::Reject => EventVote::Reject,
        }
    }
}
