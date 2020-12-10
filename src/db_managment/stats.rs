

use relay_ton::prelude::UInt256;

use crate::db_managment::constants::STATS_TREE_NAME;
use crate::db_managment::Table;

use super::models::Stats;
use super::prelude::{Deserialize, Error, Serialize, Tree};
use super::Db;
use relay_ton::prelude::serde_uint256;

struct StatsProvider {
    tree: Tree,
}

impl StatsProvider {
    pub fn new(db: Db) -> Result<Self, Error> {
        Ok(Self {
            tree: db.open_tree(STATS_TREE_NAME)?,
        })
    }

    pub fn update_relay_stats(&self, relay_key: &UInt256, stats: Stats) {
        self.tree
            .insert(
                &relay_key.as_slice(),
                bincode::serialize(&stats).expect("Shouldn't fail"),
            )
            .unwrap();
    }
}
#[derive(Serialize, Deserialize, Debug)]
struct TonPubKey{
    #[serde(with = "serde_uint256")]
    inner:UInt256
}

impl Table for StatsProvider {
    type Key = TonPubKey;
    type Value = Stats;

    fn dump_elements(&self) -> Vec<(Self::Key, Self::Value)> {
        self.tree
            .iter()
            .filter_map(|x| x.ok())
            .map(|x| {
                (
                    bincode::deserialize(&x.0).expect("Shouldn't fail"),
                    bincode::deserialize(&x.1).expect("Shouldn't fail"),
                )
            })
            .collect()
    }
}
