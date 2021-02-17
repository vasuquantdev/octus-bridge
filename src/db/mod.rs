mod stats_db;
mod verification_queue;
mod votes_queues;

mod constants;
pub mod migrate;

use std::collections::HashMap;

pub use self::migrate::*;
pub use self::stats_db::*;
pub use self::verification_queue::*;
pub use self::votes_queues::*;

/// This module contains all db related operations.
/// We are using `sled` as kv storage and `borsh` for encoding in the binary format.
pub trait Table {
    type Key;
    type Value;
    fn dump_elements(&self) -> HashMap<Self::Key, Self::Value>;
}
