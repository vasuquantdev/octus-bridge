use std::borrow::Borrow;
use std::hash::BuildHasherDefault;

use dashmap::DashMap;
use rustc_hash::FxHasher;

pub use self::eth_address::*;
pub use self::existing_contract::*;
pub use self::pending_messages_queue::*;
pub use self::retry::*;
pub use self::serde_helpers::*;
pub use self::shard_utils::*;
pub use self::tristate::*;
pub use self::tx_context::*;

mod eth_address;
mod existing_contract;
mod pending_messages_queue;
mod retry;
mod serde_helpers;
mod shard_utils;
mod tristate;
mod tx_context;

#[macro_export]
macro_rules! once {
    ($ty:path, || $expr:expr) => {{
        static ONCE: once_cell::race::OnceBox<$ty> = once_cell::race::OnceBox::new();
        ONCE.get_or_init(|| Box::new($expr))
    }};
}

pub type FxDashMap<K, V> = DashMap<K, V, BuildHasherDefault<FxHasher>>;

#[derive(Clone, Copy)]
pub struct DisplayAddr<T>(pub T);

impl<T> std::fmt::Display for DisplayAddr<T>
where
    T: Borrow<ton_types::UInt256>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("0:{:x}", self.0.borrow()))
    }
}
