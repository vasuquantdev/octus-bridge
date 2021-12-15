pub use self::eth_address::*;
pub use self::existing_contract::*;
pub use self::pending_messages_queue::*;
pub use self::protected_region::*;
pub use self::retry::*;
pub use self::serde_helpers::*;
pub use self::shard_utils::*;
pub use self::tx_context::*;

mod eth_address;
mod existing_contract;
mod pending_messages_queue;
mod protected_region;
mod retry;
mod serde_helpers;
mod shard_utils;
mod tx_context;

#[macro_export]
macro_rules! once {
    ($ty:path, || $expr:expr) => {{
        static ONCE: once_cell::race::OnceBox<$ty> = once_cell::race::OnceBox::new();
        ONCE.get_or_init(|| Box::new($expr))
    }};
}
