pub use self::abi_mapping::*;
pub use self::db_pool::*;
pub use self::existing_contract::*;
pub use self::pending_messages_cache::*;
pub use self::retry::*;
pub use self::shard_utils::*;
pub use self::topic_hash::*;
pub use self::tx_context::*;

mod abi_mapping;
mod db_pool;
mod existing_contract;
mod pending_messages_cache;
mod retry;
mod shard_utils;
mod topic_hash;
mod tx_context;

#[macro_export]
/// Maps `Result<T,E>` to `Option<T>` logging errors.
macro_rules! filter_log {
    ($val:expr, $log_msg:literal) => {{
        match $val {
            Ok(a) => Some(a),
            Err(e) => {
                ::log::error!("{}:{}", $log_msg, e);
                None
            }
        }
    }};
}
