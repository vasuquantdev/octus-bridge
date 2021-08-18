pub use self::db_pool::*;
pub use self::existing_contract::*;
pub use self::retry::*;
pub use self::shard_utils::*;

mod db_pool;
mod existing_contract;
mod retry;
mod shard_utils;

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
