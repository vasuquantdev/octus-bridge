pub mod errors;
pub mod graphql_transport;

use self::errors::*;
use crate::models::*;
use crate::prelude::*;

#[async_trait]
pub trait Transport {
    async fn get_account_state(addr: &MsgAddrStd) -> TransportResult<Option<AccountState>>;
    async fn send_message(message: ExternalMessage) -> TransportResult<TransactionId>;
}
