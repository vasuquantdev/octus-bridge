use super::Transport;
use super::errors::*;
use crate::prelude::*;
use crate::models::*;

pub struct GraphQlTransport {}

#[async_trait]
impl Transport for GraphQlTransport {
    async fn get_account_state(addr: &MsgAddrStd) -> TransportResult<Option<AccountState>> {
        todo!()
    }

    async fn send_message(message: ExternalMessage) -> TransportResult<TransactionId> {
        todo!()
    }
}
