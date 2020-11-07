pub mod errors;
pub mod graphql_transport;

use self::errors::*;
use crate::models::*;
use crate::prelude::*;

#[async_trait]
pub trait Transport: Send + Sync + 'static {
    async fn get_account_state(&self, addr: &MsgAddrStd) -> TransportResult<Option<AccountState>>;
    async fn send_message(&self, message: ExternalMessage) -> TransportResult<ContractOutput>;
}

#[async_trait]
pub trait Sendable {
    async fn send(self, transport: &dyn Transport) -> TransportResult<ContractOutput>;
}

#[async_trait]
impl Sendable for ExternalMessage {
    async fn send(self, transport: &dyn Transport) -> TransportResult<ContractOutput> {
        transport.send_message(self).await
    }
}
