pub mod errors;
pub mod graphql_transport;

use self::errors::*;
use crate::models::*;
use crate::prelude::*;

#[async_trait]
pub trait Transport: Send + Sync + 'static {
    async fn get_account_state(
        &self,
        addr: &MsgAddressInt,
    ) -> TransportResult<Option<AccountState>>;

    async fn send_message(
        &self,
        abi: &AbiContract,
        message: ExternalMessage,
    ) -> TransportResult<ContractOutput>;
}

#[async_trait]
pub trait Sendable {
    async fn send(
        self,
        abi: &AbiContract,
        transport: &dyn Transport,
    ) -> TransportResult<ContractOutput>;
}

#[async_trait]
impl Sendable for ExternalMessage {
    async fn send(
        self,
        abi: &AbiContract,
        transport: &dyn Transport,
    ) -> TransportResult<ContractOutput> {
        transport.send_message(abi, self).await
    }
}
