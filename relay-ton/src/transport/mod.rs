pub mod errors;
//pub mod graphql_transport;
pub mod tonlib_transport;

//pub use graphql_transport::GraphQlTransport;
pub use tonlib_transport::TonlibTransport;

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
        abi: &AbiFunction,
        message: ExternalMessage,
    ) -> TransportResult<ContractOutput>;
}
