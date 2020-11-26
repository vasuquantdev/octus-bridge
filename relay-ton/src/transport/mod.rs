pub mod errors;
mod tvm;
mod utils;

#[cfg(feature = "graphql_transport")]
pub mod graphql_transport;
#[cfg(feature = "tonlib_transport")]
pub mod tonlib_transport;

#[cfg(feature = "graphql_transport")]
pub use graphql_transport::GraphQLTransport;
#[cfg(feature = "tonlib")]
pub use tonlib_transport::TonlibTransport;

use self::errors::*;
use crate::models::*;
use crate::prelude::*;

#[async_trait]
pub trait RunLocal: Send + Sync + 'static {
    async fn run_local(
        &self,
        abi: &AbiFunction,
        message: ExternalMessage,
    ) -> TransportResult<ContractOutput>;
}

#[async_trait]
pub trait Transport: RunLocal {
    async fn subscribe(&self, addr: &str) -> TransportResult<Arc<dyn AccountSubscription>>;
}

#[async_trait]
pub trait AccountSubscription: RunLocal {
    fn events(&self) -> watch::Receiver<AccountEvent>;

    async fn send_message(
        &self,
        abi: Arc<AbiFunction>,
        message: ExternalMessage,
    ) -> TransportResult<ContractOutput>;
}

#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq)]
pub enum AccountEvent {
    StateChanged,
    OutboundEvent(Arc<SliceData>),
}
