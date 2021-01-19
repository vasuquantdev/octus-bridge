pub mod errors;
mod tvm;
mod utils;

#[cfg(feature = "graphql-transport")]
pub mod graphql_transport;
#[cfg(feature = "tonlib-transport")]
pub mod tonlib_transport;

#[cfg(feature = "graphql-transport")]
pub use graphql_transport::GraphQLTransport;
#[cfg(feature = "tonlib-transport")]
pub use tonlib_transport::TonlibTransport;

pub use self::errors::*;
pub use crate::models::*;
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
    async fn subscribe_without_events(
        &self,
        account: MsgAddressInt,
    ) -> TransportResult<Arc<dyn AccountSubscription>>;

    async fn subscribe(
        &self,
        account: MsgAddressInt,
    ) -> TransportResult<(Arc<dyn AccountSubscription>, RawEventsRx)>;

    async fn subscribe_full(
        &self,
        account: MsgAddressInt,
    ) -> TransportResult<(Arc<dyn AccountSubscriptionFull>, FullEventsRx)>;

    fn rescan_events(
        &self,
        account: MsgAddressInt,
        since_lt: Option<u64>,
        until_lt: Option<u64>,
    ) -> BoxStream<TransportResult<SliceData>>;
}

#[async_trait]
pub trait AccountSubscription: RunLocal {
    fn since_lt(&self) -> u64;

    async fn simulate_call(
        &self,
        message: InternalMessage,
    ) -> TransportResult<Vec<ton_block::Message>>;

    async fn send_message(
        &self,
        abi: Arc<AbiFunction>,
        message: ExternalMessage,
    ) -> TransportResult<ContractOutput>;

    fn rescan_events(
        &self,
        since_lt: Option<u64>,
        until_lt: Option<u64>,
    ) -> BoxStream<TransportResult<SliceData>>;
}

#[async_trait]
pub trait AccountSubscriptionFull: AccountSubscription {
    fn rescan_events_full(
        &self,
        since_lt: Option<u64>,
        until_lt: Option<u64>,
    ) -> BoxStream<TransportResult<FullEventInfo>>;
}
