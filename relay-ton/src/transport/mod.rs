pub mod errors;
pub mod tonlib_transport;

pub use tonlib_transport::TonlibTransport;

use self::errors::*;
use crate::models::*;
use crate::prelude::*;

#[async_trait]
pub trait Transport: Send + Sync + 'static {
    async fn run_local(
        &self,
        abi: &AbiFunction,
        message: ExternalMessage,
    ) -> TransportResult<ContractOutput>;

    async fn subscribe(&self, addr: &str) -> TransportResult<Arc<dyn AccountSubscription>>;
}

#[async_trait]
pub trait AccountSubscription: Send + Sync {
    fn events(&self) -> watch::Receiver<AccountEvent>;

    async fn run_local(
        &self,
        abi: &AbiFunction,
        message: ExternalMessage,
    ) -> TransportResult<ContractOutput>;

    async fn send_message(
        &self,
        abi: Arc<AbiFunction>,
        message: ExternalMessage,
    ) -> TransportResult<ContractOutput>;
}

#[derive(Debug, Copy, Clone, Ord, PartialOrd, Eq, PartialEq)]
pub enum AccountEvent {
    StateChanged,
}
