use super::message_builder::*;
use crate::models::*;
use crate::prelude::*;
use crate::transport::*;

use super::errors::*;
use super::models::*;
use super::prelude::*;

pub async fn make_eth_event_contract(transport: Arc<dyn Transport>) -> Arc<EthEventContract> {
    Arc::new(EthEventContract::new(transport).await)
}

#[derive(Clone)]
pub struct EthEventContract {
    transport: Arc<dyn Transport>,
    contract: Arc<ton_abi::Contract>,
}

impl EthEventContract {
    pub async fn new(transport: Arc<dyn Transport>) -> Self {
        let contract = Arc::new(
            ton_abi::Contract::load(Cursor::new(ABI)).expect("failed to load EthereumEvent ABI"),
        );

        Self {
            transport,
            contract,
        }
    }

    #[inline]
    fn message(&self, addr: MsgAddrStd, name: &str) -> ContractResult<MessageBuilder> {
        MessageBuilder::new(
            Cow::Owned(ContractConfig {
                account: MsgAddressInt::AddrStd(addr),
                timeout_sec: 60,
            }),
            &self.contract,
            self.transport.as_ref(),
            name,
        )
    }

    pub async fn get_details(&self, addr: MsgAddrStd) -> ContractResult<EthEventDetails> {
        self.message(addr, "getDetails")?
            .run_local()
            .await?
            .try_into()
    }
}

impl Contract for EthEventContract {
    #[inline]
    fn abi(&self) -> &Arc<ton_abi::Contract> {
        &self.contract
    }
}

const ABI: &str = include_str!("../../../abi/EthEvent.abi.json");
