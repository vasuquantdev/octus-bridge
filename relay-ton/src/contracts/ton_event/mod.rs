use crate::models::*;
use crate::prelude::*;
use crate::transport::*;

use super::errors::*;
use super::message_builder::*;
use super::models::*;
use super::prelude::*;

pub async fn make_ton_event_contract(transport: Arc<dyn Transport>) -> Arc<TonEventContract> {
    Arc::new(TonEventContract::new(transport).await)
}

#[derive(Clone)]
pub struct TonEventContract {
    transport: Arc<dyn Transport>,
    contract: Arc<ton_abi::Contract>,
}

impl TonEventContract {
    pub async fn new(transport: Arc<dyn Transport>) -> Self {
        let contract = Arc::new(
            ton_abi::Contract::load(Cursor::new(ABI)).expect("failed to load TonEvent ABI"),
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

    pub async fn get_details(&self, addr: MsgAddrStd) -> ContractResult<TonEventDetails> {
        self.message(addr, "getDetails")?
            .run_local()
            .await?
            .try_into()
    }
}

impl Contract for TonEventContract {
    #[inline]
    fn abi(&self) -> &Arc<ton_abi::Contract> {
        &self.contract
    }
}

const ABI: &str = include_str!("../../../abi/TonEvent.abi.json");
