use super::errors::*;
use super::models::*;
use super::prelude::*;
use crate::models::*;
use crate::prelude::*;
use crate::transport::*;

#[derive(Clone)]
pub struct EthereumEventConfigurationContract {
    transport: Arc<dyn Transport>,
    subscription: Arc<dyn AccountSubscription>,
    contract: Arc<ton_abi::Contract>,
    config: ContractConfig,
}

impl EthereumEventConfigurationContract {
    pub async fn new(
        transport: Arc<dyn Transport>,
        account: MsgAddressInt,
    ) -> ContractResult<Self> {
        let subscription = transport.subscribe(&account.to_string()).await?;
        let contract = abi();

        let config = ContractConfig {
            account,
            timeout_sec: 60,
        };

        Ok(Self {
            transport,
            subscription,
            contract,
            config,
        })
    }

    #[inline]
    fn message(&self, name: &str) -> ContractResult<MessageBuilder> {
        MessageBuilder::new(
            Cow::Borrowed(&self.config),
            &self.contract,
            self.transport.as_ref(),
            name,
        )
    }

    pub async fn get_details(&self) -> ContractResult<EthereumEventConfiguration> {
        self.message("getDetails")?.run_local().await?.parse_all()
    }

    pub async fn get_details_hash(&self) -> ContractResult<UInt256> {
        self.message("getDetails")?.run_local().await?.hash()
    }
}

impl Contract for EthereumEventConfigurationContract {
    #[inline]
    fn abi(&self) -> &Arc<ton_abi::Contract> {
        &self.contract
    }
}

impl ContractWithEvents for EthereumEventConfigurationContract {
    type Event = EthereumEventConfigurationContractEvent;
    type EventKind = EthereumEventConfigurationContractEventKind;

    fn subscription(&self) -> &Arc<dyn AccountSubscription> {
        &self.subscription
    }
}

fn abi() -> Arc<AbiContract> {
    ABI.get_or_init(|| {
        Arc::new(
            AbiContract::load(Cursor::new(JSON_ABI))
                .expect("failed to load bridge EthereumEventConfigurationContract ABI"),
        )
    })
    .clone()
}

static ABI: OnceCell<Arc<AbiContract>> = OnceCell::new();
const JSON_ABI: &str = include_str!("../../../abi/EthereumEventConfiguration.abi.json");

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test() {}
}
