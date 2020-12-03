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
    use crate::transport::graphql_transport::Config;
    use crate::transport::GraphQLTransport;
    use tokio::stream::StreamExt;

    const LOCAL_SERVER_ADDR: &str = "http://127.0.0.1:80/graphql";

    fn ethereum_event_configuration_addr() -> MsgAddressInt {
        MsgAddressInt::from_str(
            "0:b739b86aa55d3016beb44b9ac97edfa4f8221b09320a622c9a0ead70eddf4a02",
        )
        .unwrap()
    }

    async fn make_transport() -> Arc<dyn Transport> {
        std::env::set_var("RUST_LOG", "relay_ton=debug");
        util::setup();
        let db = sled::Config::new().temporary(true).open().unwrap();

        Arc::new(
            GraphQLTransport::new(
                Config {
                    addr: LOCAL_SERVER_ADDR.to_string(),
                    next_block_timeout_sec: 60,
                },
                db,
            )
            .await
            .unwrap(),
        )
    }

    async fn make_config_contract() -> EthereumEventConfigurationContract {
        EthereumEventConfigurationContract::new(
            make_transport().await,
            ethereum_event_configuration_addr(),
        )
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn get_details() {
        let config_contract = make_config_contract().await;
        let details = config_contract.get_details().await.unwrap();
        println!("Details: {:?}", details);
    }

    #[tokio::test]
    async fn subscribe() {
        let config_contract = Arc::new(make_config_contract().await);

        let mut events = config_contract.events();
        while let Some(event) = events.next().await {
            log::debug!("New event at: {:?}", event);
        }
    }
}
