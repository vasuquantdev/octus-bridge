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
    events_map: Arc<EventsMap>,
    config: ContractConfig,
    bridge_address: MsgAddressInt,
}

impl EthereumEventConfigurationContract {
    pub async fn new(
        transport: Arc<dyn Transport>,
        account: MsgAddressInt,
        bridge_address: MsgAddressInt,
    ) -> ContractResult<Self> {
        let subscription = transport.subscribe(account.clone()).await?;
        let contract = abi();
        let events_map = shared_events_map();

        let config = ContractConfig {
            account,
            timeout_sec: 60,
        };

        Ok(Self {
            transport,
            subscription,
            contract,
            events_map,
            config,
            bridge_address,
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

    pub fn address(&self) -> &MsgAddressInt {
        &self.config.account
    }

    pub fn get_known_events(&self) -> BoxStream<'_, EthereumEventConfigurationContractEvent> {
        self.subscription
            .rescan_events(None, None)
            .filter_map(move |event_body| async move {
                match event_body
                    .map_err(ContractError::TransportError)
                    .and_then(|body| Self::parse_event(&self.events_map, &body))
                {
                    Ok(event) => Some(event),
                    Err(e) => {
                        log::warn!("skipping outbound message. {:?}", e);
                        None
                    }
                }
            })
            .boxed()
    }

    pub async fn compute_event_address(
        &self,
        event_transaction: Vec<u8>,
        event_index: BigUint,
        event_data: Cell,
        event_block_number: BigUint,
        event_block: Vec<u8>,
    ) -> ContractResult<MsgAddrStd> {
        const TON: u64 = 1_000_000_000;
        const CONFIRM_VALUE: u64 = 1_000_000 * TON;

        let message = self
            .message("confirmEvent")?
            .arg(event_transaction)
            .arg(BigUint256(event_index))
            .arg(event_data)
            .arg(BigUint256(event_block_number))
            .arg(event_block)
            .arg(BigUint256(0u8.into()))
            .build_internal(self.bridge_address.clone(), CONFIRM_VALUE)?;

        let messages = self.subscription.simulate_call(message).await?;
        for msg in messages {
            if !matches!(msg.header(), ton_block::CommonMsgInfo::ExtOutMsgInfo(_)) {
                continue;
            }

            let body = msg.body().ok_or_else(|| TransportError::ExecutionError {
                reason: "output message has not body".to_string(),
            })?;

            type Event = <EthereumEventConfigurationContract as ContractWithEvents>::Event;
            if let Ok(Event::NewEthereumEventConfirmation { address, .. }) =
                Self::parse_event(&self.events_map, &body)
            {
                return Ok(address);
            }
        }

        Err(ContractError::UnknownEvent)
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

fn shared_events_map() -> Arc<EventsMap> {
    EVENTS
        .get_or_init(|| {
            Arc::new(make_events_map::<EthereumEventConfigurationContract>(
                abi().as_ref(),
            ))
        })
        .clone()
}

static ABI: OnceCell<Arc<AbiContract>> = OnceCell::new();
static EVENTS: OnceCell<Arc<EventsMap>> = OnceCell::new();
const JSON_ABI: &str = include_str!("../../../abi/EthereumEventConfiguration.abi.json");

type EventsMap = HashMap<
    u32,
    (
        <EthereumEventConfigurationContract as ContractWithEvents>::EventKind,
        AbiEvent,
    ),
>;

#[cfg(test)]
mod test {
    use super::*;
    use crate::contracts::tests::*;
    use crate::transport::graphql_transport::Config;
    use crate::transport::GraphQLTransport;
    use tokio::stream::StreamExt;

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

        tokio::spawn(async move {
            let mut events = config_contract.events();
            while let Some(event) = events.next().await {
                log::debug!("New event at: {:?}", event);
            }
        });

        tokio::time::delay_for(tokio::time::Duration::from_secs(10)).await;
    }
}
