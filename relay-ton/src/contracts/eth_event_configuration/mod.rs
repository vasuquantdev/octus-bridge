use super::errors::*;
use super::message_builder::*;
use super::models::*;
use super::prelude::*;
use crate::models::*;
use crate::prelude::*;
use crate::transport::*;

pub async fn make_eth_event_configuration_contract(
    transport: Arc<dyn Transport>,
    account: MsgAddressInt,
    bridge_address: MsgAddressInt,
) -> ContractResult<(
    Arc<EthEventConfigurationContract>,
    EventsRx<<EthEventConfigurationContract as ContractWithEvents>::Event>,
)> {
    let (subscription, events_rx) = transport.subscribe(account.clone()).await?;
    let contract = abi();
    let events_map = shared_events_map();

    let config = ContractConfig {
        account,
        timeout_sec: 60,
    };

    let contract = Arc::new(EthEventConfigurationContract {
        transport,
        subscription,
        contract,
        events_map,
        config,
        bridge_address,
    });

    let events_rx = start_processing_events(&contract, events_rx);

    Ok((contract, events_rx))
}

#[derive(Clone)]
pub struct EthEventConfigurationContract {
    transport: Arc<dyn Transport>,
    subscription: Arc<dyn AccountSubscription>,
    contract: Arc<ton_abi::Contract>,
    events_map: Arc<EventsMap>,
    config: ContractConfig,
    bridge_address: MsgAddressInt,
}

impl EthEventConfigurationContract {
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

    /// Returns logical time right before subscription started.
    pub fn since_lt(&self) -> u64 {
        self.subscription.since_lt()
    }

    /// Returns iterator over events in reversed order since lt (inclusive), until lt (also inclusive)
    pub fn get_known_events(
        &self,
        since_lt: Option<u64>,
        until_lt: u64,
    ) -> BoxStream<'_, EthEventConfigurationContractEvent> {
        let stream = self
            .subscription
            .rescan_events(since_lt, Some(until_lt + 1))
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
            });
        stream.boxed()
    }

    pub async fn compute_event_address(
        &self,
        vote: EthEventVoteData,
    ) -> ContractResult<MsgAddrStd> {
        const TON: u64 = 1_000_000_000;
        const CONFIRM_VALUE: u64 = 1_000_000 * TON;

        let message = self
            .message("confirmEvent")?
            .arg(vote)
            .arg(MsgAddrStd::default())
            .build_internal(self.bridge_address.clone(), CONFIRM_VALUE)?;

        let messages = self.subscription.simulate_call(message).await?;
        for msg in messages {
            if !matches!(msg.header(), ton_block::CommonMsgInfo::ExtOutMsgInfo(_)) {
                continue;
            }

            let body = msg.body().ok_or_else(|| TransportError::ExecutionError {
                reason: "output message has not body".to_string(),
            })?;

            type Event = <EthEventConfigurationContract as ContractWithEvents>::Event;
            if let Ok(Event::EventConfirmation { address, .. }) =
                Self::parse_event(&self.events_map, &body)
            {
                return Ok(address);
            }
        }

        Err(ContractError::UnknownEvent)
    }

    pub async fn get_details(&self) -> ContractResult<EthEventConfiguration> {
        self.message("getDetails")?.run_local().await?.try_into()
    }
}

impl Contract for EthEventConfigurationContract {
    #[inline]
    fn abi(&self) -> &Arc<ton_abi::Contract> {
        &self.contract
    }
}

impl ContractWithEvents for EthEventConfigurationContract {
    type Event = EthEventConfigurationContractEvent;
    type EventKind = EthEventConfigurationContractEventKind;
}

fn abi() -> Arc<AbiContract> {
    ABI.get_or_init(|| {
        Arc::new(
            AbiContract::load(Cursor::new(JSON_ABI))
                .expect("failed to load EthereumEventConfiguration ABI"),
        )
    })
    .clone()
}

fn shared_events_map() -> Arc<EventsMap> {
    EVENTS
        .get_or_init(|| {
            Arc::new(make_events_map::<EthEventConfigurationContract>(
                abi().as_ref(),
            ))
        })
        .clone()
}

static ABI: OnceCell<Arc<AbiContract>> = OnceCell::new();
static EVENTS: OnceCell<Arc<EventsMap>> = OnceCell::new();
const JSON_ABI: &str = include_str!("../../../abi/EthEventConfiguration.abi.json");

type EventsMap = HashMap<
    u32,
    (
        <EthEventConfigurationContract as ContractWithEvents>::EventKind,
        AbiEvent,
    ),
>;
