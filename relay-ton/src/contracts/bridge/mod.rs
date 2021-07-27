#![allow(clippy::too_many_arguments)]

use nekoton_parser::abi::UnpackFirst;

use super::errors::*;
use super::message_builder::*;
use super::models::*;
use super::prelude::*;
use crate::models::*;
use crate::prelude::*;
use crate::transport::*;

pub async fn make_bridge_contract(
    transport: Arc<dyn Transport>,
    account: MsgAddressInt,
) -> ContractResult<(
    Arc<BridgeContract>,
    EventsRx<<BridgeContract as ContractWithEvents>::Event>,
)> {
    let (subscription, events_rx) = transport.subscribe(account.clone()).await?;
    let contract = abi();
    let events_map = shared_events_map();

    let config = ContractConfig {
        account,
        timeout_sec: 60,
    };

    let contract = Arc::new(BridgeContract {
        transport,
        subscription,
        config,
        events_map,
        contract,
    });

    let events_rx = start_processing_events(&contract, events_rx);

    Ok((contract, events_rx))
}

#[derive(Clone)]
pub struct BridgeContract {
    transport: Arc<dyn Transport>,
    subscription: Arc<dyn AccountSubscription>,
    config: ContractConfig,
    events_map: Arc<EventsMap>,
    contract: Arc<AbiContract>,
}

impl BridgeContract {
    pub fn address(&self) -> &MsgAddressInt {
        &self.config.account
    }

    pub async fn get_active_event_configurations(
        &self,
    ) -> ContractResult<Vec<ActiveEventConfiguration>> {
        self.message("getActiveEventConfigurations")?
            .run_local()
            .await?
            .try_into()
    }

    pub async fn get_bridge_configuration_votes(
        &self,
        configuration: BridgeConfiguration,
    ) -> ContractResult<(Vec<MsgAddressInt>, Vec<MsgAddressInt>)> {
        self.message("getBridgeConfigurationVotes")?
            .arg(configuration)
            .run_local()
            .await?
            .try_into()
    }

    pub async fn is_relay_active(&self, relay: MsgAddressInt) -> ContractResult<bool> {
        Ok(self
            .message("getAccountStatus")?
            .arg(relay)
            .run_local()
            .await?
            .tokens
            .unpack_first()?)
    }

    pub async fn get_details(&self) -> ContractResult<BridgeConfiguration> {
        Ok(self
            .message("getDetails")?
            .run_local()
            .await?
            .tokens
            .unpack_first()?)
    }

    pub async fn get_ethereum_account(
        &self,
        relay: MsgAddressInt,
    ) -> ContractResult<EthereumAccount> {
        Ok(self
            .message("getEthereumAccount")?
            .arg(relay)
            .run_local()
            .await?
            .tokens
            .unpack_first()?)
    }

    pub async fn get_keys(&self) -> ContractResult<Vec<BridgeKey>> {
        self.message("getAccounts")?.run_local().await?.try_into()
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
}

impl Contract for BridgeContract {
    #[inline]
    fn abi(&self) -> &Arc<ton_abi::Contract> {
        &self.contract
    }
}

impl ContractWithEvents for BridgeContract {
    type Event = BridgeContractEvent;
    type EventKind = BridgeContractEventKind;
}

static ABI: OnceCell<Arc<AbiContract>> = OnceCell::new();
static EVENTS: OnceCell<Arc<EventsMap>> = OnceCell::new();
const JSON_ABI: &str = include_str!("../../../abi/Bridge.abi.json");

pub fn abi() -> Arc<AbiContract> {
    ABI.get_or_init(|| {
        Arc::new(
            AbiContract::load(Cursor::new(JSON_ABI))
                .expect("failed to load bridge BridgeContract ABI"),
        )
    })
    .clone()
}

fn shared_events_map() -> Arc<EventsMap> {
    EVENTS
        .get_or_init(|| Arc::new(make_events_map::<BridgeContract>(abi().as_ref())))
        .clone()
}

type EventsMap = HashMap<u32, (<BridgeContract as ContractWithEvents>::EventKind, AbiEvent)>;
