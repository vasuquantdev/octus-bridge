#![allow(clippy::too_many_arguments)]

use super::errors::*;
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
            .parse_all()
    }

    pub async fn get_bridge_configuration_votes(
        &self,
        configuration: BridgeConfiguration,
    ) -> ContractResult<(Vec<MsgAddressInt>, Vec<MsgAddressInt>)> {
        self.message("getBridgeConfigurationVotes")?
            .arg(configuration)
            .run_local()
            .await?
            .parse_all()
    }

    pub async fn is_relay_active(&self, relay: MsgAddressInt) -> ContractResult<bool> {
        self.message("getAccountStatus")?
            .arg(relay)
            .run_local()
            .await?
            .parse_first()
    }

    pub async fn get_details(&self) -> ContractResult<BridgeConfiguration> {
        self.message("getDetails")?.run_local().await?.parse_first()
    }

    pub async fn get_ethereum_account(
        &self,
        relay: MsgAddressInt,
    ) -> ContractResult<ethereum_types::Address> {
        self.message("getEthereumAccount")?
            .arg(relay)
            .run_local()
            .await?
            .parse_first()
    }

    pub async fn get_keys(&self) -> ContractResult<Vec<BridgeKey>> {
        self.message("getAccounts")?.run_local().await?.parse_all()
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contracts::tests::*;
    use crate::transport::graphql_transport::Config;
    use crate::transport::GraphQLTransport;

    async fn make_bridge() -> BridgeContract {
        BridgeContract::new(make_transport().await, bridge_addr(), keypair())
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn get_known_contracts() {
        util::setup();
        let bridge = make_bridge().await;
        let configs = bridge.get_known_config_contracts().await.unwrap();
        println!("Configs: {:?}", configs);
    }

    #[tokio::test]
    async fn add_ethereum_event_configuration() {
        let bridge = make_bridge().await;

        let configs_before = bridge.get_known_config_contracts().await.unwrap();

        let event_configuration_address = bridge
            .add_ethereum_event_configuration(
                "Test ABI",
                Vec::new(),
                BigUint::from(10u8),
                BigUint::from(10u8),
                BigUint::from(10u8),
                &event_proxy_address(),
            )
            .await
            .unwrap();
        log::debug!(
            "event_configuration_address: {:?}",
            event_configuration_address
        );

        let configs_after = bridge.get_known_config_contracts().await.unwrap();

        assert_eq!(configs_after.len(), configs_before.len() + 1);
    }
}
