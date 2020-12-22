#![allow(clippy::too_many_arguments)]

use super::errors::*;
use super::models::*;
use super::prelude::*;
use crate::models::*;
use crate::prelude::*;
use crate::transport::*;

#[derive(Clone)]
pub struct BridgeContract {
    transport: Arc<dyn AccountSubscription>,
    keypair: Arc<Keypair>,
    config: ContractConfig,
    events_map: Arc<EventsMap>,
    contract: Arc<ton_abi::Contract>,
}

impl BridgeContract {
    pub async fn new(
        transport: Arc<dyn Transport>,
        account: MsgAddressInt,
        keypair: Arc<Keypair>,
    ) -> ContractResult<Self> {
        let transport = transport.subscribe(account.clone()).await?;
        let contract = abi();
        let events_map = shared_events_map();

        let config = ContractConfig {
            account,
            timeout_sec: 60,
        };

        Ok(Self {
            transport,
            keypair,
            config,
            events_map,
            contract,
        })
    }

    #[inline]
    fn message(&self, name: &str) -> ContractResult<SignedMessageBuilder> {
        SignedMessageBuilder::new(
            Cow::Borrowed(&self.config),
            &self.contract,
            self.transport.as_ref(),
            self.keypair.as_ref(),
            name,
        )
    }

    pub fn address(&self) -> &MsgAddressInt {
        &self.config.account
    }

    pub fn pubkey(&self) -> UInt256 {
        UInt256::from(self.keypair.public.to_bytes())
    }

    pub fn get_known_config_contracts(&self) -> BoxStream<'_, MsgAddrStd> {
        self.transport
            .rescan_events(None, None)
            .filter_map(move |event_body| async move {
                match event_body
                    .map_err(ContractError::TransportError)
                    .and_then(|body| Self::parse_event(&self.events_map, &body))
                {
                    Ok(BridgeContractEvent::NewEthereumEventConfiguration { address }) => {
                        Some(address)
                    }
                    Ok(_) => None,
                    Err(e) => {
                        log::warn!("skipping outbound message. {:?}", e);
                        None
                    }
                }
            })
            .boxed()
    }

    pub async fn confirm_bridge_configuration_update(
        &self,
        configuration: BridgeConfiguration,
    ) -> ContractResult<MsgAddrStd> {
        self.message("confirmBridgeConfigurationUpdate")?
            .args(configuration)
            .send()
            .await?
            .parse_first()
    }

    pub async fn add_ethereum_event_configuration(
        &self,
        new_config: NewEventConfiguration,
    ) -> ContractResult<MsgAddrStd> {
        self.message("addEthereumEventConfiguration")?
            .args(new_config)
            .send()
            .await?
            .parse_first()
    }

    pub async fn confirm_ethereum_event_configuration(
        &self,
        ethereum_event_configuration_address: &MsgAddressInt,
    ) -> ContractResult<()> {
        self.message("confirmEthereumEventConfiguration")?
            .arg(ethereum_event_configuration_address)
            .send()
            .await?
            .ignore_output()
    }

    pub async fn reject_ethereum_event_configuration(
        &self,
        ethereum_event_configuration_address: &MsgAddressInt,
    ) -> ContractResult<()> {
        self.message("rejectEthereumEventConfiguration")?
            .arg(ethereum_event_configuration_address)
            .send()
            .await?
            .ignore_output()
    }

    pub async fn confirm_ethereum_event(
        &self,
        event_transaction: ethereum_types::H256,
        event_index: BigUint,
        event_data: Cell,
        event_block_number: BigUint,
        event_block: ethereum_types::H256,
        ethereum_event_configuration_address: MsgAddressInt,
    ) -> ContractResult<()> {
        log::info!(
            "CONFIRMING: {:?}, {}, {}, {}",
            hex::encode(&event_transaction),
            event_index,
            event_block_number,
            ethereum_event_configuration_address
        );

        self.message("confirmEthereumEvent")?
            .arg(event_transaction)
            .arg(BigUint256(event_index))
            .arg(event_data)
            .arg(BigUint256(event_block_number))
            .arg(event_block)
            .arg(ethereum_event_configuration_address)
            .send()
            .await?
            .ignore_output()
    }

    pub async fn reject_ethereum_event(
        &self,
        event_transaction: ethereum_types::H256,
        event_index: BigUint,
        event_data: Cell,
        event_block_number: BigUint,
        event_block: ethereum_types::H256,
        ethereum_event_configuration_address: MsgAddressInt,
    ) -> ContractResult<()> {
        log::info!(
            "REJECTING: {:?}, {}, {}, {}",
            hex::encode(&event_transaction),
            event_index,
            event_block_number,
            ethereum_event_configuration_address
        );

        self.message("rejectEthereumEvent")?
            .arg(event_transaction)
            .arg(BigUint256(event_index))
            .arg(event_data)
            .arg(BigUint256(event_block_number))
            .arg(event_block)
            .arg(ethereum_event_configuration_address)
            .send()
            .await?
            .ignore_output()
    }

    pub async fn get_details(&self) -> ContractResult<(BridgeConfiguration, SequentialIndex)> {
        self.message("getDetails")?.run_local().await?.parse_all()
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

    #[inline]
    fn subscription(&self) -> &Arc<dyn AccountSubscription> {
        &self.transport
    }
}

static ABI: OnceCell<Arc<AbiContract>> = OnceCell::new();
static EVENTS: OnceCell<Arc<EventsMap>> = OnceCell::new();
const JSON_ABI: &str = include_str!("../../../abi/Bridge.abi.json");

fn abi() -> Arc<AbiContract> {
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
