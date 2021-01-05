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
    keypair: Arc<Keypair>,
) -> ContractResult<(
    Arc<BridgeContract>,
    EventsRx<<BridgeContract as ContractWithEvents>::Event>,
)> {
    let (transport, events_rx) = transport.subscribe(account.clone()).await?;
    let contract = abi();
    let events_map = shared_events_map();

    let config = ContractConfig {
        account,
        timeout_sec: 60,
    };

    let contract = Arc::new(BridgeContract {
        transport,
        keypair,
        config,
        events_map,
        contract,
    });

    let events_rx = start_processing_events(&contract, events_rx);

    Ok((contract, events_rx))
}

#[derive(Clone)]
pub struct BridgeContract {
    transport: Arc<dyn AccountSubscription>,
    keypair: Arc<Keypair>,
    config: ContractConfig,
    events_map: Arc<EventsMap>,
    contract: Arc<ton_abi::Contract>,
}

impl BridgeContract {
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

    pub async fn update_event_configuration(
        &self,
        event_configuration: &MsgAddressInt,
        voting: Voting,
    ) -> ContractResult<()> {
        self.message("updateEventConfiguration")?
            .arg(event_configuration)
            .arg(voting)
            .send()
            .await?
            .ignore_output()
    }

    pub async fn get_details(&self) -> ContractResult<BridgeConfiguration> {
        self.message("getDetails")?.run_local().await?.parse_first()
    }

    pub async fn get_event_configuration_status(
        &self,
        event_configuration: &MsgAddressInt,
    ) -> ContractResult<EventConfigurationStatus> {
        self.message("getEventConfigurationStatus")?
            .arg(event_configuration)
            .run_local()
            .await?
            .parse_all()
    }

    pub async fn get_active_event_configurations(&self) -> ContractResult<Vec<MsgAddressInt>> {
        self.message("getActiveEventConfigurations")?
            .run_local()
            .await?
            .parse_first()
    }

    pub async fn confirm_ethereum_event(
        &self,
        event_transaction: ethereum_types::H256,
        event_index: BigUint,
        event_data: Cell,
        event_block_number: BigUint,
        event_block: ethereum_types::H256,
        event_configuration: MsgAddressInt,
    ) -> ContractResult<()> {
        log::info!(
            "CONFIRMING ETH EVENT: {:?}, {}, {}, {}",
            hex::encode(&event_transaction),
            event_index,
            event_block_number,
            event_configuration
        );

        self.message("confirmEthereumEvent")?
            .arg(event_transaction)
            .arg(BigUint256(event_index))
            .arg(event_data)
            .arg(BigUint256(event_block_number))
            .arg(event_block)
            .arg(event_configuration)
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
        event_configuration: MsgAddressInt,
    ) -> ContractResult<()> {
        log::info!(
            "REJECTING ETH EVENT: {:?}, {}, {}, {}",
            hex::encode(&event_transaction),
            event_index,
            event_block_number,
            event_configuration
        );

        self.message("rejectEthereumEvent")?
            .arg(event_transaction)
            .arg(BigUint256(event_index))
            .arg(event_data)
            .arg(BigUint256(event_block_number))
            .arg(event_block)
            .arg(event_configuration)
            .send()
            .await?
            .ignore_output()
    }

    pub async fn confirm_ton_event(
        &self,
        event_transaction: ethereum_types::H256,
        event_index: BigUint,
        event_data: Cell,
        event_block_number: BigUint,
        event_block: ethereum_types::H256,
        event_data_signature: Vec<u8>,
        event_configuration: MsgAddressInt,
    ) -> ContractResult<()> {
        log::info!(
            "CONFIRMING TON EVENT: {:?}, {}, {}, {}",
            hex::encode(&event_transaction),
            event_index,
            event_block_number,
            event_configuration
        );

        self.message("confirmTonEvent")?
            .arg(event_transaction)
            .arg(BigUint256(event_index))
            .arg(event_data)
            .arg(BigUint256(event_block_number))
            .arg(event_block)
            .arg(event_data_signature)
            .arg(event_configuration)
            .send()
            .await?
            .ignore_output()
    }

    pub async fn reject_ton_event(
        &self,
        event_transaction: ethereum_types::H256,
        event_index: BigUint,
        event_data: Cell,
        event_block_number: BigUint,
        event_block: ethereum_types::H256,
        event_configuration: MsgAddressInt,
    ) -> ContractResult<()> {
        log::info!(
            "CONFIRMING TON EVENT: {:?}, {}, {}, {}",
            hex::encode(&event_transaction),
            event_index,
            event_block_number,
            event_configuration
        );

        self.message("rejectTonEvent")?
            .arg(event_transaction)
            .arg(BigUint256(event_index))
            .arg(event_data)
            .arg(BigUint256(event_block_number))
            .arg(event_block)
            .arg(event_configuration)
            .send()
            .await?
            .ignore_output()
    }

    pub async fn update_bridge_configuration(
        &self,
        configuration: BridgeConfiguration,
        vote: VoteData,
    ) -> ContractResult<()> {
        self.message("updateBridgeConfiguration")?
            .arg(configuration)
            .arg(vote)
            .send()
            .await?
            .ignore_output()
    }

    pub async fn get_bridge_configuration_votes(
        &self,
        configuration: BridgeConfiguration,
    ) -> ContractResult<(Vec<UInt256>, Vec<UInt256>)> {
        self.message("getBridgeConfigurationVotes")?
            .arg(configuration)
            .run_local()
            .await?
            .parse_all()
    }

    pub async fn is_key_active(&self, key: UInt256) -> ContractResult<bool> {
        self.message("getKeyStatus")?
            .arg(key)
            .run_local()
            .await?
            .parse_first()
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
