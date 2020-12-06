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
    contract: Arc<ton_abi::Contract>,
}

impl BridgeContract {
    pub async fn new(
        transport: Arc<dyn Transport>,
        account: MsgAddressInt,
        keypair: Arc<Keypair>,
    ) -> ContractResult<Self> {
        let transport = transport.subscribe(account.clone()).await?;

        let config = ContractConfig {
            account,
            timeout_sec: 60,
        };

        let contract = Arc::new(
            ton_abi::Contract::load(Cursor::new(ABI))
                .expect("failed to load bridge BridgeContract ABI"),
        );

        Ok(Self {
            transport,
            keypair,
            config,
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

    pub async fn get_known_config_contracts(&self) -> ContractResult<Vec<MsgAddrStd>> {
        let events = self.events_map();
        let mut scanner = self.transport.rescan_events(None, None);
        let mut configs = Vec::new();
        while let Some(event_body) = scanner.next().await {
            match event_body
                .map_err(ContractError::TransportError)
                .and_then(|body| Self::parse_event(&events, &body))
            {
                Ok(event) => match event {
                    BridgeContractEvent::NewEthereumEventConfiguration { address } => {
                        configs.push(address);
                    }
                },
                Err(e) => {
                    log::warn!("skipping outbound message. {:?}", e);
                }
            }
        }
        Ok(configs)
    }

    pub async fn add_ethereum_event_configuration(
        &self,
        ethereum_event_abi: &str,
        ethereum_address: Vec<u8>,
        ethereum_event_blocks_to_confirm: BigUint,
        ethereum_event_required_confirmations: BigUint,
        ethereum_event_required_rejects: BigUint,
        event_proxy_address: &MsgAddressInt,
    ) -> ContractResult<MsgAddrStd> {
        self.message("addEthereumEventConfiguration")?
            .arg(ethereum_event_abi)
            .arg(ethereum_address)
            .arg(ethereum_event_blocks_to_confirm)
            .arg(ethereum_event_required_confirmations)
            .arg(ethereum_event_required_rejects)
            .arg(event_proxy_address)
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
        event_transaction: Vec<u8>,
        event_index: BigUint,
        event_data: Cell,
        event_block_number: BigUint,
        event_block: Vec<u8>,
        ethereum_event_configuration_address: MsgAddressInt,
    ) -> ContractResult<()> {
        self.message("confirmEthereumEvent")?
            .arg(event_transaction)
            .arg(event_index)
            .arg(event_data)
            .arg(event_block_number)
            .arg(event_block)
            .arg(ethereum_event_configuration_address)
            .send()
            .await?
            .ignore_output()
    }

    pub async fn reject_ethereum_event(
        &self,
        event_transaction: Vec<u8>,
        event_index: BigUint,
        event_data: Cell,
        event_block_number: BigUint,
        event_block: Vec<u8>,
        ethereum_event_configuration_address: MsgAddressInt,
    ) -> ContractResult<()> {
        self.message("rejectEthereumEvent")?
            .arg(event_transaction)
            .arg(event_index)
            .arg(event_data)
            .arg(event_block_number)
            .arg(event_block)
            .arg(ethereum_event_configuration_address)
            .send()
            .await?
            .ignore_output()
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

const ABI: &str = include_str!("../../../abi/Bridge.abi.json");

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::graphql_transport::Config;
    use crate::transport::GraphQLTransport;
    use util::setup;

    const LOCAL_SERVER_ADDR: &str = "http://127.0.0.1:80/graphql";

    fn bridge_addr() -> MsgAddressInt {
        MsgAddressInt::from_str(
            "0:afafb5172cc4423266311712e0b6132cc3800d454c21335ea363eb353acda59c",
        )
        .unwrap()
    }

    fn event_proxy_address() -> MsgAddressInt {
        MsgAddressInt::from_str(
            "0:d7997ed240134f63cefce3e5eb6463bcc60a5c92df3bcaaec7264ff10423d4e0",
        )
        .unwrap()
    }

    async fn make_transport() -> Arc<dyn Transport> {
        std::env::set_var("RUST_LOG", "relay_ton=debug");
        setup();
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

    fn keypair() -> Arc<Keypair> {
        let ton_private_key = ed25519_dalek::SecretKey::from_bytes(
            &hex::decode("90f71be09b86a65791fc0740598849f00066d0ae81ed5f8b2aa8f2e3522a991e")
                .unwrap(),
        )
        .unwrap();
        let ton_public_key = ed25519_dalek::PublicKey::from(&ton_private_key);

        Arc::new(ed25519_dalek::Keypair {
            secret: ton_private_key,
            public: ton_public_key,
        })
    }

    async fn make_bridge() -> BridgeContract {
        BridgeContract::new(make_transport().await, bridge_addr(), keypair())
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn get_known_contracts() {
        setup();
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
