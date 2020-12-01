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
        account: &MsgAddressInt,
        keypair: Arc<Keypair>,
    ) -> ContractResult<Self> {
        let transport = transport.subscribe(&account.to_string()).await?;

        let config = ContractConfig {
            account: account.clone(),
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

    async fn get_known_config_contracts(&self) -> ContractResult<Vec<MsgAddrStd>> {
        let events = self.events_map();
        let mut scanner = self.transport.rescan_events(None, None)?;
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
                    log::error!("failed to parse event. {}", e);
                }
            }
        }
        Ok(configs)
    }

    pub async fn add_ethereum_event_configuration(
        &self,
        ethereum_event_abi: &str,
        ethereum_address: &str,
        event_proxy_address: &MsgAddrStd,
    ) -> ContractResult<MsgAddrStd> {
        self.message("addEthereumEventConfiguration")?
            .arg(ethereum_event_abi)
            .arg(ethereum_address)
            .arg(event_proxy_address)
            .send()
            .await?
            .parse_first()
    }

    pub async fn confirm_ethereum_event_configuration(
        &self,
        ethereum_event_configuration_address: &MsgAddrStd,
    ) -> ContractResult<()> {
        self.message("confirmEthereumEventConfiguration")?
            .arg(ethereum_event_configuration_address)
            .send()
            .await?
            .ignore_output()
    }

    pub async fn reject_ethereum_event_configuration(
        &self,
        ethereum_event_configuration_address: &MsgAddrStd,
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
        ethereum_event_configuration_address: MsgAddressInt,
    ) -> ContractResult<()> {
        self.message("confirmEthereumEvent")?
            .arg(event_transaction)
            .arg(event_index)
            .arg(event_data)
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

    const LOCAL_SERVER_ADDR: &str = "http://127.0.0.1:80/graphql";

    fn bridge_addr() -> MsgAddressInt {
        MsgAddressInt::from_str(
            "0:a3fb29fb5d681820eb8a45714101fccd6ff6e7e742f29549a0a87dbb505c50ba",
        )
        .unwrap()
    }

    async fn make_transport() -> Arc<dyn Transport> {
        std::env::set_var("RUST_LOG", "relay_ton::transport::graphql_transport=debug");
        env_logger::init();

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
            &hex::decode("e371ef1d7266fc47b30d49dc886861598f09e2e6294d7f0520fe9aa460114e51")
                .unwrap(),
        )
        .unwrap();
        let ton_public_key = ed25519_dalek::PublicKey::from(&ton_private_key);

        Arc::new(ed25519_dalek::Keypair {
            secret: ton_private_key,
            public: ton_public_key,
        })
    }

    #[tokio::test]
    async fn get_known_contracts() {
        let transport = make_transport().await;
        let keypair = keypair();

        let bridge = BridgeContract::new(transport, &bridge_addr(), keypair)
            .await
            .unwrap();
        let configs = bridge.get_known_config_contracts().await.unwrap();
        println!("Configs: {:?}", configs);
    }
}
