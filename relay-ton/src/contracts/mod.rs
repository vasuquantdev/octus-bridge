pub use bridge::*;
pub use contract::*;
pub use errors::*;
pub use eth_event::*;
pub use eth_event_configuration::*;
pub use models::*;
pub use ton_event::*;
pub use ton_event_configuration::*;
pub use ton_swapback_contract::*;

pub mod bridge;
pub mod eth_event;
pub mod eth_event_configuration;
pub mod ton_event;
pub mod ton_event_configuration;
pub mod ton_swapback_contract;

mod contract;
pub mod errors;
pub mod message_builder;
pub mod models;
mod prelude;

#[cfg(test)]
mod tests {
    use tokio::stream::StreamExt;

    use crate::prelude::*;
    use crate::transport::graphql_transport::Config;
    use crate::transport::{GraphQLTransport, Transport};

    use super::*;

    const LOCAL_SERVER_ADDR: &str = "http://127.0.0.1:80/graphql";

    pub fn bridge_addr() -> MsgAddressInt {
        MsgAddressInt::from_str(
            "0:22f351d691040450417f429c12d89dd1f1559eeafa19cb92125466ab4bb1b0f6",
        )
        .unwrap()
    }

    pub fn event_proxy_address() -> MsgAddressInt {
        MsgAddressInt::from_str(
            "0:d7997ed240134f63cefce3e5eb6463bcc60a5c92df3bcaaec7264ff10423d4e0",
        )
        .unwrap()
    }

    pub fn ethereum_event_configuration_addr() -> MsgAddressInt {
        MsgAddressInt::from_str(
            "0:a001a219ceb310c73a42fd56e9d849411df786d3c9ea92bb61ae2679460f8c9c",
        )
        .unwrap()
    }

    pub async fn make_transport() -> Arc<dyn Transport> {
        std::env::set_var("RUST_LOG", "relay_ton=debug");
        util::setup();
        let db = sled::Config::new().temporary(true).open().unwrap();

        Arc::new(
            GraphQLTransport::new(
                Config {
                    address: LOCAL_SERVER_ADDR.to_string(),
                    next_block_timeout_sec: 60,
                },
                db,
            )
            .await
            .unwrap(),
        )
    }

    pub fn keypair() -> Arc<Keypair> {
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

    async fn make_bridge(transport: &Arc<dyn Transport>) -> Arc<BridgeContract> {
        Arc::new(
            BridgeContract::new(transport.clone(), bridge_addr(), keypair())
                .await
                .unwrap(),
        )
    }

    async fn make_config_contract(
        transport: &Arc<dyn Transport>,
        addr: MsgAddrStd,
    ) -> Arc<EthEventConfigurationContract> {
        Arc::new(
            EthEventConfigurationContract::new(transport.clone(), MsgAddressInt::AddrStd(addr))
                .await
                .unwrap(),
        )
    }

    async fn make_ethereum_event_contract(transport: &Arc<dyn Transport>) -> Arc<EthEventContract> {
        Arc::new(EthEventContract::new(transport.clone()).await.unwrap())
    }

    #[tokio::test]
    async fn test_flow() {
        let transport = make_transport().await;
        let bridge = make_bridge(&transport).await;

        let known_configs = bridge.get_known_config_contracts().await.unwrap();

        async fn listener(
            transport: Arc<dyn Transport>,
            tx: mpsc::UnboundedSender<EthEventConfigurationContractEvent>,
            config: MsgAddrStd,
        ) {
            log::debug!("start listening config: {:?}", config);

            let configuration_contract = make_config_contract(&transport, config).await;
            let mut eth_events = configuration_contract.events();
            while let Some(event) = eth_events.next().await {
                log::debug!("got event configuration config event: {:?}", event);
                if tx.send(event).is_err() {
                    return;
                }
            }
        }

        let (tx, mut ton_events) = mpsc::unbounded_channel();
        for config in known_configs.into_iter() {
            tokio::spawn(listener(transport.clone(), tx.clone(), config));
        }
        tokio::spawn({
            let transport = transport.clone();
            let bridge = bridge.clone();
            async move {
                let mut bridge_events = bridge.events();
                while let Some(event) = bridge_events.next().await {
                    if let BridgeContractEvent::NewEthereumEventConfiguration { address } = event {
                        tokio::spawn(listener(transport.clone(), tx.clone(), address));
                    }
                }
            }
        });

        // Business logic
        let ethereum_event_contract = make_ethereum_event_contract(&transport).await;
        tokio::spawn(async move {
            while let Some(event) = ton_events.next().await {
                if let EthEventConfigurationContractEvent::NewEthereumEventConfirmation {
                    address,
                    ..
                } = event
                {
                    let details = ethereum_event_contract.get_details(address).await.unwrap();
                    println!("got ethereum event: {:?}", details);
                }
            }
        });

        tokio::time::delay_for(tokio::time::Duration::from_secs(1)).await;

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
            "added event configuration address: {:?}",
            event_configuration_address
        );

        tokio::time::delay_for(tokio::time::Duration::from_secs(10)).await;
    }
}
