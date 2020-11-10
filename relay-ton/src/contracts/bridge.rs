use std::io::Cursor;

use ton_abi::Contract;

use super::errors::*;
use super::prelude::*;
use crate::models::*;
use crate::prelude::*;
use crate::transport::*;

pub struct BridgeContract {
    transport: Arc<dyn Transport>,
    config: ContractConfig,
    contract: Contract,
}

impl BridgeContract {
    pub fn new(transport: &Arc<dyn Transport>, account: &MsgAddressInt) -> BridgeContract {
        let contract =
            Contract::load(Cursor::new(ABI)).expect("Failed to load bridge contract ABI");

        Self {
            transport: transport.clone(),
            config: ContractConfig {
                account: account.clone(),
                timeout_sec: 60,
            },
            contract,
        }
    }

    pub async fn add_ethereum_event_configuration(
        &self,
        ethereum_event_abi: &str,
        ethereum_address: &str,
        event_proxy_address: &AccountId,
    ) -> ContractResult<()> {
        let _ = MessageBuilder::with_args(
            &self.config,
            &self.contract,
            "addEthereumEventConfiguration",
            3,
        )?
        .arg("ethereumEventABI", ethereum_event_abi)
        .arg("ethereumAddress", ethereum_address)
        .arg("address", event_proxy_address)
        .send(self.transport.as_ref())
        .await?;

        Ok(())
    }

    pub async fn confirm_ethereum_event_configuration(
        &self,
        ethereum_event_configuration_id: UInt256,
    ) -> ContractResult<()> {
        let _ = MessageBuilder::with_args(
            &self.config,
            &self.contract,
            "confirmEthereumEventConfiguration",
            1,
        )?
        .arg(
            "ethereumEventConfigurationID",
            ethereum_event_configuration_id,
        )
        .send(self.transport.as_ref())
        .await?;

        Ok(())
    }

    pub async fn emit_event_instance(&self) -> ContractResult<()> {
        todo!()
    }

    pub async fn confirm_event_instance(
        &self,
        ethereum_event_configuration_id: UInt256,
        ethereum_event_data: BuilderData,
    ) -> ContractResult<()> {
        let _ = MessageBuilder::with_args(&self.config, &self.contract, "confirmEventInstance", 2)?
            .arg(
                "ethereumEventConfigurationID",
                ethereum_event_configuration_id,
            )
            .arg("ethereumEventData", ethereum_event_data)
            .send(self.transport.as_ref())
            .await?;

        Ok(())
    }

    pub async fn get_ethereum_events_configuration(
        &self,
    ) -> ContractResult<Vec<EthereumEventsConfiguration>> {
        let message = MessageBuilder::empty(
            &self.config,
            &self.contract,
            "getEthereumEventsConfiguration",
        )?
        .run_local()
        .send(self.transport.as_ref())
        .await?;

        println!("{:?}", message);

        Ok(Vec::new())
    }
}

#[derive(Debug, Clone)]
pub struct EthereumEventsConfiguration {
    pub ethereum_event_abi: String,
    pub ethereum_address: String,
    pub event_proxy_address: String,
    pub confirmations: u64,
    pub confirmed: bool,
}

const ABI: &str = include_str!("../../abi/Bridge.abi.json");

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::graphql_transport::config::ClientConfig;
    use crate::transport::GraphQlTransport;

    const LOCAL_SERVER_ADDR: &str = "http://127.0.0.1:80";

    fn bridge_addr() -> MsgAddressInt {
        MsgAddressInt::from_str(
            "0:7c6a933179824c23c3f684f28df909ed13cb371f7f22a118241237d7bec1a2de",
        )
        .unwrap()
    }

    #[tokio::test]
    async fn get_ethereum_events_configuration() {
        let transport: Arc<dyn Transport> = Arc::new(
            GraphQlTransport::new(ClientConfig {
                server_address: LOCAL_SERVER_ADDR.to_owned(),
                ..Default::default()
            })
            .await
            .unwrap(),
        );

        let bridge = BridgeContract::new(&transport, &bridge_addr());
        let config = bridge.get_ethereum_events_configuration().await.unwrap();
        println!("Configs: {:#?}", config);
    }
}
