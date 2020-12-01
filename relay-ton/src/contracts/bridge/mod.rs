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
    // use super::*;
    // use crate::transport::tonlib_transport::config::Config;
    // use crate::transport::TonlibTransport;
    //
    // const LOCAL_SERVER_ADDR: &str = "http://127.0.0.1:80";
    //
    // fn bridge_addr() -> MsgAddressInt {
    //     MsgAddressInt::from_str(
    //         "0:a3fb29fb5d681820eb8a45714101fccd6ff6e7e742f29549a0a87dbb505c50ba",
    //     )
    //     .unwrap()
    // }
    //

    // #[tokio::test]
    // async fn get_ethereum_events_configuration() {
    //     let transport: Arc<dyn Transport> = Arc::new(
    //         GraphQlTransport::new(ClientConfig {
    //             server_address: LOCAL_SERVER_ADDR.to_owned(),
    //             ..Default::default()
    //         })
    //         .await
    //         .unwrap(),
    //     );
    //
    //     let bridge = BridgeContract::new(&transport, &bridge_addr());
    //     let config = bridge.get_ethereum_events_configuration().await.unwrap();
    //     println!("Configs: {:?}", config);
    // }
}
