use std::io::Cursor;

use ton_abi::{Contract, Token, TokenValue};

use super::errors::*;
use super::prelude::*;
use crate::models::*;
use crate::prelude::*;
use crate::transport::*;

pub struct BridgeContract {
    transport: Arc<dyn AccountSubscription>,
    config: ContractConfig,
    contract: Contract,
}

impl BridgeContract {
    pub async fn new(
        transport: &Arc<dyn Transport>,
        account: &MsgAddressInt,
    ) -> ContractResult<BridgeContract> {
        let contract =
            Contract::load(Cursor::new(ABI)).expect("Failed to load bridge contract ABI");

        let transport = transport.subscribe(&account.to_string()).await?;

        Ok(Self {
            transport,
            config: ContractConfig {
                account: account.clone(),
                timeout_sec: 60,
            },
            contract,
        })
    }

    pub fn events(self: &Arc<Self>) -> impl Stream<Item = BridgeContractEvent> {
        let mut events = self.transport.events();
        let this = Arc::downgrade(self);
        let (tx, rx) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            while let Some(AccountEvent::StateChanged) = events.recv().await {
                let this = match this.upgrade() {
                    Some(this) => this,
                    _ => return,
                };

                let configs = match this.get_ethereum_events_configuration().await {
                    Ok(configs) => configs,
                    Err(e) => {
                        log::error!("failed to get ethereum events configuration. {}", e);
                        continue;
                    }
                };

                if let Err(_) = tx.send(BridgeContractEvent::ConfigurationChanged(configs)) {
                    return;
                }
            }
        });

        rx
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
        MessageBuilder::empty(
            &self.config,
            &self.contract,
            "getEthereumEventsConfiguration",
        )?
        .run_local()
        .send(self.transport.as_ref())
        .await?
        .try_into()
    }
}

#[derive(Debug, Clone)]
pub enum BridgeContractEvent {
    ConfigurationChanged(Vec<EthereumEventsConfiguration>),
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct EthereumEventsConfiguration {
    pub ethereum_event_abi: String,
    pub ethereum_address: Vec<u8>,
    pub event_proxy_address: MsgAddrStd,
    pub confirmations: BigUint,
    pub confirmed: bool,
}

impl TryFrom<ContractOutput> for Vec<EthereumEventsConfiguration> {
    type Error = ContractError;

    fn try_from(mut value: ContractOutput) -> Result<Self, Self::Error> {
        let tokens = match value.tokens.into_iter().next() {
            Some(array) => match array.value {
                TokenValue::Array(tokens) => tokens,
                _ => return Err(ContractError::InvalidAbi),
            },
            None => return Err(ContractError::InvalidAbi),
        };

        let mut configs = Vec::with_capacity(tokens.len());
        for token in tokens {
            let mut tuple = match token {
                TokenValue::Tuple(tuple) => tuple,
                _ => return Err(ContractError::InvalidAbi),
            }
            .into_iter();

            let ethereum_event_abi = match tuple.next() {
                Some(Token {
                    value: TokenValue::Bytes(bytes),
                    ..
                }) => bytes,
                _ => return Err(ContractError::InvalidAbi),
            };

            let ethereum_address = match tuple.next() {
                Some(Token {
                    value: TokenValue::Bytes(bytes),
                    ..
                }) => bytes,
                _ => return Err(ContractError::InvalidAbi),
            };

            let event_proxy_address = match tuple.next() {
                Some(Token {
                    value: TokenValue::Address(ton_block::MsgAddress::AddrStd(address)),
                    ..
                }) => address,
                _ => return Err(ContractError::InvalidAbi),
            };

            let confirmations = match tuple.next() {
                Some(Token {
                    value: TokenValue::Uint(confirmations),
                    ..
                }) => confirmations,
                _ => return Err(ContractError::InvalidAbi),
            };

            let confirmed = match tuple.next() {
                Some(Token {
                    value: TokenValue::Bool(confirmed),
                    ..
                }) => confirmed,
                _ => return Err(ContractError::InvalidAbi),
            };

            configs.push(EthereumEventsConfiguration {
                ethereum_event_abi: String::from_utf8(ethereum_event_abi)
                    .map_err(|_| ContractError::InvalidString)?,
                ethereum_address,
                event_proxy_address,
                confirmations: confirmations.number,
                confirmed,
            });
        }

        Ok(configs)
    }
}

const ABI: &str = include_str!("../../abi/Bridge.abi.json");

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::tonlib_transport::config::Config;
    use crate::transport::TonlibTransport;

    const LOCAL_SERVER_ADDR: &str = "http://127.0.0.1:80";

    fn bridge_addr() -> MsgAddressInt {
        MsgAddressInt::from_str(
            "0:a3fb29fb5d681820eb8a45714101fccd6ff6e7e742f29549a0a87dbb505c50ba",
        )
        .unwrap()
    }

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
