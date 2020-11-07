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
    pub fn new(transport: &Arc<dyn Transport>, addr: &AccountId) -> BridgeContract {
        let contract =
            Contract::load(Cursor::new(ABI)).expect("Failed to load bridge contract ABI");
        let account = MsgAddressInt::AddrStd(addr.clone().into());

        Self {
            transport: transport.clone(),
            config: ContractConfig {
                account,
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
        .build()?
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
        .build()?
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
            .build()?
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
        .build()?;

        todo!()
    }
}

pub struct EthereumEventsConfiguration {
    pub ethereum_event_abi: String,
    pub ethereum_address: String,
    pub event_proxy_address: String,
    pub confirmations: u64,
    pub confirmed: bool,
}

const ABI: &str = include_str!("../../abi/Bridge.abi.json");
