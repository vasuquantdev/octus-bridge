mod models;

use ton_abi::Contract;

pub use self::models::*;
use super::errors::*;
use super::prelude::*;
use crate::models::*;
use crate::prelude::*;
use crate::transport::*;

#[derive(Clone)]
pub struct BridgeContract {
    transport: Arc<dyn AccountSubscription>,
    keypair: Arc<Keypair>,
    config: ContractConfig,
    contract: Contract,
}

impl BridgeContract {
    pub async fn new(
        transport: Arc<dyn Transport>,
        account: &MsgAddressInt,
        keypair: Arc<Keypair>,
    ) -> ContractResult<BridgeContract> {
        let contract =
            Contract::load(Cursor::new(ABI)).expect("Failed to load bridge contract ABI");

        let transport = transport.subscribe(&account.to_string()).await?;

        Ok(Self {
            transport,
            keypair,
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

                if tx
                    .send(BridgeContractEvent::ConfigurationChanged(configs))
                    .is_err()
                {
                    return;
                }
            }
        });

        rx
    }

    #[inline]
    fn message(&self, name: &str) -> ContractResult<MessageBuilder> {
        MessageBuilder::new(
            &self.config,
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
    ) -> ContractResult<()> {
        self.message("addEthereumEventConfiguration")?
            .arg(ethereum_event_abi)
            .arg(ethereum_address)
            .arg(event_proxy_address)
            .send()
            .await?
            .ignore_output()
    }

    pub async fn confirm_ethereum_event_configuration(
        &self,
        ethereum_event_configuration_id: UInt256,
    ) -> ContractResult<()> {
        self.message("confirmEthereumEventConfiguration")?
            .arg(ethereum_event_configuration_id)
            .send()
            .await?
            .ignore_output()
    }

    pub async fn confirm_event_instance(
        &self,
        ethereum_event_configuration_id: UInt256,
        ethereum_event_data: Cell,
    ) -> ContractResult<()> {
        self.message("confirmEventInstance")?
            .arg(ethereum_event_configuration_id)
            .arg(ethereum_event_data)
            .send()
            .await?
            .ignore_output()
    }

    pub async fn get_ethereum_events_configuration(
        &self,
    ) -> ContractResult<Vec<EthereumEventsConfiguration>> {
        self.message("getEthereumEventsConfiguration")?
            .run_local()
            .await?
            .parse_first()
    }

    pub async fn sign_ton_to_eth_event(
        &self,
        payload: Vec<u8>,
        eth_public_key: Vec<u8>,
        sign: Vec<u8>,
        signed_at: u32,
        event_root_address: MsgAddrStd,
    ) -> ContractResult<()> {
        self.message("signTonToEthEvent")?
            .arg(payload)
            .arg(eth_public_key)
            .arg(sign)
            .arg(BigUint::from(signed_at))
            .arg(event_root_address)
            .send()
            .await?
            .ignore_output()
    }

    pub async fn sign_eth_to_ton_event(
        &self,
        payload: Cell,
        eth_event_address: MsgAddrStd,
    ) -> ContractResult<()> {
        self.message("signEthToTonEvent")?
            .arg(payload)
            .arg(eth_event_address)
            .send()
            .await?
            .ignore_output()
    }

    pub async fn get_current_config(&self) -> ContractResult<BridgeConfiguration> {
        self.message("getCurrentConfig")?
            .run_local()
            .await?
            .parse_all()
    }

    pub async fn get_event_roots(&self) -> ContractResult<Vec<MsgAddrStd>> {
        self.message("getEventRoots")?
            .run_local()
            .await?
            .parse_first()
    }

    pub async fn get_event_config(
        &self,
        event_root_address: MsgAddrStd,
    ) -> ContractResult<(MsgAddrStd, TonEventConfiguration)> {
        self.message("getEventConfig")?
            .arg(event_root_address)
            .run_local()
            .await?
            .parse_all()
    }

    pub async fn start_voting_for_update_config(
        &self,
        new_config: BridgeConfiguration,
    ) -> ContractResult<MsgAddrStd> {
        self.message("startVotingForUpdateConfig")?
            .arg(new_config.add_event_type_required_confirmations_percent)
            .arg(new_config.remove_event_type_required_confirmations_percent)
            .arg(new_config.add_relay_required_confirmations_percent)
            .arg(new_config.remove_relay_required_confirmations_percent)
            .arg(new_config.update_config_required_confirmations_percent)
            .arg(new_config.event_root_code)
            .arg(new_config.ton_to_eth_event_code)
            .arg(new_config.eth_to_ton_event_code)
            .send()
            .await?
            .parse_first()
    }

    pub async fn vote_for_update_config(
        &self,
        voting_address: MsgAddrStd,
        high_part: UInt256,
        low_part: UInt256,
    ) -> ContractResult<()> {
        self.message("voteForUpdateConfig")?
            .arg(voting_address)
            .arg(high_part)
            .arg(low_part)
            .send()
            .await?
            .ignore_output()
    }

    pub async fn update_config(
        &self,
        new_config: BridgeConfiguration,
        voting_set: OffchainVotingSet,
    ) -> ContractResult<()> {
        self.message("updateConfig")?
            .arg(new_config.add_event_type_required_confirmations_percent)
            .arg(new_config.remove_event_type_required_confirmations_percent)
            .arg(new_config.add_relay_required_confirmations_percent)
            .arg(new_config.remove_relay_required_confirmations_percent)
            .arg(new_config.update_config_required_confirmations_percent)
            .arg(new_config.event_root_code)
            .arg(new_config.ton_to_eth_event_code)
            .arg(new_config.eth_to_ton_event_code)
            .arg(voting_set.change_nonce)
            .arg(voting_set.signers)
            .arg(voting_set.signatures_high_parts)
            .arg(voting_set.signatures_low_parts)
            .send()
            .await?
            .ignore_output()
    }

    pub async fn start_voting_for_add_event_type(
        &self,
        new_event_type: TonEventConfiguration,
    ) -> ContractResult<MsgAddrStd> {
        self.message("startVotingForAddEventType")?
            .arg(new_event_type.eth_address)
            .arg(new_event_type.eth_event_abi)
            .arg(new_event_type.event_proxy_address)
            .arg(new_event_type.min_signs)
            .arg(new_event_type.min_signs_percent)
            .arg(new_event_type.ton_to_eth_rate)
            .arg(new_event_type.eth_to_ton_rate)
            .send()
            .await?
            .parse_first()
    }

    pub async fn vote_for_event_type(
        &self,
        voting_address: MsgAddrStd,
        high_part: UInt256,
        low_part: UInt256,
    ) -> ContractResult<()> {
        self.message("voteForAddEventType")?
            .arg(voting_address)
            .arg(high_part)
            .arg(low_part)
            .send()
            .await?
            .ignore_output()
    }

    pub async fn add_event_type(
        &self,
        new_event_type: TonEventConfiguration,
        voting_set: OffchainVotingSet,
    ) -> ContractResult<()> {
        self.message("addEventType")?
            .arg(new_event_type.eth_address)
            .arg(new_event_type.eth_event_abi)
            .arg(new_event_type.event_proxy_address)
            .arg(new_event_type.min_signs)
            .arg(new_event_type.min_signs_percent)
            .arg(new_event_type.ton_to_eth_rate)
            .arg(new_event_type.eth_to_ton_rate)
            .arg(voting_set.change_nonce)
            .arg(voting_set.signers)
            .arg(voting_set.signatures_high_parts)
            .arg(voting_set.signatures_low_parts)
            .send()
            .await?
            .ignore_output()
    }

    pub async fn start_voting_for_remove_event_type(
        &self,
        ton_address: MsgAddrStd,
    ) -> ContractResult<MsgAddrStd> {
        self.message("startVotingForRemoveEventType")?
            .arg(ton_address)
            .send()
            .await?
            .parse_first()
    }

    pub async fn vote_for_remove_event_type(
        &self,
        voting_address: MsgAddrStd,
        high_part: UInt256,
        low_part: UInt256,
    ) -> ContractResult<()> {
        self.message("startVotingForRemoveEventType")?
            .arg(voting_address)
            .arg(high_part)
            .arg(low_part)
            .send()
            .await?
            .ignore_output()
    }

    pub async fn remove_event_type(
        &self,
        ton_address: MsgAddrStd,
        voting_set: OffchainVotingSet,
    ) -> ContractResult<()> {
        self.message("removeEventType")?
            .arg(ton_address)
            .arg(voting_set.change_nonce)
            .arg(voting_set.signers)
            .arg(voting_set.signatures_high_parts)
            .arg(voting_set.signatures_low_parts)
            .send()
            .await?
            .ignore_output()
    }
}

const ABI: &str = include_str!("../../../abi/Bridge.abi.json");

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

    #[test]
    fn test() {}

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
