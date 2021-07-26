use nekoton_parser::abi::BigUint128;

use super::errors::*;
use super::message_builder::*;
use super::models::*;
use super::prelude::*;
use crate::models::*;
use crate::prelude::*;
use crate::transport::*;

use super::BridgeContract;

pub async fn make_relay_contract(
    transport: Arc<dyn Transport>,
    account: MsgAddrStd,
    keypair: Arc<Keypair>,
    bridge_contract: Arc<BridgeContract>,
) -> ContractResult<Arc<RelayContract>> {
    let subscription = transport
        .subscribe_without_events(MsgAddressInt::AddrStd(account.clone()))
        .await?;
    let config = ContractConfig {
        account: MsgAddressInt::AddrStd(account.clone()),
        timeout_sec: 60,
    };

    let contract = abi();

    Ok(Arc::new(RelayContract {
        transport,
        subscription,
        keypair,
        account,
        config,
        contract,
        bridge_contract,
    }))
}

pub struct RelayContract {
    transport: Arc<dyn Transport>,
    subscription: Arc<dyn AccountSubscription>,
    keypair: Arc<Keypair>,
    account: MsgAddrStd,
    config: ContractConfig,
    contract: Arc<AbiContract>,
    bridge_contract: Arc<BridgeContract>,
}

impl RelayContract {
    pub fn bridge(&self) -> &Arc<BridgeContract> {
        &self.bridge_contract
    }

    pub fn address(&self) -> &MsgAddrStd {
        &self.account
    }

    pub async fn initialize_event_configuration_creation(
        &self,
        id: u32,
        event_configuration: &MsgAddressInt,
        event_type: EventType,
    ) -> ContractResult<()> {
        self.send(
            self.message("initializeEventConfigurationCreation")?
                .arg(id)
                .arg(event_configuration)
                .arg(event_type),
        )
        .await
    }

    pub async fn vote_for_event_configuration_creation(
        &self,
        id: u32,
        voting: Voting,
    ) -> ContractResult<()> {
        self.send(
            self.message("voteForEventConfigurationCreation")?
                .arg(id)
                .arg(voting),
        )
        .await
    }

    pub async fn confirm_ethereum_event(&self, vote: EthEventVoteData) -> ContractResult<()> {
        log::info!(
            "CONFIRMING ETH EVENT: {:?}, {}, {}",
            hex::encode(&vote.event_transaction),
            vote.event_index,
            vote.event_block_number
        );

        let configuration_id = vote.configuration_id;

        self.send(
            self.message("confirmEthereumEvent")?
                .arg(vote)
                .arg(configuration_id),
        )
        .await
    }

    pub async fn reject_ethereum_event(&self, vote: EthEventVoteData) -> ContractResult<()> {
        log::info!(
            "REJECTING ETH EVENT: {:?}, {}, {}",
            hex::encode(&vote.event_transaction),
            vote.event_index,
            vote.event_block_number
        );

        let configuration_id = vote.configuration_id;

        self.send(
            self.message("rejectEthereumEvent")?
                .arg(vote)
                .arg(configuration_id),
        )
        .await
    }

    pub async fn confirm_ton_event(
        &self,
        vote: TonEventVoteData,
        event_data_signature: Vec<u8>,
    ) -> ContractResult<()> {
        log::info!(
            "CONFIRMING TON EVENT: {}, {}, {}",
            hex::encode(&vote.event_transaction),
            vote.event_transaction_lt,
            vote.event_index,
        );

        let configuration_id = vote.configuration_id;

        self.send(
            self.message("confirmTonEvent")?
                .arg(vote)
                .arg(event_data_signature)
                .arg(configuration_id),
        )
        .await
    }

    pub async fn reject_ton_event(&self, vote: TonEventVoteData) -> ContractResult<()> {
        log::info!(
            "REJECTING TON EVENT: {}, {}, {}",
            hex::encode(&vote.event_transaction),
            vote.event_transaction_lt,
            vote.event_index,
        );

        let configuration_id = vote.configuration_id;

        self.send(
            self.message("rejectTonEvent")?
                .arg(vote)
                .arg(configuration_id),
        )
        .await
    }

    pub async fn update_bridge_configuration(
        &self,
        configuration: BridgeConfiguration,
        vote: VoteData,
    ) -> ContractResult<()> {
        self.send(
            self.message("updateBridgeConfiguration")?
                .arg(configuration)
                .arg(vote),
        )
        .await
    }

    #[inline]
    async fn send(&self, message: MessageBuilder<'_>) -> ContractResult<()> {
        const ONE_TON: u64 = 1_000_000_000;
        const FLAGS: u8 = 3;

        SignedMessageBuilder::new(
            Cow::Borrowed(&self.config),
            &self.contract,
            self.subscription.as_ref(),
            self.keypair.as_ref(),
            "sendTransaction",
        )?
        .arg(self.bridge_contract.address())
        .arg(BigUint128(ONE_TON.into()))
        .arg(true)
        .arg(FLAGS)
        .arg(message.build_internal_body()?)
        .send()
        .await?
        .ignore_output()
    }

    #[inline]
    fn message(&self, name: &str) -> ContractResult<MessageBuilder> {
        MessageBuilder::new(
            Cow::Borrowed(&self.config),
            &self.bridge_contract.abi(),
            self.transport.as_ref(),
            name,
        )
    }
}

impl Contract for RelayContract {
    #[inline]
    fn abi(&self) -> &Arc<ton_abi::Contract> {
        &self.contract
    }
}

static ABI: OnceCell<Arc<AbiContract>> = OnceCell::new();
const JSON_ABI: &str = include_str!("../../../abi/Relay.abi.json");

pub fn abi() -> Arc<AbiContract> {
    ABI.get_or_init(|| {
        Arc::new(AbiContract::load(Cursor::new(JSON_ABI)).expect("failed to load RelayAccount ABI"))
    })
    .clone()
}
