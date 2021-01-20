mod eth_events_handler;
mod event_transport;
mod semaphore;
mod ton_events_handler;

use relay_ton::contracts::*;
use relay_ton::transport::*;

use super::utils;
use crate::config::TonSettings;
use crate::crypto::key_managment::*;
use crate::db::*;
use crate::models::*;
use crate::prelude::*;

use self::eth_events_handler::*;
use self::event_transport::*;
use self::semaphore::*;
use self::ton_events_handler::*;

/// Listens to config streams and maps them.
pub struct TonListener {
    transport: Arc<dyn Transport>,
    relay_contract: Arc<RelayContract>,

    eth_signer: EthSigner,
    eth_queue: EthQueue,

    ton: Arc<EventTransport<TonEventConfigurationContract>>,
    eth: Arc<EventTransport<EthEventConfigurationContract>>,

    scanning_state: ScanningState,

    configs_state: Arc<RwLock<ConfigsState>>,
}

pub async fn make_ton_listener(
    db: &Db,
    transport: Arc<dyn Transport>,
    relay_contract: Arc<RelayContract>,
    eth_queue: EthQueue,
    eth_signer: EthSigner,
    settings: TonSettings,
) -> Result<Arc<TonListener>, Error> {
    let scanning_state = ScanningState::new(db)?;

    let ton = Arc::new(
        EventTransport::new(
            db,
            transport.clone(),
            scanning_state.clone(),
            relay_contract.clone(),
            settings.clone(),
        )
        .await?,
    );
    let eth = Arc::new(
        EventTransport::new(
            db,
            transport.clone(),
            scanning_state.clone(),
            relay_contract.clone(),
            settings.clone(),
        )
        .await?,
    );

    Ok(Arc::new(TonListener {
        transport,
        relay_contract,
        eth_signer,
        eth_queue,
        ton,
        eth,
        scanning_state,
        configs_state: Arc::new(Default::default()),
    }))
}

impl TonListener {
    /// Spawn configuration contracts listeners
    pub async fn start<T>(
        self: &Arc<Self>,
        mut bridge_contract_events: T,
    ) -> impl Stream<Item = (Address, H256)>
    where
        T: Stream<Item = BridgeContractEvent> + Send + Unpin + 'static,
    {
        let (subscriptions_tx, subscriptions_rx) = mpsc::unbounded_channel();

        // Subscribe to bridge events
        tokio::spawn({
            let listener = self.clone();
            let subscriptions_tx = subscriptions_tx.clone();

            async move {
                while let Some(event) = bridge_contract_events.next().await {
                    match event {
                        BridgeContractEvent::EventConfigurationCreationEnd {
                            id,
                            address,
                            active: true,
                            event_type: EventType::ETH,
                        } => {
                            tokio::spawn(listener.clone().subscribe_to_eth_events_configuration(
                                subscriptions_tx.clone(),
                                id,
                                address,
                                None,
                            ));
                        }
                        BridgeContractEvent::EventConfigurationCreationEnd {
                            id,
                            address,
                            active: true,
                            event_type: EventType::TON,
                        } => {
                            tokio::spawn(
                                listener
                                    .clone()
                                    .subscribe_to_ton_events_configuration(id, address),
                            );
                        }
                        _ => {
                            // TODO: handle other events
                        }
                    }
                }
            }
        });

        // Get all configs before now
        let known_contracts = self
            .relay_contract
            .bridge()
            .get_active_event_configurations()
            .await
            .expect("Failed to get known event configurations"); // TODO: is it really a fatal error?

        // Wait for all existing configuration contracts subscriptions to start ETH part properly
        let semaphore = Semaphore::new(known_contracts.len());
        for active_configuration in known_contracts.into_iter() {
            match active_configuration.event_type {
                EventType::ETH => {
                    tokio::spawn(self.clone().subscribe_to_eth_events_configuration(
                        subscriptions_tx.clone(),
                        active_configuration.id,
                        active_configuration.address,
                        Some(semaphore.clone()),
                    ));
                }
                EventType::TON => {
                    tokio::spawn(self.clone().subscribe_to_ton_events_configuration(
                        active_configuration.id,
                        active_configuration.address,
                    ));
                    semaphore.notify().await;
                }
            }
        }

        // Wait until all initial subscriptions done
        semaphore.wait().await;

        // Restart sending for all enqueued confirmations
        self.eth.retry_pending();

        // Return subscriptions stream
        subscriptions_rx
    }

    /// Adds ETH transaction to queue, starts reliable sending
    pub async fn enqueue_vote(self: &Arc<Self>, data: EthEventTransaction) -> Result<(), Error> {
        self.eth.enqueue_vote(data).await
    }

    pub fn retry_failed(&self) {
        self.eth.retry_failed();
        self.ton.retry_failed();
    }

    /// Current configs state
    pub async fn get_state(&self) -> RwLockReadGuard<'_, ConfigsState> {
        self.configs_state.read().await
    }

    /// Creates a listener for TON event votes in TON and its target contract
    async fn subscribe_to_ton_events_configuration(
        self: Arc<Self>,
        configuration_id: BigUint,
        address: MsgAddressInt,
    ) {
        let _handler = match TonEventsHandler::new(
            self.ton.clone(),
            self.eth_signer.clone(),
            configuration_id,
            address,
        )
        .await
        {
            Ok(handler) => handler,
            Err(e) => {
                log::error!("Failed to subscribe to TON events configuration: {:?}", e);
                return;
            }
        };

        // TODO: insert to map
        future::pending().await
    }

    /// Creates a listener for ETH event votes in TON
    async fn subscribe_to_eth_events_configuration(
        self: Arc<Self>,
        subscriptions_tx: mpsc::UnboundedSender<(Address, H256)>,
        configuration_id: BigUint,
        address: MsgAddressInt,
        semaphore: Option<Semaphore>,
    ) {
        let handler = match EthEventsHandler::uninit(
            self.eth.clone(),
            self.eth_queue.clone(),
            configuration_id.clone(),
            address,
        )
        .await
        {
            Ok(handler) => handler,
            Err(e) => {
                log::error!("Failed to subscribe to ETH events configuration: {:?}", e);
                return;
            }
        };

        // Insert configuration
        self.configs_state
            .write()
            .await
            .set_configuration(configuration_id, handler.details());

        // Start listening ETH events
        self.notify_subscription(subscriptions_tx, handler.details().event_address)
            .await;

        let _handler = handler.start().await;

        semaphore.try_notify().await;

        // TODO: insert to map
        future::pending().await
    }

    async fn notify_subscription(
        &self,
        subscriptions_tx: mpsc::UnboundedSender<(Address, H256)>,
        ethereum_event_address: Address,
    ) {
        let topics = self.get_state().await;
        if let Some((topic, _, _)) = topics.address_topic_map.get(&ethereum_event_address) {
            let _ = subscriptions_tx.send((ethereum_event_address, *topic));
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct ConfigsState {
    pub eth_addr: HashSet<Address>,
    pub address_topic_map: HashMap<Address, EthTopicItem>,
    pub topic_abi_map: HashMap<H256, Vec<ethabi::ParamType>>,
    pub eth_configs_map: HashMap<Address, (BigUint, EthEventConfiguration)>,
}

type EthTopicItem = (H256, Vec<ethabi::ParamType>, Vec<ton_abi::ParamType>);

impl ConfigsState {
    fn set_configuration(
        &mut self,
        configuration_id: BigUint,
        configuration: &EthEventConfiguration,
    ) {
        let (topic_hash, eth_abi, ton_abi) =
            match utils::parse_eth_abi(&configuration.common.event_abi) {
                Ok(a) => a,
                Err(e) => {
                    log::error!("Failed parsing abi: {:?}", e);
                    return;
                }
            };

        self.eth_addr.insert(configuration.event_address.clone());
        self.address_topic_map.insert(
            configuration.event_address.clone(),
            (topic_hash, eth_abi.clone(), ton_abi),
        );
        self.topic_abi_map.insert(topic_hash, eth_abi);
        self.eth_configs_map.insert(
            configuration.event_address,
            (configuration_id, configuration.clone()),
        );
    }

    fn remove_event_address(&mut self, event_address: &Address) {
        self.eth_addr.remove(event_address);
        self.address_topic_map.remove(event_address);
        self.eth_configs_map.remove(event_address);
    }
}

#[async_trait]
impl ConfigurationContract for EthEventConfigurationContract {
    type Details = EthEventConfiguration;
    type EventContract = EthEventContract;
    type ReceivedVote = EthEventReceivedVote;
    type EventTransaction = EthEventTransaction;

    async fn make_config_contract(
        transport: Arc<dyn Transport>,
        account: MsgAddressInt,
        bridge_address: MsgAddressInt,
    ) -> ContractResult<(Arc<Self>, EventsRx<<Self as ContractWithEvents>::Event>)> {
        make_eth_event_configuration_contract(transport, account, bridge_address).await
    }

    async fn make_event_contract(transport: Arc<dyn Transport>) -> Arc<Self::EventContract> {
        make_eth_event_contract(transport).await
    }

    fn make_voting_stats(
        db: &Db,
    ) -> Result<VotingStats<<Self::ReceivedVote as ReceivedVote>::VoteWithData>, Error> {
        EthVotingStats::new(db)
    }

    fn make_votes_queue(db: &Db) -> Result<VotesQueue<Self::EventTransaction>, Error> {
        EthEventVotesQueue::new(db)
    }

    fn address(&self) -> &MsgAddressInt {
        self.address()
    }

    fn validate(&self, details: &Self::Details) -> Result<(), Error> {
        utils::validate_ethereum_event_configuration(details)
    }

    async fn compute_event_address(
        &self,
        init_data: <Self::EventContract as EventContract>::InitData,
    ) -> ContractResult<MsgAddrStd> {
        self.compute_event_address(init_data).await
    }

    async fn get_details(&self) -> ContractResult<Self::Details> {
        self.get_details().await
    }
}

#[async_trait]
impl ConfigurationContract for TonEventConfigurationContract {
    type Details = TonEventConfiguration;
    type EventContract = TonEventContract;
    type ReceivedVote = TonEventReceivedVote;
    type EventTransaction = TonEventTransaction;

    async fn make_config_contract(
        transport: Arc<dyn Transport>,
        account: MsgAddressInt,
        bridge_address: MsgAddressInt,
    ) -> ContractResult<(Arc<Self>, EventsRx<<Self as ContractWithEvents>::Event>)> {
        make_ton_event_configuration_contract(transport, account, bridge_address).await
    }

    async fn make_event_contract(transport: Arc<dyn Transport>) -> Arc<Self::EventContract> {
        make_ton_event_contract(transport).await
    }

    fn make_voting_stats(
        db: &Db,
    ) -> Result<VotingStats<<Self::ReceivedVote as ReceivedVote>::VoteWithData>, Error> {
        TonVotingStats::new(db)
    }

    fn make_votes_queue(db: &Db) -> Result<VotesQueue<Self::EventTransaction>, Error> {
        TonEventVotesQueue::new(db)
    }

    fn address(&self) -> &MsgAddressInt {
        self.address()
    }

    fn validate(&self, config: &Self::Details) -> Result<(), Error> {
        let _ = serde_json::from_str::<SwapBackEventAbi>(&config.common.event_abi)
            .map_err(|e| Error::new(e).context("Bad SwapBack event ABI"))?;
        Ok(())
    }

    async fn compute_event_address(
        &self,
        init_data: <Self::EventContract as EventContract>::InitData,
    ) -> ContractResult<MsgAddrStd> {
        self.compute_event_address(init_data).await
    }

    async fn get_details(&self) -> ContractResult<Self::Details> {
        self.get_details().await
    }
}

#[async_trait]
impl EventContract for EthEventContract {
    type Details = EthEventDetails;
    type InitData = EthEventInitData;

    async fn get_details(&self, address: &MsgAddrStd) -> ContractResult<Self::Details> {
        self.get_details(address.clone()).await
    }
}

#[async_trait]
impl EventContract for TonEventContract {
    type Details = TonEventDetails;
    type InitData = TonEventInitData;

    async fn get_details(&self, address: &MsgAddrStd) -> ContractResult<Self::Details> {
        self.get_details(address.clone()).await
    }
}

fn make_confirmed_ton_event_transaction(
    event_id: u32,
    configuration_id: BigUint,
    event: SwapBackEvent,
    signature: Vec<u8>,
) -> Result<TonEventTransaction, Error> {
    let event_data = utils::pack_event_data_into_cell(event_id, &event.tokens)?;

    Ok(TonEventTransaction::Confirm(SignedEventVotingData {
        data: TonEventVotingData {
            event_transaction: event.event_transaction,
            event_transaction_lt: event.event_transaction_lt,
            event_index: event.event_index,
            event_data,
            configuration_id,
        },
        signature,
    }))
}

#[async_trait]
impl EventTransactionExt for EthEventTransaction {
    type InitData = <EthEventContract as EventContract>::InitData;

    #[inline]
    fn configuration_id(&self) -> &BigUint {
        match self {
            Self::Confirm(data) => &data.configuration_id,
            Self::Reject(data) => &data.configuration_id,
        }
    }

    #[inline]
    fn kind(&self) -> Voting {
        match self {
            Self::Confirm(_) => Voting::Confirm,
            Self::Reject(_) => Voting::Reject,
        }
    }

    fn init_data(&self) -> Self::InitData {
        match self.clone() {
            Self::Confirm(data) => data.into(),
            Self::Reject(data) => data.into(),
        }
    }

    async fn send(&self, bridge: Arc<RelayContract>) -> ContractResult<()> {
        let id = self.configuration_id().clone();
        let data = self.init_data();
        match self.kind() {
            Voting::Confirm => bridge.confirm_ethereum_event(id, data).await,
            Voting::Reject => bridge.reject_ethereum_event(id, data).await,
        }
    }
}

impl std::fmt::Display for EthEventTransaction {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let transaction = match self {
            Self::Confirm(data) => &data.event_transaction,
            Self::Reject(data) => &data.event_transaction,
        };
        f.write_fmt(format_args!("Vote for ETH transaction {}", transaction))
    }
}

#[async_trait]
impl EventTransactionExt for TonEventTransaction {
    type InitData = <TonEventContract as EventContract>::InitData;

    #[inline]
    fn configuration_id(&self) -> &BigUint {
        match self {
            Self::Confirm(signed) => &signed.data.configuration_id,
            Self::Reject(data) => &data.configuration_id,
        }
    }

    #[inline]
    fn kind(&self) -> Voting {
        match self {
            Self::Confirm(_) => Voting::Confirm,
            Self::Reject(_) => Voting::Reject,
        }
    }

    fn init_data(&self) -> Self::InitData {
        match self {
            Self::Confirm(signed) => signed.data.clone().into(),
            Self::Reject(data) => data.clone().into(),
        }
    }

    async fn send(&self, bridge: Arc<RelayContract>) -> ContractResult<()> {
        match self.clone() {
            Self::Confirm(SignedEventVotingData { data, signature }) => {
                let id = data.configuration_id.clone();
                let data = data.into();
                bridge.confirm_ton_event(id, data, signature).await
            }
            Self::Reject(data) => {
                let id = data.configuration_id.clone();
                let data = data.into();
                bridge.reject_ton_event(id, data).await
            }
        }
    }
}

impl std::fmt::Display for TonEventTransaction {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let transaction = match self {
            Self::Confirm(signed) => &signed.data.event_transaction,
            Self::Reject(data) => &data.event_transaction,
        };
        f.write_fmt(format_args!(
            "Vote for TON transaction {}",
            hex::encode(transaction.as_slice())
        ))
    }
}
