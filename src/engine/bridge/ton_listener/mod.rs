mod event_transport;
mod semaphore;

use relay_ton::contracts::*;
use relay_ton::transport::*;

use super::utils;
use crate::config::TonSettings;
use crate::crypto::key_managment::*;
use crate::db::*;
use crate::models::*;
use crate::prelude::*;

use self::event_transport::*;
use self::semaphore::*;

/// Listens to config streams and maps them.
pub struct TonListener {
    transport: Arc<dyn Transport>,
    bridge_contract: Arc<BridgeContract>,

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
    bridge_contract: Arc<BridgeContract>,
    eth_queue: EthQueue,
    eth_signer: EthSigner,
    settings: TonSettings,
) -> Result<Arc<TonListener>, Error> {
    let scanning_state = ScanningState::new(db)?;

    let ton = Arc::new(
        EventTransport::new(
            db,
            transport.clone(),
            bridge_contract.clone(),
            settings.clone(),
        )
        .await?,
    );
    let eth = Arc::new(
        EventTransport::new(
            db,
            transport.clone(),
            bridge_contract.clone(),
            settings.clone(),
        )
        .await?,
    );

    Ok(Arc::new(TonListener {
        transport,
        bridge_contract,
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
                        _ => {
                            // TODO: handle other events
                        }
                    }
                }
            }
        });

        // Get all configs before now
        let known_contracts = self
            .bridge_contract
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
        if !self.ton.ensure_configuration_identity(&address).await {
            return;
        }

        // Create TON config contract
        let (config_contract, mut config_contract_events) = make_ton_event_configuration_contract(
            self.transport.clone(),
            address.clone(),
            self.bridge_contract.address().clone(),
        )
        .await
        .unwrap(); // todo retry subscription

        // Get its data
        let details = match self
            .ton
            .get_event_configuration_details(config_contract.as_ref())
            .await
        {
            Ok(details) => details,
            Err(e) => {
                log::error!("{:?}", e);
                self.ton
                    .forget_configuration(config_contract.address())
                    .await;
                return;
            }
        };

        // Register contract
        self.ton
            .add_configuration_contract(configuration_id.clone(), config_contract.clone())
            .await;

        // Start listening swapback events
        let mut swapback_events_contract = match make_ton_swapback_contract(
            self.transport.clone(),
            details.event_address.clone(),
            details.common.event_abi.clone(),
        )
        .await
        {
            Ok(contract) => contract,
            Err(e) => {
                log::error!("{:?}", e);
                return;
            }
        };

        let abi = swapback_events_contract.abi().clone();
        let event_id = abi.id;

        // Start listening swapback events
        tokio::spawn({
            let listener = self.clone();
            let configuration_id = configuration_id.clone();

            async move {
                while let Some(event) = swapback_events_contract.next().await {
                    log::info!("Got swap back event: {:?}", event);

                    let signature = match listener.calculate_signature(&event) {
                        Ok(signature) => signature,
                        Err(e) => {
                            log::error!("Failed to calculate event signature: {:?}", e);
                            continue;
                        }
                    };

                    match make_confirmed_ton_event_transaction(
                        event_id,
                        configuration_id.clone(),
                        event,
                        signature,
                    ) {
                        Ok(data) => {
                            if let Err(e) = listener.ton.enqueue_vote(data).await {
                                log::error!("Failed to enqueue swap back event: {:?}", e);
                            }
                        }
                        Err(e) => {
                            log::error!("Failed to prepare swap back event: {:?}", e);
                        }
                    }
                }
            }
        });

        let handle_voting_event =
            move |listener: Arc<Self>,
                  configuration_id,
                  abi,
                  event,
                  semaphore: Option<Semaphore>| {
                log::debug!("got TON->ETH event vote: {:?}", event);

                let (event_address, relay_key, vote) = match event {
                    TonEventConfigurationContractEvent::EventConfirmation {
                        address,
                        relay_key,
                    } => (address, relay_key, Voting::Confirm),
                    TonEventConfigurationContractEvent::EventReject { address, relay_key } => {
                        (address, relay_key, Voting::Reject)
                    }
                };

                tokio::spawn({
                    async move {
                        listener
                            .ton
                            .handle_event(
                                listener.as_ref(),
                                TonEventReceivedVote::new(
                                    configuration_id,
                                    event_address,
                                    relay_key,
                                    vote,
                                    abi,
                                ),
                            )
                            .await;
                        if let Some(semaphore) = semaphore {
                            semaphore.notify().await;
                        }
                    }
                });
            };

        // Spawn listener of new voting events
        tokio::spawn({
            let listener = self.clone();
            let configuration_id = configuration_id.clone();
            let abi = abi.clone();

            async move {
                while let Some(event) = config_contract_events.next().await {
                    handle_voting_event(
                        listener.clone(),
                        configuration_id.clone(),
                        abi.clone(),
                        event,
                        None,
                    );
                }
            }
        });

        // Process all past events
        let latest_known_lt = config_contract.since_lt();

        let latest_scanned_lt = self
            .scanning_state
            .get_latest_scanned_lt(config_contract.address())
            .expect("Fatal db error");

        let events_semaphore = Semaphore::new_empty();

        let mut known_events = config_contract.get_known_events(latest_scanned_lt, latest_known_lt);
        let mut known_event_count = 0;
        while let Some(event) = known_events.next().await {
            handle_voting_event(
                self.clone(),
                configuration_id.clone(),
                abi.clone(),
                event,
                Some(events_semaphore.clone()),
            );
            known_event_count += 1;
        }

        // Only update latest scanned lt when all scanned events are in ton queue
        events_semaphore.wait_count(known_event_count).await;

        self.scanning_state
            .update_latest_scanned_lt(config_contract.address(), latest_known_lt)
            .expect("Fatal db error");
    }

    /// Creates a listener for ETH event votes in TON
    async fn subscribe_to_eth_events_configuration(
        self: Arc<Self>,
        subscriptions_tx: mpsc::UnboundedSender<(Address, H256)>,
        configuration_id: BigUint,
        address: MsgAddressInt,
        semaphore: Option<Semaphore>,
    ) {
        if !self.eth.ensure_configuration_identity(&address).await {
            return semaphore.try_notify().await;
        }

        // Create ETH config contract
        let (config_contract, mut config_contract_events) = make_eth_event_configuration_contract(
            self.transport.clone(),
            address.clone(),
            self.bridge_contract.address().clone(),
        )
        .await
        .unwrap(); //todo retry subscription

        // Get its data
        let details = match self
            .eth
            .get_event_configuration_details(config_contract.as_ref())
            .await
        {
            Ok(details) => details,
            Err(e) => {
                log::error!("{:?}", e);
                self.eth
                    .forget_configuration(config_contract.address())
                    .await;
                return semaphore.try_notify().await;
            }
        };

        // Register contract object
        self.eth
            .add_configuration_contract(configuration_id.clone(), config_contract.clone())
            .await;
        semaphore.try_notify().await;

        // Prefetch required properties
        let eth_blocks_to_confirm = details.event_blocks_to_confirm;
        let ethereum_event_address = details.event_address;

        // Store configuration contract details
        self.configs_state
            .write()
            .await
            .set_configuration(configuration_id.clone(), details);

        // Start listening ETH events
        self.notify_subscription(subscriptions_tx, ethereum_event_address)
            .await;

        let handle_event =
            move |listener: Arc<Self>, configuration_id, event, semaphore: Option<Semaphore>| {
                log::debug!("got event confirmation event: {:?}", event);

                let (event_address, relay_key, vote) = match event {
                    EthEventConfigurationContractEvent::EventConfirmation {
                        address,
                        relay_key,
                    } => (address, relay_key, Voting::Confirm),
                    EthEventConfigurationContractEvent::EventReject { address, relay_key } => {
                        (address, relay_key, Voting::Reject)
                    }
                };

                tokio::spawn({
                    async move {
                        listener
                            .eth
                            .handle_event(
                                listener.as_ref(),
                                EthEventReceivedVote::new(
                                    configuration_id,
                                    event_address,
                                    relay_key,
                                    vote,
                                    eth_blocks_to_confirm,
                                ),
                            )
                            .await;
                        if let Some(semaphore) = semaphore {
                            semaphore.notify().await;
                        }
                    }
                });
            };

        // Spawn listener of new events
        tokio::spawn({
            let listener = self.clone();
            let configuration_id = configuration_id.clone();

            async move {
                while let Some(event) = config_contract_events.next().await {
                    handle_event(listener.clone(), configuration_id.clone(), event, None);
                }
            }
        });

        // Process all past events
        let latest_known_lt = config_contract.since_lt();

        let latest_scanned_lt = self
            .scanning_state
            .get_latest_scanned_lt(config_contract.address())
            .expect("Fatal db error");

        let events_semaphore = Semaphore::new_empty();

        let mut known_events = config_contract.get_known_events(latest_scanned_lt, latest_known_lt);
        let mut known_event_count = 0;
        while let Some(event) = known_events.next().await {
            handle_event(
                self.clone(),
                configuration_id.clone(),
                event,
                Some(events_semaphore.clone()),
            );
            known_event_count += 1;
        }

        // Only update latest scanned lt when all scanned events are in ton queue
        events_semaphore.wait_count(known_event_count).await;

        self.scanning_state
            .update_latest_scanned_lt(config_contract.address(), latest_known_lt)
            .expect("Fatal db error");
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

    fn calculate_signature(&self, event: &SwapBackEvent) -> Result<Vec<u8>, Error> {
        let payload = utils::prepare_ton_event_payload(event)?;
        let signature = self.eth_signer.sign(&payload);

        log::info!(
            "Calculated swap back event signature: {} for payload: {}",
            hex::encode(&signature),
            hex::encode(&payload)
        );
        Ok(signature)
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
        configuration: EthEventConfiguration,
    ) {
        let (topic_hash, eth_abi, ton_abi) =
            match utils::parse_eth_abi(&configuration.common.event_abi) {
                Ok(a) => a,
                Err(e) => {
                    log::error!("Failed parsing abi: {:?}", e);
                    return;
                }
            };

        self.eth_addr.insert(configuration.event_address);
        self.address_topic_map.insert(
            configuration.event_address,
            (topic_hash, eth_abi.clone(), ton_abi),
        );
        self.topic_abi_map.insert(topic_hash, eth_abi);
        self.eth_configs_map.insert(
            configuration.event_address,
            (configuration_id, configuration),
        );
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

#[async_trait]
impl VerificationQueue<EthEventReceivedVote> for TonListener {
    async fn enqueue(&self, event: <EthEventReceivedVote as ReceivedVote>::VoteWithData) {
        let info = event.info();

        let target_block_number = event
            .data()
            .init_data
            .event_block_number
            .to_u64()
            .unwrap_or_else(u64::max_value)
            + *info.additional() as u64;

        if let Err(e) = self
            .eth_queue
            .insert(target_block_number, &event.into())
            .await
        {
            log::error!("Failed to insert event confirmation. {:?}", e);
        }
    }
}

#[async_trait]
impl VerificationQueue<TonEventReceivedVote> for TonListener {
    async fn enqueue(&self, event: <TonEventReceivedVote as ReceivedVote>::VoteWithData) {
        async fn verify(
            listener: &TonListener,
            transaction: TonEventReceivedVoteWithData,
        ) -> Result<(), Error> {
            let abi: &Arc<AbiEvent> = transaction.info().additional();

            let tokens = abi
                .decode_input(transaction.data().init_data.event_data.clone().into())
                .map_err(|e| anyhow!("failed decoding TON event data: {:?}", e))?;

            let data = &transaction.data().init_data;
            let signature = listener.calculate_signature(&SwapBackEvent {
                event_transaction: data.event_transaction.clone(),
                event_transaction_lt: data.event_transaction_lt,
                event_index: data.event_index,
                tokens,
            })?;

            listener
                .ton
                .enqueue_vote(TonEventTransaction::Confirm(SignedEventVotingData {
                    data: transaction.into(),
                    signature,
                }))
                .await
        }

        if let Err(e) = verify(self, event).await {
            log::error!("Failed to enqueue TON event: {:?}", e);
        }
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

impl std::fmt::Display for DisplayReceivedVote<'_, EthEventReceivedVoteWithData> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let info = self.inner().info();
        let data = self.inner().data();
        f.write_fmt(format_args!(
            "ETH->TON event {:?} tx {} (block {}) from {}. status: {:?}. address: {}",
            info.kind(),
            hex::encode(&data.init_data.event_transaction),
            data.init_data.event_block_number,
            hex::encode(&info.relay_key()),
            data.status,
            info.event_address()
        ))
    }
}

impl std::fmt::Display for DisplayReceivedVote<'_, TonEventReceivedVoteWithData> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let info = self.inner().info();
        let data = self.inner().data();
        f.write_fmt(format_args!(
            "TON->ETH event {:?} tx {} (lt {}) from {}. status: {:?}. address: {}",
            info.kind(),
            hex::encode(&data.init_data.event_transaction),
            data.init_data.event_transaction_lt,
            hex::encode(&info.relay_key()),
            data.status,
            info.event_address()
        ))
    }
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

    async fn send(&self, bridge: Arc<BridgeContract>) -> ContractResult<()> {
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

    async fn send(&self, bridge: Arc<BridgeContract>) -> ContractResult<()> {
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
