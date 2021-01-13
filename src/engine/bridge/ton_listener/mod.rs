mod semaphore;

use std::collections::hash_map::Entry;

use async_trait::async_trait;
use ethabi::ParamType as EthParamType;
use num_bigint::BigUint;
use tokio::stream::StreamExt;
use tokio::sync::oneshot;
use tokio::sync::{mpsc, Mutex, RwLock, RwLockReadGuard};
use ton_abi::ParamType as TonParamType;

use relay_ton::contracts::*;
use relay_ton::prelude::UInt256;
use relay_ton::transport::{Transport, TransportError};

use self::semaphore::*;
use super::util;
use crate::config::TonSettings;
use crate::crypto::key_managment::EthSigner;
use crate::db_management::*;
use crate::models::*;
use crate::prelude::*;
use serde::de::DeserializeOwned;

/// Listens to config streams and maps them.
pub struct TonListener {
    transport: Arc<dyn Transport>,
    bridge_contract: Arc<BridgeContract>,

    eth_signer: EthSigner,
    eth_queue: EthQueue,

    ton: Arc<TonAccessor<TonEventConfigurationContract>>,
    eth: Arc<TonAccessor<EthEventConfigurationContract>>,

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
        TonAccessor::new(
            db,
            transport.clone(),
            bridge_contract.clone(),
            settings.clone(),
        )
        .await?,
    );
    let eth = Arc::new(
        TonAccessor::new(
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
            if active_configuration.event_type == EventType::ETH {
                tokio::spawn(self.clone().subscribe_to_eth_events_configuration(
                    subscriptions_tx.clone(),
                    active_configuration.id,
                    active_configuration.address,
                    Some(semaphore.clone()),
                ));
            }
            // TODO: subscribe to TON configuration contract
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

    /// Created listener for TON events
    async fn subscribe_to_ton_events_configuration(
        self: Arc<Self>,
        configuration_id: BigUint,
        address: MsgAddressInt,
        semaphore: Option<Semaphore>,
    ) {
        if !self.ton.ensure_configuration_identity(&address).await {
            return semaphore.try_notify().await;
        }

        // Create TON config contract
        let (config_contract, mut _config_contract_events) = make_ton_event_configuration_contract(
            self.transport.clone(),
            address.clone(),
            self.bridge_contract.address().clone(),
        )
        .await
        .unwrap(); // todo retry subscription

        // Get it's data
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
                return semaphore.try_notify().await;
            }
        };

        // Register contract
        self.ton
            .config_contracts
            .write()
            .await
            .insert(configuration_id, config_contract.clone());

        // Start listening swapback events
        let mut swapback_events_contract = match make_ton_swapback_contract(
            self.transport.clone(),
            details.event_address.clone(),
            details.common.event_abi.clone(),
        )
        .await
        {
            Ok(contract) => {
                semaphore.try_notify().await;
                contract
            }
            Err(e) => {
                log::error!("{:?}", e);
                return semaphore.try_notify().await;
            }
        };

        // Start listening swapback events
        tokio::spawn({
            let listener = self.clone();
            async move {
                while let Some(tokens) = swapback_events_contract.next().await {
                    log::debug!("Got swap back event: {:?}", tokens);

                    let event_data = util::ton_tokens_to_ethereum_bytes(tokens);
                    let signature = listener.eth_signer.sign(&event_data);

                    log::debug!(
                        "Calculated swap back event signature: {}",
                        hex::encode(&signature)
                    );

                    // TODO: spawn confirmation
                }
            }
        });

        todo!()
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
            .config_contracts
            .write()
            .await
            .insert(configuration_id.clone(), config_contract.clone());
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
                                &listener,
                                EthEventReceivedVote::new_eth_event_vote(
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
}

#[derive(Debug, Clone, Default)]
pub struct ConfigsState {
    pub eth_addr: HashSet<Address>,
    pub address_topic_map: HashMap<Address, (H256, Vec<EthParamType>, Vec<TonParamType>)>,
    pub topic_abi_map: HashMap<H256, Vec<EthParamType>>,
    pub eth_configs_map: HashMap<Address, (BigUint, EthEventConfiguration)>,
}

impl ConfigsState {
    fn set_configuration(
        &mut self,
        configuration_id: BigUint,
        configuration: EthEventConfiguration,
    ) {
        let (topic_hash, eth_abi, ton_abi) =
            match util::parse_eth_abi(&configuration.common.event_abi) {
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

struct TonAccessor<C>
where
    C: ConfigurationContract,
{
    settings: TonSettings,
    relay_key: UInt256,

    voting_stats:
        VotingStats<<<C as ConfigurationContract>::ReceivedVote as ReceivedVote>::VoteWithData>,
    votes_queue: VotesQueue<<C as ConfigurationContract>::EventTransaction>,

    bridge_contract: Arc<BridgeContract>,
    event_contract: Arc<C::EventContract>,
    config_contracts: RwLock<ConfigContractsMap<C>>,

    confirmations: Mutex<HashMap<MsgAddrStd, oneshot::Sender<()>>>,
    rejections: Mutex<HashMap<MsgAddrStd, oneshot::Sender<()>>>,

    known_config_addresses: Mutex<HashSet<MsgAddressInt>>,
}

impl<C> TonAccessor<C>
where
    C: ConfigurationContract,
    <C::ReceivedVote as ReceivedVote>::VoteWithData: ReceivedVoteWithDataExt + Send + Sync,
    <C as ConfigurationContract>::ReceivedVote: ReceivedVote<
        Data = <<C as ConfigurationContract>::EventContract as EventContract>::Details,
    >,
    <C as ConfigurationContract>::EventTransaction: EventTransactionExt<
            InitData = <<C as ConfigurationContract>::EventContract as EventContract>::InitData,
        > + std::fmt::Display,
    <C::ReceivedVote as ReceivedVote>::VoteWithData: GetStoredData,
    <<C::ReceivedVote as ReceivedVote>::VoteWithData as GetStoredData>::Stored:
        Serialize + DeserializeOwned,
    for<'a> DisplayReceivedVote<'a, <C::ReceivedVote as ReceivedVote>::VoteWithData>:
        std::fmt::Display,
{
    async fn new(
        db: &Db,
        transport: Arc<dyn Transport>,
        bridge_contract: Arc<BridgeContract>,
        settings: TonSettings,
    ) -> Result<Self, Error> {
        let relay_key = bridge_contract.pubkey();
        let event_contract = C::make_event_contract(transport.clone()).await;
        let voting_stats = C::make_voting_stats(db)?;
        let votes_queue = C::make_votes_queue(db)?;

        Ok(Self {
            settings,
            relay_key,
            voting_stats,
            votes_queue,
            bridge_contract,
            event_contract,
            config_contracts: Default::default(),
            confirmations: Default::default(),
            rejections: Default::default(),
            known_config_addresses: Default::default(),
        })
    }

    pub async fn handle_event(&self, ton_listener: &TonListener, received_vote: C::ReceivedVote) {
        let data = match self.get_event_details(received_vote.event_address()).await {
            Ok(data) => data,
            Err(e) => {
                log::error!("Failed to get event details: {:?}", e);
                return;
            }
        };

        let received_vote = received_vote.with_data(data);
        let vote_info = received_vote.info();

        let should_check = vote_info.kind() == Voting::Confirm
            && received_vote.status() == EventStatus::InProcess
            && vote_info.relay_key() != self.relay_key // event from other relay
            && !self.is_in_queue(vote_info.event_address())
            && !self.has_already_voted(vote_info.event_address());

        log::info!(
            "Received {}, should check: {}",
            DisplayReceivedVote(&received_vote),
            should_check
        );

        self.voting_stats
            .insert_vote(&received_vote)
            .await
            .expect("Fatal db error");

        if vote_info.relay_key() == self.relay_key {
            // Stop retrying after our event response was found
            if let Err(e) = self.votes_queue.mark_complete(vote_info.event_address()) {
                log::error!("Failed to mark transaction completed. {:?}", e);
            }

            self.notify_found(vote_info.event_address(), vote_info.kind())
                .await;
        } else if should_check {
            received_vote.enqueue_verify(ton_listener).await
        }
    }

    /// Sends a message to TON with a small amount of retries on failures.
    /// Can be stopped using `cancel` or `notify_found`
    async fn ensure_sent(self: Arc<Self>, event_address: MsgAddrStd, data: C::EventTransaction) {
        // Skip voting for events which are already in stats db and TON queue
        if self.has_already_voted(&event_address) {
            // Make sure that TON queue doesn't contain this event
            self.votes_queue
                .mark_complete(&event_address)
                .expect("Fatal db error");
            return;
        }

        // Insert specified data in TON queue, replacing failed transaction if it exists
        self.votes_queue
            .insert_pending(&event_address, &data)
            .expect("Fatal db error");

        // Start listening for cancellation
        let (rx, vote) = {
            let vote = data.kind();

            // Get suitable channels map
            let mut runtime_queue = match vote {
                Voting::Confirm => self.confirmations.lock().await,
                Voting::Reject => self.rejections.lock().await,
            };

            // Just in case of duplication, check if we are already waiting cancellation for this
            // event data set
            match runtime_queue.entry(event_address.clone()) {
                Entry::Occupied(_) => {
                    return;
                }
                Entry::Vacant(entry) => {
                    let (tx, rx) = oneshot::channel();
                    entry.insert(tx);
                    (rx, vote)
                }
            }
        };

        let mut rx = Some(rx);
        let mut retries_count = self.settings.message_retry_count;
        let mut retries_interval = self.settings.message_retry_interval;

        // Send a message with several retries on failure
        let result = loop {
            // Prepare delay future
            let delay = tokio::time::delay_for(retries_interval);
            retries_interval = std::time::Duration::from_secs_f64(
                retries_interval.as_secs_f64() * self.settings.message_retry_interval_multiplier,
            );

            // Try to send message
            if let Err(e) = data.send(self.bridge_contract.clone()).await {
                log::error!(
                    "Failed to vote for event: {:?}. Retrying ({} left)",
                    e,
                    retries_count
                );

                retries_count -= 1;
                if retries_count < 0 {
                    break Err(e.into());
                }

                // Wait for prepared delay on failure
                delay.await;
            } else if let Some(rx_fut) = rx.take() {
                // Handle future results
                match future::select(rx_fut, delay).await {
                    // Got cancellation notification
                    future::Either::Left((Ok(()), _)) => {
                        log::info!("Got response for voting for {:?} {}", vote, data);
                        break Ok(());
                    }
                    // Stopped waiting for notification
                    future::Either::Left((Err(e), _)) => {
                        break Err(e.into());
                    }
                    // Timeout reached
                    future::Either::Right((_, new_rx)) => {
                        log::error!(
                            "Failed to get voting event response: timeout reached. Retrying ({} left for {})",
                            retries_count,
                            data
                        );

                        retries_count -= 1;
                        if retries_count < 0 {
                            break Err(anyhow!(
                                "Failed to vote for an event, no retries left ({})",
                                data
                            ));
                        }

                        rx = Some(new_rx);
                    }
                }
            } else {
                unreachable!()
            }

            // Check if event arrived nevertheless unsuccessful sending
            if self
                .voting_stats
                .has_already_voted(&event_address, &self.relay_key)
                .expect("Fatal db error")
            {
                break Ok(());
            }
        };

        match result {
            // Do nothing on success
            Ok(_) => log::info!("Stopped waiting for transaction: {}", data),
            // When ran out of retries, stop waiting for transaction and mark it as failed
            Err(e) => {
                log::error!("Stopped waiting for transaction: {}. Reason: {:?}", data, e);
                if let Err(e) = self.votes_queue.mark_failed(&event_address) {
                    log::error!(
                        "Failed to mark transaction with hash {} as failed: {:?}",
                        data,
                        e
                    );
                }
            }
        }

        // Remove cancellation channel
        self.cancel(&event_address, vote).await;
    }

    /// Remove transaction from TON queue and notify spawned `ensure_sent`
    async fn notify_found(&self, event_address: &MsgAddrStd, vote: Voting) {
        let mut table = match vote {
            Voting::Confirm => self.confirmations.lock().await,
            Voting::Reject => self.rejections.lock().await,
        };

        if let Some(tx) = table.remove(event_address) {
            if tx.send(()).is_err() {
                log::error!("Failed sending event notification");
            }
        }
    }

    /// Just remove the transaction from TON queue
    async fn cancel(&self, event_address: &MsgAddrStd, vote: Voting) {
        match vote {
            Voting::Confirm => self.confirmations.lock().await.remove(event_address),
            Voting::Reject => self.rejections.lock().await.remove(event_address),
        };
    }

    /// Restart voting for pending transactions
    pub fn retry_pending(self: &Arc<Self>) {
        for (event_address, data) in self.votes_queue.get_all_pending() {
            tokio::spawn(self.clone().ensure_sent(event_address, data));
        }
    }

    /// Restart voting for failed transactions
    pub fn retry_failed(self: &Arc<Self>) {
        for (event_address, data) in self.votes_queue.get_all_failed() {
            tokio::spawn(self.clone().ensure_sent(event_address, data));
        }
    }

    /// Adds transaction to queue, starts reliable sending
    pub async fn enqueue_vote(self: &Arc<Self>, data: C::EventTransaction) -> Result<(), Error> {
        let event_address = self.get_event_contract_address(&data).await?;

        tokio::spawn(self.clone().ensure_sent(event_address, data));

        Ok(())
    }

    /// Compute event address based on its data
    async fn get_event_contract_address(
        &self,
        transaction: &C::EventTransaction,
    ) -> Result<MsgAddrStd, Error> {
        let configuration_id = transaction.configuration_id();

        let config_contract = match self.get_configuration_contract(configuration_id).await {
            Some(contract) => contract,
            None => {
                return Err(anyhow!(
                    "Unknown event configuration contract: {}",
                    configuration_id
                ));
            }
        };

        let event_addr = config_contract
            .compute_event_address(transaction.init_data())
            .await?;

        Ok(event_addr)
    }

    /// Check statistics whether transaction exists
    fn has_already_voted(&self, event_address: &MsgAddrStd) -> bool {
        self.voting_stats
            .has_already_voted(event_address, &self.relay_key)
            .expect("Fatal db error")
    }

    /// Check current queue whether transaction exists
    pub fn is_in_queue(&self, event_address: &MsgAddrStd) -> bool {
        self.votes_queue
            .has_event(event_address)
            .expect("Fatal db error")
    }

    /// Find configuration contract by its address
    pub async fn get_configuration_contract(&self, configuration_id: &BigUint) -> Option<Arc<C>> {
        self.config_contracts
            .read()
            .await
            .get(configuration_id)
            .cloned()
    }

    /// Inserts address into known addresses and returns `true` if it wasn't known before
    async fn ensure_configuration_identity(&self, address: &MsgAddressInt) -> bool {
        self.known_config_addresses
            .lock()
            .await
            .insert(address.clone())
    }

    /// Removes address from known addresses
    async fn forget_configuration(&self, address: &MsgAddressInt) {
        self.known_config_addresses.lock().await.remove(address);
    }

    pub async fn get_event_configuration_details(
        &self,
        config_contract: &C,
    ) -> Result<<C as ConfigurationContract>::Details, Error> {
        let mut retry_count = self.settings.event_configuration_details_retry_count;
        let retry_interval = self.settings.event_configuration_details_retry_interval;

        loop {
            match config_contract.get_details().await {
                Ok(details) => match config_contract.validate(&details) {
                    Ok(_) => break Ok(details),
                    Err(e) => {
                        break Err(e);
                    }
                },
                Err(ContractError::TransportError(TransportError::AccountNotFound))
                    if retry_count > 0 =>
                {
                    retry_count -= 1;
                    log::warn!(
                        "failed to get configuration contract details for {}. Retrying ({} left)",
                        config_contract.address(),
                        retry_count
                    );
                    tokio::time::delay_for(retry_interval).await;
                }
                Err(e) => {
                    break Err(anyhow!(
                        "failed to get configuration contract details: {:?}",
                        e
                    ));
                }
            }
        }
    }

    async fn get_event_details(
        &self,
        address: &MsgAddrStd,
    ) -> Result<
        <<C as ConfigurationContract>::EventContract as EventContract>::Details,
        ContractError,
    > {
        let mut retry_count = self.settings.event_details_retry_count;
        let retry_interval = self.settings.event_details_retry_interval;

        loop {
            match self.event_contract.get_details(address).await {
                Ok(details) => break Ok(details),
                Err(ContractError::TransportError(TransportError::AccountNotFound))
                    if retry_count > 0 =>
                {
                    retry_count -= 1;
                    log::error!(
                        "Failed to get event details for {}. Retrying ({} left)",
                        address,
                        retry_count
                    );
                    tokio::time::delay_for(retry_interval).await;
                }
                Err(e) => break Err(e),
            };
        }
    }
}

type ConfigContractsMap<T> = HashMap<BigUint, Arc<T>>;

#[async_trait]
trait ConfigurationContract: ContractWithEvents {
    type Details;
    type EventContract: EventContract + Send + Sync;
    type ReceivedVote: ReceivedVote + Send + Sync;
    type EventTransaction: Serialize + DeserializeOwned + Send + Sync;

    async fn make_config_contract(
        transport: Arc<dyn Transport>,
        account: MsgAddressInt,
        bridge_address: MsgAddressInt,
    ) -> ContractResult<(Arc<Self>, EventsRx<<Self as ContractWithEvents>::Event>)>;

    async fn make_event_contract(transport: Arc<dyn Transport>) -> Arc<Self::EventContract>;

    fn make_voting_stats(
        db: &Db,
    ) -> Result<VotingStats<<Self::ReceivedVote as ReceivedVote>::VoteWithData>, Error>;

    fn make_votes_queue(db: &Db) -> Result<VotesQueue<Self::EventTransaction>, Error>;

    fn address(&self) -> &MsgAddressInt;

    fn validate(&self, details: &Self::Details) -> Result<(), Error>;

    async fn compute_event_address(
        &self,
        init_data: <Self::EventContract as EventContract>::InitData,
    ) -> ContractResult<MsgAddrStd>;

    async fn get_details(&self) -> ContractResult<Self::Details>;
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
        VotingStats::new_eth_voting_stats(db)
    }

    fn make_votes_queue(db: &Db) -> Result<VotesQueue<Self::EventTransaction>, Error> {
        VotesQueue::new_eth_votes_queue(db)
    }

    fn address(&self) -> &MsgAddressInt {
        self.address()
    }

    fn validate(&self, details: &Self::Details) -> Result<(), Error> {
        util::validate_ethereum_event_configuration(details)
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
        VotingStats::new_ton_voting_stats(db)
    }

    fn make_votes_queue(db: &Db) -> Result<VotesQueue<Self::EventTransaction>, Error> {
        VotesQueue::new_ton_votes_queue(db)
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
trait EventContract {
    type Details;
    type InitData;

    async fn get_details(&self, address: &MsgAddrStd) -> ContractResult<Self::Details>;
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
trait ReceivedVoteWithDataExt {
    fn status(&self) -> EventStatus;

    async fn enqueue_verify(self, listener: &TonListener);
}

#[async_trait]
impl ReceivedVoteWithDataExt for EthEventReceivedVoteWithData {
    fn status(&self) -> EventStatus {
        self.data().status
    }

    async fn enqueue_verify(self, listener: &TonListener) {
        let info = self.info();

        let target_block_number = self
            .data()
            .init_data
            .event_block_number
            .to_u64()
            .unwrap_or_else(u64::max_value)
            + *info.additional() as u64;

        if let Err(e) = listener
            .eth_queue
            .insert(target_block_number, &self.into())
            .await
        {
            log::error!("Failed to insert event confirmation. {:?}", e);
        }
    }
}

#[async_trait]
impl ReceivedVoteWithDataExt for TonEventReceivedVoteWithData {
    fn status(&self) -> EventStatus {
        self.data().status
    }

    async fn enqueue_verify(self, listener: &TonListener) {
        todo!()
    }
}

struct DisplayReceivedVote<'a, T>(&'a T);

impl std::fmt::Display for DisplayReceivedVote<'_, EthEventReceivedVoteWithData> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let info = self.0.info();
        let data = self.0.data();
        f.write_fmt(format_args!(
            "ETH->TON event {:?} tx {} (block {}) from {}. status: {:?}",
            info.kind(),
            hex::encode(&data.init_data.event_transaction),
            data.init_data.event_block_number,
            hex::encode(&info.relay_key()),
            data.status,
        ))
    }
}

impl std::fmt::Display for DisplayReceivedVote<'_, TonEventReceivedVoteWithData> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let info = self.0.info();
        let data = self.0.data();
        f.write_fmt(format_args!(
            "TON->ETH event {:?} tx {} (block {}) from {}. status: {:?}",
            info.kind(),
            hex::encode(&data.init_data.event_transaction),
            data.init_data.event_block_number,
            hex::encode(&info.relay_key()),
            data.status
        ))
    }
}

#[async_trait]
trait EventTransactionExt: std::fmt::Display + Clone + Send + Sync {
    type InitData: Clone;

    fn configuration_id(&self) -> &BigUint;
    fn kind(&self) -> Voting;
    fn init_data(&self) -> Self::InitData;
    async fn send(&self, bridge: Arc<BridgeContract>) -> ContractResult<()>;
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
        f.write_fmt(format_args!("Vote for TON transaction {}", transaction))
    }
}
