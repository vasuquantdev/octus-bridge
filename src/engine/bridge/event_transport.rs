use relay_ton::contracts::*;
use relay_ton::transport::*;

use crate::config::TonSettings;
use crate::db::*;
use crate::models::*;
use crate::prelude::*;

pub struct EventTransport<C>
where
    C: ConfigurationContract,
{
    settings: TonSettings,
    relay: MsgAddrStd,

    parallel_spawned_contracts_limiter: tokio::sync::Semaphore,
    voting_stats:
        VotingStats<<<C as ConfigurationContract>::ReceivedVote as ReceivedVote>::VoteWithData>,
    votes_queue: VotesQueue<<C as ConfigurationContract>::EventTransaction>,

    transport: Arc<dyn Transport>,
    scanning_state: ScanningState,
    relay_contract: Arc<RelayContract>,
    event_contract: Arc<C::EventContract>,
    config_contracts: RwLock<ConfigContractsMap<C>>,

    confirmations: Mutex<HashMap<MsgAddrStd, oneshot::Sender<()>>>,
    rejections: Mutex<HashMap<MsgAddrStd, oneshot::Sender<()>>>,

    known_config_addresses: Mutex<HashSet<MsgAddressInt>>,
}

impl<C> EventTransport<C>
where
    C: ConfigurationContract,
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
    pub async fn new(
        db: &Db,
        transport: Arc<dyn Transport>,
        scanning_state: ScanningState,
        relay_contract: Arc<RelayContract>,
        settings: TonSettings,
    ) -> Result<Self, Error> {
        let relay = relay_contract.address().clone();
        let event_contract = C::make_event_contract(transport.clone()).await;
        let voting_stats = C::make_voting_stats(db)?;
        let votes_queue = C::make_votes_queue(db)?;

        Ok(Self {
            parallel_spawned_contracts_limiter: tokio::sync::Semaphore::new(
                settings.parallel_spawned_contracts_limit,
            ),
            settings,
            relay,
            voting_stats,
            votes_queue,
            transport,
            scanning_state,
            relay_contract,
            event_contract,
            config_contracts: Default::default(),
            confirmations: Default::default(),
            rejections: Default::default(),
            known_config_addresses: Default::default(),
        })
    }

    pub async fn handle_event(
        &self,
        verification_queue: &dyn VerificationQueue<C::ReceivedVote>,
        received_vote: C::ReceivedVote,
    ) {
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
            && vote_info.relay() != &self.relay // event from other relay
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

        if vote_info.relay() == &self.relay {
            // Stop retrying after our event response was found
            if let Err(e) = self.votes_queue.mark_complete(vote_info.event_address()) {
                log::error!("Failed to mark transaction completed. {:?}", e);
            }

            self.notify_found(vote_info.event_address(), vote_info.kind())
                .await;
        } else if should_check {
            verification_queue.enqueue(received_vote).await
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
            use std::collections::hash_map::Entry;
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
        let _permit = self.parallel_spawned_contracts_limiter.acquire().await;
        // Send a message with several retries on failure
        let result = loop {
            // Prepare delay future
            let delay = tokio::time::delay_for(retries_interval);
            retries_interval = std::time::Duration::from_secs_f64(
                retries_interval.as_secs_f64() * self.settings.message_retry_interval_multiplier,
            );

            // Try to send message
            if let Err(e) = data.send(self.relay_contract.clone()).await {
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
                .has_already_voted(&event_address, &self.relay)
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

    /// Relay contract for this event transport
    pub fn bridge_contract(&self) -> &Arc<BridgeContract> {
        self.relay_contract.bridge()
    }

    /// TON transport, used to send events to the network
    pub fn ton_transport(&self) -> &Arc<dyn Transport> {
        &self.transport
    }

    /// Current account scanning positions
    pub fn scanning_state(&self) -> &ScanningState {
        &self.scanning_state
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
            .has_already_voted(event_address, &self.relay)
            .expect("Fatal db error")
    }

    /// Check current queue whether transaction exists
    fn is_in_queue(&self, event_address: &MsgAddrStd) -> bool {
        self.votes_queue
            .has_event(event_address)
            .expect("Fatal db error")
    }

    /// Add new configuration contract
    pub async fn add_configuration_contract(&self, configuration_id: BigUint, contract: Arc<C>) {
        self.config_contracts
            .write()
            .await
            .insert(configuration_id, contract);
    }

    /// Find configuration contract by its id
    pub async fn get_configuration_contract(&self, configuration_id: &BigUint) -> Option<Arc<C>> {
        self.config_contracts
            .read()
            .await
            .get(configuration_id)
            .cloned()
    }

    /// Inserts address into known addresses and returns `true` if it wasn't known before
    pub async fn ensure_configuration_identity(&self, address: &MsgAddressInt) -> bool {
        self.known_config_addresses
            .lock()
            .await
            .insert(address.clone())
    }

    /// Removes address from known addresses
    pub async fn forget_configuration(&self, address: &MsgAddressInt) {
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

pub struct DisplayReceivedVote<'a, T>(&'a T);

impl<'a, T> DisplayReceivedVote<'a, T> {
    pub fn inner(&self) -> &'a T {
        self.0
    }
}

type ConfigContractsMap<T> = HashMap<BigUint, Arc<T>>;

#[async_trait]
pub trait VerificationQueue<T: ReceivedVote>: Send + Sync {
    async fn enqueue(&self, event: T::VoteWithData);
}

#[async_trait]
pub trait ConfigurationContract: ContractWithEvents {
    type Details;
    type EventContract: EventContract;
    type ReceivedVote: ReceivedVote;
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
pub trait EventContract: Send + Sync {
    type Details;
    type InitData;

    async fn get_details(&self, address: &MsgAddrStd) -> ContractResult<Self::Details>;
}

#[async_trait]
pub trait EventTransactionExt: std::fmt::Display + Clone + Send + Sync {
    type InitData: Clone;

    fn configuration_id(&self) -> &BigUint;
    fn kind(&self) -> Voting;
    fn init_data(&self) -> Self::InitData;
    async fn send(&self, bridge: Arc<RelayContract>) -> ContractResult<()>;
}
