use relay_ton::contracts::*;

use crate::config::TonSettings;
use crate::crypto::key_managment::*;
use crate::db::TonVerificationQueue;
use crate::models::*;
use crate::prelude::*;

use super::event_transport::*;
use super::semaphore::*;
use super::utils;

pub type TonEventTransport = EventTransport<TonEventConfigurationContract>;

pub struct TonEventsHandler {
    state: Arc<State>,
}

struct State {
    transport: Arc<TonEventTransport>,
    eth_signer: EthSigner,
    verification_queue: TonVerificationQueue,
    address: MsgAddressInt,

    configuration_id: u32,
    details: TonEventConfiguration,
    config_contract: Arc<TonEventConfigurationContract>,
    swapback_contract: Arc<TonSwapBackContract>,
    is_scanning: RwLock<bool>,
    settings: TonSettings,
}

impl TonEventsHandler {
    pub async fn new(
        transport: Arc<TonEventTransport>,
        eth_signer: EthSigner,
        verification_queue: TonVerificationQueue,
        configuration_id: u32,
        address: MsgAddressInt,
        ton_config: &TonSettings,
    ) -> Result<Arc<Self>, Error> {
        use futures::FutureExt;

        if !transport.ensure_configuration_identity(&address).await {
            return Err(anyhow!(
                "Already subscribed to this TON configuration contract"
            ));
        }
        let mut attempts_count = ton_config.events_handler_retry_count;

        // Create TON config contract
        let (config_contract, config_contract_events) = loop {
            match make_ton_event_configuration_contract(
                transport.ton_transport().clone(),
                address.clone(),
                transport.bridge_contract().address().clone(),
            )
            .await
            {
                Ok(a) => break a,
                Err(e) => {
                    log::error!(
                        "Failed creating ton config contract: {}. Attempts left: {}",
                        e,
                        attempts_count
                    );
                    tokio::time::delay_for(ton_config.events_handler_interval).await;
                    if attempts_count == 0 {
                        log::error!("Failed creating ton config contract. Giving up.")
                    }
                    attempts_count -= 1;
                    continue;
                }
            }
        };

        // Get its data
        let details = match transport
            .get_event_configuration_details(config_contract.as_ref())
            .await
        {
            Ok(details) => details,
            Err(e) => {
                transport
                    .forget_configuration(config_contract.address())
                    .await;
                return Err(e);
            }
        };

        // Register contract
        transport
            .add_configuration_contract(configuration_id, config_contract.clone())
            .await;

        // Get swapback contract
        let (swapback_contract, swapback_events) = match make_ton_swapback_contract(
            transport.ton_transport().clone(),
            details.event_address.clone(),
            details.common.event_abi.clone(),
        )
        .await
        {
            Ok(contract) => contract,
            Err(e) => {
                transport
                    .forget_configuration(config_contract.address())
                    .await;
                return Err(e.into());
            }
        };

        // Finally create handler
        let handler = Arc::new(Self {
            state: Arc::new(State {
                transport: transport.clone(),
                eth_signer,
                verification_queue,
                address,
                configuration_id,
                details,
                config_contract: config_contract.clone(),
                swapback_contract: swapback_contract.clone(),
                is_scanning: RwLock::new(true),
                settings: ton_config.clone(),
            }),
        });

        // Start listeners
        handler.start_listening_swapback_events(swapback_events);
        handler.start_listening_vote_events(config_contract_events);
        handler.start_watching_pending_confirmations();

        //
        let scanning_state = transport.scanning_state();

        // Process all past swapback events
        let swapback_events_semaphore = {
            let semaphore = Semaphore::new_empty();

            let latest_known_lt = swapback_contract.since_lt();
            let address = swapback_contract.address();

            let latest_scanned_lt = scanning_state
                .get_latest_scanned_lt(address)
                .expect("Fatal db error");

            let mut known_event_count = 0;

            let mut known_events =
                swapback_contract.get_known_events(latest_scanned_lt, latest_known_lt);
            while let Some(event) = known_events.next().await {
                handler.handle_swapback(event, Some(semaphore.clone()));
                known_event_count += 1;
            }

            semaphore.wait_count(known_event_count).map(move |_| {
                // Only update latest scanned lt when all scanned events were processed
                scanning_state
                    .update_latest_scanned_lt(address, latest_known_lt)
                    .expect("Fatal db error");
            })
        };

        // Process all past voting events
        let votes_events_semaphore = {
            let semaphore = Semaphore::new_empty();

            let latest_known_lt = config_contract.since_lt();
            let address = config_contract.address();

            let latest_scanned_lt = scanning_state
                .get_latest_scanned_lt(address)
                .expect("Fatal db error");

            let mut known_event_count = 0;

            let mut known_events =
                config_contract.get_known_events(latest_scanned_lt, latest_known_lt);
            while let Some(event) = known_events.next().await {
                handler.handle_vote(event, Some(semaphore.clone()));
                known_event_count += 1;
            }

            semaphore.wait_count(known_event_count).map(move |_| {
                // Only update latest scanned lt when all scanned events are in ton queue
                scanning_state
                    .update_latest_scanned_lt(address, latest_known_lt)
                    .expect("Fatal db error");
            })
        };

        futures::future::join(swapback_events_semaphore, votes_events_semaphore).await;

        *handler.state.is_scanning.write().await = false;

        Ok(handler)
    }

    pub fn get_details(&self) -> (&TonEventConfiguration, &MsgAddressInt) {
        (&self.state.details, &self.state.address)
    }

    fn start_listening_vote_events(
        self: &Arc<Self>,
        mut config_contract_events: EventsRx<
            <TonEventConfigurationContract as ContractWithEvents>::Event,
        >,
    ) {
        let handler = Arc::downgrade(&self);

        tokio::spawn(async move {
            while let Some(event) = config_contract_events.next().await {
                match handler.upgrade() {
                    // Handle event if handler is still alive
                    Some(handler) => handler.handle_vote(event, None),
                    // Stop subscription when handler is dropped
                    None => return,
                }
            }
        });
    }

    fn handle_vote(
        &self,
        event: <TonEventConfigurationContract as ContractWithEvents>::Event,
        semaphore: Option<Semaphore>,
    ) {
        log::debug!("Got TON->ETH event vote: {:?}", event);

        let (event_address, relay, vote) = match event {
            TonEventConfigurationContractEvent::EventConfirmation { address, relay } => {
                (address, relay, Voting::Confirm)
            }
            TonEventConfigurationContractEvent::EventReject { address, relay } => {
                (address, relay, Voting::Reject)
            }
        };

        tokio::spawn({
            let state = self.state.clone();

            async move {
                state
                    .transport
                    .handle_event(
                        &state,
                        TonEventReceivedVote::new(
                            state.configuration_id,
                            event_address,
                            relay,
                            vote,
                            state.swapback_contract.abi().clone(),
                        ),
                    )
                    .await;
                semaphore.try_notify().await;
            }
        });
    }

    fn start_listening_swapback_events(self: &Arc<Self>, mut swapback_events: SwapBackEvents) {
        let handler = Arc::downgrade(&self);

        tokio::spawn(async move {
            while let Some(event) = swapback_events.next().await {
                match handler.upgrade() {
                    // Handle event if handler is still alive
                    Some(handler) => handler.handle_swapback(event, None),
                    // Stop subscription when handler is dropped
                    None => return,
                }
            }
        });
    }

    fn handle_swapback(&self, event: SwapBackEvent, semaphore: Option<Semaphore>) {
        async fn confirm(state: Arc<State>, event: SwapBackEvent) -> Result<(), Error> {
            state.transport.enqueue_vote(event.confirmed(&state)?).await
        }

        log::info!("Got swap back event: {:?}", event);

        tokio::spawn({
            let state = self.state.clone();

            async move {
                if let Err(e) = confirm(state, event).await {
                    log::error!("Failed to enqueue swapback event confirmation: {:?}", e);
                }
                semaphore.try_notify().await;
            }
        });
    }

    fn start_watching_pending_confirmations(self: &Arc<Self>) {
        let settings = self.state.transport.settings();
        let interval = settings.ton_events_verification_interval;
        let handler = Arc::downgrade(self);

        tokio::spawn(async move {
            {
                let handler = match handler.upgrade() {
                    Some(handler) => handler,
                    None => return,
                };

                let (lt, _) = handler.state.swapback_contract.current_time().await;

                let prepared_votes = handler
                    .state
                    .verification_queue
                    .range_before(
                        lt.checked_sub(
                            handler
                                .state
                                .transport
                                .settings()
                                .ton_events_verification_queue_lt_offset,
                        )
                        .unwrap_or_default(),
                    )
                    .await;

                for (entry, event) in prepared_votes {
                    tokio::spawn(handler.clone().handle_restored_swapback(event));
                    entry.remove().expect("Fatal db error");
                }
            }

            tokio::time::delay_for(interval).await;
        });
    }

    async fn handle_restored_swapback(self: Arc<Self>, event: SignedTonEventVoteData) {
        let mut counter = 50;
        let event_address = loop {
            match self
                .state
                .config_contract
                .compute_event_address(event.data.clone())
                .await
            {
                Ok(address) => break address,
                Err(e) => {
                    if counter == 0 {
                        log::error!("Failed to compute address for restored event. 0 attempts left. Giving up to do it.");
                        return;
                    }
                    log::error!("Failed to compute address for restored event: {:?}. Retrying. {} attempts left", e, counter);
                    tokio::time::delay_for(Duration::from_secs(10)).await;
                    counter -= 1;
                }
            }
        };

        if !self.state.transport.is_in_queue(&event_address)
            && !self.state.transport.has_already_voted(&event_address)
        {
            tokio::spawn(
                self.state
                    .transport
                    .clone()
                    .ensure_sent(event_address, EventTransaction::Reject(event.data)),
            );
        }
    }
}

impl State {
    fn calculate_signature(&self, event: &SwapBackEvent) -> Result<Vec<u8>, Error> {
        let payload = utils::prepare_ton_event_payload(
            &self.config_contract.address(),
            &self.details,
            event,
        )?;
        let signature = self.eth_signer.sign(&payload);

        log::info!(
            "Calculated swap back event signature: {} for payload: {}",
            hex::encode(&signature),
            hex::encode(&payload)
        );
        Ok(signature)
    }
}

#[async_trait]
impl EventsVerifier<TonEventReceivedVote> for State {
    async fn enqueue(&self, event: TonEventReceivedVoteWithData) {
        let reject = |event: TonEventReceivedVoteWithData| async move {
            if let Err(e) = self
                .transport
                .enqueue_vote(EventTransaction::Reject(event.into_vote()))
                .await
            {
                log::error!("Failed to enqueue invalid event rejection vote: {:?}", e);
            }
        };

        let event_lt = event.data().init_data.event_transaction_lt;
        let latest_scanned_lt = self
            .transport
            .scanning_state()
            .get_latest_scanned_lt(self.swapback_contract.address())
            .expect("Fatal db error");

        if let Some(lt) = latest_scanned_lt {
            if event_lt < lt {
                log::error!("Event is too old: {} < {}", event_lt, lt);
                return reject(event).await;
            }
        }

        let now = chrono::Utc::now().timestamp();
        let allowed_timestamp = now + self.settings.ton_events_allowed_time_diff as i64;
        let event_timestamp = event.data().init_data.event_timestamp as i64;
        if event_timestamp > allowed_timestamp {
            log::error!(
                "Got event from future. Now {}, Event: {}, Diff: {:?}",
                now,
                event_timestamp,
                event_timestamp.checked_sub(now)
            );
            return reject(event).await;
        }

        let abi: &Arc<AbiEvent> = event.info().additional();
        let init_data = &event.data().init_data;

        let signature = match abi
            .decode_input(init_data.event_data.clone().into())
            .map_err(|e| anyhow!("failed decoding TON event data: {:?}", e))
            .and_then(|tokens| {
                self.calculate_signature(&SwapBackEvent {
                    event_transaction: init_data.event_transaction,
                    event_transaction_lt: init_data.event_transaction_lt,
                    event_timestamp: init_data.event_timestamp,
                    event_index: init_data.event_index,
                    tokens,
                })
            }) {
            Ok(signature) => signature,
            Err(e) => {
                log::error!("Failed to sign event from TON: {:?}", e);
                return reject(event).await;
            }
        };

        self.verification_queue
            .insert(
                event_lt,
                &SignedTonEventVoteData {
                    data: event.into_vote(),
                    signature,
                },
            )
            .await
            .expect("Fatal db error");
    }
}

trait SwapBackEventExt: Sized {
    fn confirmed(self, state: &State) -> Result<TonEventTransaction, Error>;
}

impl SwapBackEventExt for SwapBackEvent {
    fn confirmed(self, state: &State) -> Result<TonEventTransaction, Error> {
        let event_id = state.swapback_contract.abi().id;

        let signature = state.calculate_signature(&self)?;
        let event_data = utils::pack_event_data_into_cell(event_id, &self.tokens)?;

        Ok(TonEventTransaction::Confirm(SignedTonEventVoteData {
            data: TonEventVoteData {
                configuration_id: state.configuration_id,
                event_transaction: self.event_transaction,
                event_transaction_lt: self.event_transaction_lt,
                event_timestamp: self.event_timestamp,
                event_index: self.event_index,
                event_data,
            },
            signature,
        }))
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
            info.relay(),
            data.status,
            info.event_address()
        ))
    }
}
