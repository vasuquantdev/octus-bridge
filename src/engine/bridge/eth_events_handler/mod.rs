use relay_ton::contracts::*;

use crate::db::*;
use crate::models::*;
use crate::prelude::*;

use super::event_transport::*;
use super::semaphore::*;

#[async_trait]
/// `EthEventsHandler` but in uninitialized state.
/// Made for not spawning several same `EthEventsHandlerS`
pub trait UnInitEventsHandler: Sized {
    type Handler;

    fn details(&self) -> &EthEventConfiguration;

    async fn start(self) -> Arc<Self::Handler>;
}

pub type EthEventTransport = EventTransport<EthEventConfigurationContract>;

pub struct EthEventsHandler {
    state: Arc<State>,
}

struct State {
    transport: Arc<EthEventTransport>,
    verification_queue: EthVerificationQueue,

    configuration_id: u32,
    details: EthEventConfiguration,
    config_contract: Arc<EthEventConfigurationContract>,
}

impl EthEventsHandler {
    pub async fn uninit(
        transport: Arc<EthEventTransport>,
        verification_queue: EthVerificationQueue,
        configuration_id: u32,
        address: MsgAddressInt,
        ton_config: &crate::config::TonSettings,
    ) -> Result<impl UnInitEventsHandler<Handler = Self>, Error> {
        if !transport.ensure_configuration_identity(&address).await {
            return Err(anyhow!(
                "Already subscribed to this ETH configuration contract"
            ));
        }
        let mut attempts_count = ton_config.events_handler_retry_count;
        // Create ETH config contract
        let (config_contract, config_contract_events) = loop {
            match make_eth_event_configuration_contract(
                transport.ton_transport().clone(),
                address.clone(),
                transport.bridge_contract().address().clone(),
            )
            .await
            {
                Ok(a) => break a,
                Err(e) => {
                    log::error!(
                        "Failed creating eth config contract: {}. Attempts left: {}",
                        e,
                        attempts_count
                    );
                    tokio::time::delay_for(ton_config.events_handler_interval).await;
                    if attempts_count == 0 {
                        log::error!("Failed creating eth config contract. Giving up.")
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

        // Register contract object
        transport
            .add_configuration_contract(configuration_id, config_contract.clone())
            .await;

        let state = Arc::new(State {
            transport,
            verification_queue,

            configuration_id,
            details,
            config_contract,
        });

        Ok(UnInitEthEventsHandler {
            state,
            config_contract_events,
        })
    }

    fn handle_vote(
        &self,
        event: <EthEventConfigurationContract as ContractWithEvents>::Event,
        semaphore: Option<Semaphore>,
    ) {
        log::debug!("got ETH->TON event vote: {:?}", event);

        let (event_addr, relay, kind) = match event {
            EthEventConfigurationContractEvent::EventConfirmation { address, relay } => {
                (address, relay, Voting::Confirm)
            }
            EthEventConfigurationContractEvent::EventReject { address, relay } => {
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
                        EthEventReceivedVote::new(
                            state.configuration_id,
                            event_addr,
                            relay,
                            kind,
                            state.details.event_blocks_to_confirm,
                        ),
                    )
                    .await;
                if let Some(semaphore) = semaphore {
                    semaphore.notify().await;
                }
            }
        });
    }

    pub fn details(&self) -> &EthEventConfiguration {
        &self.state.details
    }
}

struct UnInitEthEventsHandler {
    state: Arc<State>,
    config_contract_events: EventsRx<<EthEventConfigurationContract as ContractWithEvents>::Event>,
}

#[async_trait]
impl UnInitEventsHandler for UnInitEthEventsHandler {
    type Handler = EthEventsHandler;

    fn details(&self) -> &EthEventConfiguration {
        &self.state.details
    }

    async fn start(self) -> Arc<Self::Handler> {
        let UnInitEthEventsHandler {
            state,
            mut config_contract_events,
        } = self;

        let handler = Arc::new(EthEventsHandler { state });

        // Spawn listener of new events
        tokio::spawn({
            let handler = Arc::downgrade(&handler);
            async move {
                while let Some(event) = config_contract_events.next().await {
                    match handler.upgrade() {
                        // Handle event if handler is still alive
                        Some(handler) => handler.handle_vote(event, None),
                        // Stop subscription when handler is dropped
                        None => return,
                    };
                }
            }
        });

        let state = &handler.state;

        // Process all past events
        let scanning_state = state.transport.scanning_state();
        let config_contract = state.config_contract.clone();

        let latest_known_lt = state.config_contract.since_lt();

        let latest_scanned_lt = scanning_state
            .get_latest_scanned_lt(config_contract.address())
            .expect("Fatal db error");

        let events_semaphore = Semaphore::new_empty();

        let mut known_events = config_contract.get_known_events(latest_scanned_lt, latest_known_lt);
        let mut known_event_count = 0;
        while let Some(event) = known_events.next().await {
            handler.handle_vote(event, Some(events_semaphore.clone()));
            known_event_count += 1;
        }

        // Only update latest scanned lt when all scanned events are in ton queue
        events_semaphore.wait_count(known_event_count).await;

        scanning_state
            .update_latest_scanned_lt(config_contract.address(), latest_known_lt)
            .expect("Fatal db error");

        handler
    }
}

#[async_trait]
impl EventsVerifier<EthEventReceivedVote> for State {
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
            .verification_queue
            .insert(target_block_number, &event.into_vote())
            .await
        {
            log::error!("Failed to insert event confirmation. {:?}", e);
        }
    }
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
            info.relay(),
            data.status,
            info.event_address()
        ))
    }
}
