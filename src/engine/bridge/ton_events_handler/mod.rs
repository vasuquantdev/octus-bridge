use relay_ton::contracts::*;

use super::utils;
use crate::crypto::key_managment::*;
use crate::models::*;
use crate::prelude::*;

use super::event_transport::*;
use super::semaphore::*;
use tokio::stream::StreamExt;

pub type TonEventTransport = EventTransport<TonEventConfigurationContract>;

pub struct TonEventsHandler {
    state: Arc<State>,
}

struct State {
    transport: Arc<TonEventTransport>,
    eth_signer: EthSigner,

    configuration_id: u64,
    details: TonEventConfiguration,
    config_contract: Arc<TonEventConfigurationContract>,
    swapback_contract: Arc<TonSwapBackContract>,
}

impl TonEventsHandler {
    pub async fn new(
        transport: Arc<TonEventTransport>,
        eth_signer: EthSigner,
        configuration_id: u64,
        address: MsgAddressInt,
    ) -> Result<Arc<Self>, Error> {
        use futures::FutureExt;

        if !transport.ensure_configuration_identity(&address).await {
            return Err(anyhow!(
                "Already subscribed to this TON configuration contract"
            ));
        }

        // Create TON config contract
        let (config_contract, mut config_contract_events) = make_ton_event_configuration_contract(
            transport.ton_transport().clone(),
            address.clone(),
            transport.bridge_contract().address().clone(),
        )
        .await
        .unwrap(); // todo retry subscription

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
        let (swapback_contract, mut swapback_events) = match make_ton_swapback_contract(
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
                configuration_id,
                details,
                config_contract: config_contract.clone(),
                swapback_contract: swapback_contract.clone(),
            }),
        });

        // Start listening swapback events
        tokio::spawn({
            let handler = Arc::downgrade(&handler);
            async move {
                while let Some(event) = swapback_events.next().await {
                    match handler.upgrade() {
                        // Handle event if handler is still alive
                        Some(handler) => handler.handle_swapback(event, None),
                        // Stop subscription when handler is dropped
                        None => return,
                    }
                }
            }
        });

        // Start listening vote events
        tokio::spawn({
            let handler = Arc::downgrade(&handler);
            async move {
                while let Some(event) = config_contract_events.next().await {
                    match handler.upgrade() {
                        // Handle event if handler is still alive
                        Some(handler) => handler.handle_vote(event, None),
                        // Stop subscription when handler is dropped
                        None => return,
                    }
                }
            }
        });

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

        Ok(handler)
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
                        state.as_ref(),
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

    pub fn details(&self) -> &TonEventConfiguration {
        &self.state.details
    }
}

impl State {
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

#[async_trait]
impl VerificationQueue<TonEventReceivedVote> for State {
    async fn enqueue(&self, event: <TonEventReceivedVote as ReceivedVote>::VoteWithData) {
        // async fn verify(
        //     listener: &TonListener,
        //     transaction: TonEventReceivedVoteWithData,
        // ) -> Result<(), Error> {
        //     let abi: &Arc<AbiEvent> = transaction.info().additional();
        //
        //     let tokens = abi
        //         .decode_input(transaction.data().init_data.event_data.clone().into())
        //         .map_err(|e| anyhow!("failed decoding TON event data: {:?}", e))?;
        //
        //     let data = &transaction.data().init_data;
        //     let signature = listener.calculate_signature(&SwapBackEvent {
        //         event_transaction: data.event_transaction.clone(),
        //         event_transaction_lt: data.event_transaction_lt,
        //         event_index: data.event_index,
        //         tokens,
        //     })?;
        //
        //     listener
        //         .ton
        //         .enqueue_vote(TonEventTransaction::Confirm(SignedEventVotingData {
        //             data: transaction.into(),
        //             signature,
        //         }))
        //         .await
        // }
        //
        // if let Err(e) = verify(self, event).await {
        //     log::error!("Failed to enqueue TON event: {:?}", e);
        // }

        todo!()
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

        Ok(TonEventTransaction::Confirm(SignedVoteData {
            data: TonEventVoteData {
                configuration_id: state.configuration_id,
                event_transaction: self.event_transaction,
                event_transaction_lt: self.event_transaction_lt,
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
