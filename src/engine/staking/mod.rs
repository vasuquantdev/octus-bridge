use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use nekoton_abi::UnpackAbiPlain;
use tokio::sync::mpsc;
use tokio::sync::Notify;
use ton_types::UInt256;

use crate::engine::keystore::*;
use crate::engine::ton_contracts::user_data_contract::confirm_ton_account;
use crate::engine::ton_contracts::*;
use crate::engine::ton_subscriber::*;
use crate::engine::EngineContext;
use crate::utils::*;

pub struct Staking {
    context: Arc<EngineContext>,

    current_relay_round: tokio::sync::Mutex<RoundState>,
    relay_round_started_notify: Notify,
    elections_start_notify: Notify,
    elections_end_notify: Notify,
    election_timings_changed_notify: Notify,

    staking_account: UInt256,
    staking_observer: Arc<AccountObserver<StakingEvent>>,
    user_data_account: UInt256,
    user_data_observer: Arc<AccountObserver<UserDataEvent>>,
}

impl Staking {
    pub async fn new(context: Arc<EngineContext>, staking_account: UInt256) -> Result<Arc<Self>> {
        let (staking_events_tx, staking_events_rx) = mpsc::unbounded_channel();
        let (user_data_events_tx, user_data_events_rx) = mpsc::unbounded_channel();

        let staking_contract = context
            .ton_subscriber
            .wait_contract_state(staking_account)
            .await?;
        let staking_contract = StakingContract(&staking_contract);

        // Get bridge ETH event configuration
        let bridge_event_configuration = staking_contract
            .get_eth_bridge_configuration_details(&context)
            .await?;
        log::info!(
            "Bridge event configuration: {:?}",
            bridge_event_configuration
        );

        // Initialize user data
        let user_data_account = staking_contract.get_user_data_address(&context.staker_address)?;
        log::info!("Waiting user data account: {:x}", user_data_account);
        let user_data_contract = context
            .ton_subscriber
            .wait_contract_state(user_data_account)
            .await?;

        let user_data_contract = UserDataContract(&user_data_contract);
        let details = user_data_contract.get_details()?;
        if &details.relay_eth_address != context.keystore.eth.address().as_fixed_bytes() {
            return Err(StakingError::UserDataEthAddressMismatch.into());
        }
        if details.relay_ton_pubkey != context.keystore.ton.public_key() {
            return Err(StakingError::UserDataTonPublicKeyMismatch.into());
        }

        let user_data_observer = AccountObserver::new(&user_data_events_tx);
        if !details.ton_pubkey_confirmed {
            context
                .deliver_message(
                    user_data_observer.clone(),
                    UnsignedMessage::new(confirm_ton_account(), user_data_account),
                    true,
                )
                .await
                .context("Failed confirming ton account")?;
            log::info!("Confirmed ton address");
        }
        if !details.eth_address_confirmed {
            let subscriber = context
                .eth_subscribers
                .get_subscriber(bridge_event_configuration.network_configuration.chain_id)
                .ok_or(StakingError::RequiredEthNetworkNotFound)?;
            subscriber
                .verify_relay_staker_address(
                    context.keystore.eth.address(),
                    context.staker_address,
                    &bridge_event_configuration
                        .network_configuration
                        .event_emitter
                        .into(),
                )
                .await?;
        }

        // Initialize relay round
        let current_relay_round = staking_contract.collect_relay_round_state(&context).await?;

        let should_vote = matches!(
            current_relay_round.elections_state,
            ElectionsState::Started { .. }
        ) && !current_relay_round.elected;

        //
        let staking = Arc::new(Self {
            context,
            current_relay_round: tokio::sync::Mutex::new(current_relay_round),
            relay_round_started_notify: Default::default(),
            elections_start_notify: Default::default(),
            elections_end_notify: Default::default(),
            election_timings_changed_notify: Default::default(),
            staking_account,
            staking_observer: AccountObserver::new(&staking_events_tx),
            user_data_account,
            user_data_observer,
        });

        start_listening_events(
            &staking,
            "StakingContract",
            staking_events_rx,
            Self::process_staking_event,
        );
        start_listening_events(
            &staking,
            "UserDataContract",
            user_data_events_rx,
            Self::process_user_data_event,
        );

        let context = &staking.context;
        context
            .ton_subscriber
            .add_transactions_subscription([staking_account], &staking.staking_observer);
        context
            .ton_subscriber
            .add_transactions_subscription([user_data_account], &staking.user_data_observer);

        if should_vote {
            staking.become_relay_next_round().await?;
        }

        // TODO: get all shard states and collect reward

        staking.start_managing_elections();

        Ok(staking)
    }

    async fn process_staking_event(
        self: Arc<Self>,
        (_, event): (UInt256, StakingEvent),
    ) -> Result<()> {
        let mut current_relay_round = self.current_relay_round.lock().await;

        match event {
            StakingEvent::ElectionStarted(_) => {
                self.elections_start_notify.notify_waiters();
                *current_relay_round = self.collect_relay_round_state().await?;

                let staking = self.clone();
                tokio::spawn(async move {
                    let notify_fut = staking.elections_end_notify.notified();
                    tokio::select! {
                        result = staking.become_relay_next_round() => {
                            if let Err(e) = result {
                                log::error!("Failed to become relay next round: {:?}", e);
                            }
                        },
                        _ = notify_fut => {
                            log::warn!("Early exit from become_relay_next_round due to the elections end");
                        }
                    }
                });
            }
            StakingEvent::ElectionEnded(_) => {
                self.elections_end_notify.notify_waiters();
                *current_relay_round = self.collect_relay_round_state().await?;
            }
            StakingEvent::RelayRoundInitialized(event) => {
                self.relay_round_started_notify.notify_waiters();
                *current_relay_round = self.collect_relay_round_state().await?;

                let staking = self.clone();
                tokio::spawn(async move {
                    const ROUND_OFFSET: u64 = 10; // seconds

                    let now = chrono::Utc::now().timestamp() as u64;
                    tokio::time::sleep(Duration::from_secs(
                        (event.round_end_time as u64).wrapping_sub(now) + ROUND_OFFSET,
                    ))
                    .await;

                    if let Err(e) = staking.get_reward_for_relay_round(event.round_num).await {
                        log::error!(
                            "Failed to collect reward for round {}: {:?}",
                            event.round_num,
                            e
                        );
                    }
                });
            }
            StakingEvent::RelayConfigUpdated(RelayConfigUpdatedEvent { config }) => {
                self.election_timings_changed_notify.notify_waiters();

                let round_start_time = current_relay_round.round_start_time;
                match &mut current_relay_round.elections_state {
                    ElectionsState::NotStarted { start_time } => {
                        *start_time = round_start_time + config.time_before_election;
                    }
                    ElectionsState::Started {
                        start_time,
                        end_time,
                    } => {
                        *end_time = *start_time + config.election_time;
                    }
                    ElectionsState::Finished => { /* do nothing */ }
                }
            }
        }

        Ok(())
    }

    async fn process_user_data_event(
        self: Arc<Self>,
        (_, event): (UInt256, UserDataEvent),
    ) -> Result<()> {
        match event {
            UserDataEvent::RelayMembershipRequested(_data) => {
                //self.staking_state.applied_round(data.round_num);
                // TODO: confirm address and pubkey
            }
        }
        Ok(())
    }

    fn start_managing_elections(self: &Arc<Self>) {
        let staking = Arc::downgrade(self);

        tokio::spawn(async move {
            loop {
                let staking = match staking.upgrade() {
                    Some(staking) => staking,
                    None => return,
                };

                let (
                    elections_state,
                    new_round_fut,
                    elections_start_fut,
                    elections_end_fut,
                    timings_changed_fut,
                ) = {
                    let current_relay_round = staking.current_relay_round.lock().await;
                    (
                        current_relay_round.elections_state,
                        staking.relay_round_started_notify.notified(),
                        staking.elections_start_notify.notified(),
                        staking.elections_end_notify.notified(),
                        staking.election_timings_changed_notify.notified(),
                    )
                };

                let now = chrono::Utc::now().timestamp() as u64;

                match elections_state {
                    ElectionsState::NotStarted { start_time } => {
                        let staking = staking.clone();
                        let action = async move {
                            tokio::time::sleep(Duration::from_secs(
                                (start_time as u64).wrapping_sub(now),
                            ))
                            .await;

                            if let Err(e) = staking.start_election().await {
                                log::error!("Failed to start election: {:?}", e);
                            }
                        };

                        tokio::select! {
                            _ = action => continue,
                            _ = elections_start_fut => {
                                log::warn!("Elections loop: cancelling elections start. Already started");
                            }
                            _ = timings_changed_fut => {
                                log::warn!("Elections loop: cancelling elections start. Timings changed");
                            }
                        }
                    }
                    ElectionsState::Started { end_time, .. } => {
                        let staking = staking.clone();
                        let action = async move {
                            tokio::time::sleep(Duration::from_secs(
                                (end_time as u64).wrapping_sub(now),
                            ))
                            .await;

                            if let Err(e) = staking.end_election().await {
                                log::error!("Failed to end election: {:?}", e);
                            }
                        };

                        tokio::select! {
                            _ = action => continue,
                            _ = elections_end_fut => {
                                log::warn!("Elections loop: cancelling elections ending. Already ended");
                            }
                            _ = timings_changed_fut => {
                                log::warn!("Elections loop: cancelling elections ending. Timings changed");
                            }
                        }
                    }
                    ElectionsState::Finished => {
                        log::info!("Elections loop: waiting new round");
                        new_round_fut.await
                    }
                }
            }
        });
    }

    async fn become_relay_next_round(&self) -> Result<()> {
        self.context
            .deliver_message(
                self.user_data_observer.clone(),
                UnsignedMessage::new(
                    user_data_contract::become_relay_next_round(),
                    self.user_data_account,
                ),
                true,
            )
            .await
    }

    async fn get_reward_for_relay_round(&self, relay_round: u32) -> Result<()> {
        self.context
            .deliver_message(
                self.user_data_observer.clone(),
                UnsignedMessage::new(
                    user_data_contract::get_reward_for_relay_round(),
                    self.user_data_account,
                )
                .arg(relay_round),
                true,
            )
            .await
    }

    async fn start_election(&self) -> Result<()> {
        self.context
            .deliver_message(
                self.staking_observer.clone(),
                UnsignedMessage::new(
                    staking_contract::start_election_on_new_round(),
                    self.staking_account,
                ),
                false,
            )
            .await
    }

    async fn end_election(&self) -> Result<()> {
        self.context
            .deliver_message(
                self.staking_observer.clone(),
                UnsignedMessage::new(staking_contract::end_election(), self.staking_account),
                false,
            )
            .await
    }

    async fn collect_relay_round_state(&self) -> Result<RoundState> {
        let context = &self.context;
        let staking_contract = context
            .ton_subscriber
            .wait_contract_state(self.staking_account)
            .await?;
        StakingContract(&staking_contract)
            .collect_relay_round_state(context)
            .await
    }
}

impl StakingContract<'_> {
    async fn get_eth_bridge_configuration_details(
        &self,
        context: &EngineContext,
    ) -> Result<EthEventConfigurationDetails> {
        let details = self.get_details()?;
        let configuration_contract = context
            .ton_subscriber
            .wait_contract_state(details.bridge_event_config_eth_ton)
            .await?;
        EthEventConfigurationContract(&configuration_contract).get_details()
    }

    async fn collect_relay_round_state(&self, context: &EngineContext) -> Result<RoundState> {
        async fn check_is_elected(
            context: &EngineContext,
            elections_account: UInt256,
        ) -> Result<bool> {
            let elections_contract = context
                .ton_subscriber
                .wait_contract_state(elections_account)
                .await?;
            let elected = ElectionsContract(&elections_contract)
                .staker_addrs()
                .context("Failed to get staker addresses")?
                .contains(&context.staker_address);
            Ok(elected)
        }

        let relay_config = self
            .get_relay_config()
            .context("Failed to get relay config")?;

        let relay_rounds_details = self
            .get_relay_rounds_details()
            .context("Failed to get relay_rounds_details")?;
        log::info!("Relay round details: {:?}", relay_rounds_details);

        let next_elections_account = self
            .get_election_address(relay_rounds_details.current_relay_round + 1)
            .context("Failed to get election address")?;
        log::info!("next_elections_account: {:x}", next_elections_account);

        let now = chrono::Utc::now().timestamp() as u32;
        let (elections_state, elected) = match relay_rounds_details.current_election_start_time {
            0 if relay_rounds_details.current_election_ended => {
                log::info!("Elections were already finished");
                (
                    ElectionsState::Finished,
                    check_is_elected(context, next_elections_account).await?,
                )
            }
            0 => {
                log::info!("Elections were not started yet");
                (
                    ElectionsState::NotStarted {
                        start_time: relay_rounds_details.current_relay_round_start_time
                            + relay_config.time_before_election,
                    },
                    false,
                )
            }
            start_time => {
                log::info!("Elections already started");
                if start_time > now {
                    log::info!("Waiting elections");
                    tokio::time::sleep(
                        Duration::from_secs(start_time as u64)
                            .saturating_sub(Duration::from_secs(now as u64)),
                    )
                    .await;
                }

                (
                    ElectionsState::Started {
                        start_time,
                        end_time: start_time + relay_config.election_time,
                    },
                    check_is_elected(context, next_elections_account).await?,
                )
            }
        };

        Ok(RoundState {
            number: relay_rounds_details.current_relay_round,
            round_start_time: relay_rounds_details.current_relay_round_start_time,
            round_end_time: relay_rounds_details.current_relay_round_end_time,
            elections_state,
            elected,
            relay_config,
        })
    }
}

#[derive(Debug, Clone)]
struct RoundState {
    number: u32,
    round_start_time: u32,
    round_end_time: u32,
    elections_state: ElectionsState,
    elected: bool,
    relay_config: RelayConfigDetails,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
enum ElectionsState {
    NotStarted { start_time: u32 },
    Started { start_time: u32, end_time: u32 },
    Finished,
}

macro_rules! parse_tokens {
    ($res:expr,$fun:expr, $body:expr, $matched:expr) => {
        match $fun
            .decode_input($body)
            .and_then(|tokens| tokens.unpack().map_err(anyhow::Error::from))
        {
            Ok(parsed) => $res = Some($matched(parsed)),
            Err(e) => {
                log::error!("Failed to parse staking event: {:?}", e);
            }
        }
    };
}

#[derive(Debug)]
enum StakingEvent {
    ElectionStarted(ElectionStartedEvent),
    ElectionEnded(ElectionEndedEvent),
    RelayRoundInitialized(RelayRoundInitializedEvent),
    RelayConfigUpdated(RelayConfigUpdatedEvent),
}

impl ReadFromTransaction for StakingEvent {
    fn read_from_transaction(ctx: &TxContext<'_>) -> Option<Self> {
        let start = staking_contract::events::election_started();
        let end = staking_contract::events::election_ended();
        let round_init = staking_contract::events::relay_round_initialized();
        let config_updated = staking_contract::events::relay_config_updated();

        let mut res = None;
        ctx.iterate_events(|id, body| {
            if id == start.id {
                parse_tokens!(res, start, body, StakingEvent::ElectionStarted);
            } else if id == end.id {
                parse_tokens!(res, end, body, StakingEvent::ElectionEnded);
            } else if id == round_init.id {
                parse_tokens!(res, round_init, body, StakingEvent::RelayRoundInitialized);
            } else if id == config_updated.id {
                parse_tokens!(res, config_updated, body, StakingEvent::RelayConfigUpdated);
            }
        });
        res
    }
}

#[derive(Debug)]
enum UserDataEvent {
    RelayMembershipRequested(RelayMembershipRequestedEvent),
}

impl ReadFromTransaction for UserDataEvent {
    fn read_from_transaction(ctx: &TxContext<'_>) -> Option<Self> {
        let membership_requested = user_data_contract::events::relay_membership_requested();

        let mut res = None;
        ctx.iterate_events(|id, body| {
            if id == membership_requested.id {
                parse_tokens!(
                    res,
                    membership_requested,
                    body,
                    UserDataEvent::RelayMembershipRequested
                );
            }
        });
        res
    }
}

#[derive(thiserror::Error, Debug)]
enum StakingError {
    #[error("Required ETH network not found")]
    RequiredEthNetworkNotFound,
    #[error("UserData ETH address mismatch")]
    UserDataEthAddressMismatch,
    #[error("UserData TON public key mismatch")]
    UserDataTonPublicKeyMismatch,
}
