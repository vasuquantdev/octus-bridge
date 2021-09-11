use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use nekoton_abi::UnpackAbiPlain;
use tokio::sync::mpsc;
use tokio::sync::Notify;
use ton_types::UInt256;

use crate::engine::keystore::*;
use crate::engine::ton_contracts::*;
use crate::engine::ton_subscriber::*;
use crate::engine::EngineContext;
use crate::utils::*;

pub struct Staking {
    context: Arc<EngineContext>,

    current_relay_round: tokio::sync::Mutex<RoundState>,
    elections_end_notify: Notify,

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

        let user_data_observer = AccountObserver::new(&user_data_events_tx);

        // Initialize relay round
        let current_relay_round = staking_contract.collect_relay_round_state(&context).await?;

        let should_vote = current_relay_round.elections_started && !current_relay_round.elected;

        //
        let staking = Arc::new(Self {
            context,
            current_relay_round: tokio::sync::Mutex::new(current_relay_round),
            elections_end_notify: Default::default(),
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

        Ok(staking)
    }

    async fn process_staking_event(
        self: Arc<Self>,
        (_, event): (UInt256, StakingEvent),
    ) -> Result<()> {
        let staking = self;

        tokio::spawn(async move {
            match event {
                StakingEvent::ElectionStarted(_) => {
                    if let Err(e) = staking.on_election_started().await {
                        log::error!("Failed to handle election started event: {:?}", e);
                    }
                }
                StakingEvent::ElectionEnded(_) => {
                    if let Err(e) = staking.on_election_ended().await {
                        log::error!("Failed to handle election started event: {:?}", e);
                    }
                }
                StakingEvent::RelayRoundInitialized(event) => {
                    log::warn!("Relay round initialized: {:?}", event);
                    // TODO: collect reward
                }
            }
        });

        Ok(())
    }

    async fn on_election_started(self: &Arc<Self>) -> Result<()> {
        let mut current_relay_round = self.current_relay_round.lock().await;

        // Update state
        *current_relay_round = self.collect_relay_round_state().await?;

        // Spawn election voting
        let staking = self.clone();
        tokio::spawn(async move {
            if let Err(e) = staking.become_relay_next_round().await {
                log::error!("Failed to become relay next round: {:?}", e);
            }
        });

        Ok(())
    }

    async fn on_election_ended(&self) -> Result<()> {
        let mut current_relay_round = self.current_relay_round.lock().await;

        // Stop pending election messages
        self.elections_end_notify.notify_waiters();

        // Update state
        *current_relay_round = self.collect_relay_round_state().await?;
        Ok(())
    }

    async fn get_relay_round_address(&self, round: u32) -> Result<UInt256> {
        let staking_contract = self
            .context
            .ton_subscriber
            .wait_contract_state(self.staking_account)
            .await?;
        StakingContract(&staking_contract).get_relay_round_address(round)
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

    async fn become_relay_next_round(&self) -> Result<()> {
        let message = UnsignedMessage::new(
            user_data_contract::become_relay_next_round(),
            self.user_data_account,
        );

        // NOTE: Create `notified` future beforehand
        let notification_fut = self.elections_end_notify.notified();

        let message_fut = self
            .context
            .deliver_message(self.user_data_observer.clone(), message);

        tokio::select! {
            result = message_fut => result,
            _ = notification_fut => {
                log::warn!("Early exit from become_relay_next_round due to the elections end");
                Ok(())
            }
        }
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
        let relay_rounds_details = self.get_relay_rounds_details()?;
        log::info!("Relay round details: {:?}", relay_rounds_details);

        let next_elections_account =
            self.get_election_address(relay_rounds_details.current_relay_round + 1)?;

        let now = chrono::Utc::now().timestamp() as u32;
        let (elections_started, elected) = match relay_rounds_details.current_election_start_time {
            0 => {
                log::info!("Elections were not started yet");
                (false, false)
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

                let elections_contract = context
                    .ton_subscriber
                    .wait_contract_state(next_elections_account)
                    .await?;
                let elected = ElectionsContract(&elections_contract)
                    .staker_addrs()?
                    .contains(&context.staker_address);
                (true, elected)
            }
        };

        Ok(RoundState {
            number: relay_rounds_details.current_relay_round,
            elections_started,
            elected,
        })
    }
}

#[derive(Debug, Copy, Clone)]
struct RoundState {
    number: u32,
    elections_started: bool,
    elected: bool,
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
}

impl ReadFromTransaction for StakingEvent {
    fn read_from_transaction(ctx: &TxContext<'_>) -> Option<Self> {
        let start = staking_contract::events::election_started();
        let end = staking_contract::events::election_ended();
        let round_init = staking_contract::events::relay_round_initialized();

        let mut res = None;
        ctx.iterate_events(|id, body| {
            if id == start.id {
                parse_tokens!(res, start, body, StakingEvent::ElectionStarted);
            } else if id == end.id {
                parse_tokens!(res, end, body, StakingEvent::ElectionEnded);
            } else if id == round_init.id {
                parse_tokens!(res, round_init, body, StakingEvent::RelayRoundInitialized);
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
