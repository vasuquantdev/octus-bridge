use std::sync::Arc;

use anyhow::{Context, Result};
use nekoton_abi::UnpackAbiPlain;
use parking_lot::{Mutex, RwLock};
use tokio::sync::mpsc;
use ton_types::UInt256;

use crate::engine::keystore::*;
use crate::engine::ton_contracts::*;
use crate::engine::ton_subscriber::*;
use crate::engine::EngineContext;
use crate::utils::*;

pub struct Staking {
    context: Arc<EngineContext>,

    staking_account: UInt256,

    staking_observer: Arc<AccountObserver<StakingEvent>>,
    user_data_observer: Mutex<Option<Arc<AccountObserver<UserDataEvent>>>>,
    vote_state: Arc<RwLock<VoteState>>,
}

impl Staking {
    pub async fn new(context: Arc<EngineContext>, staking_account: UInt256) -> Result<Arc<Self>> {
        let (staking_events_tx, staking_events_rx) = mpsc::unbounded_channel();

        let staking = Arc::new(Self {
            context,
            staking_account,
            staking_observer: AccountObserver::new(&staking_events_tx),
            user_data_observer: Default::default(),
            vote_state: Arc::new(Default::default()),
        });

        start_listening_events(
            &staking,
            "StakingContract",
            staking_events_rx,
            Self::process_staking_event,
        );

        Ok(staking)
    }

    async fn process_staking_event(
        self: Arc<Self>,
        (_, event): (UInt256, StakingEvent),
    ) -> Result<()> {
        match event {
            StakingEvent::ElectionStarted(event) => {
                let message =
                    UnsignedMessage::new(user_data_contract::become_relay_next_round(), todo!());

                let staking = self.clone();
                tokio::spawn(async move {
                    // TODO: replace with user data
                    staking
                        .context
                        .deliver_message(staking.staking_observer.clone(), message);
                });
            }
            StakingEvent::ElectionEnded(event) => {
                log::warn!("Election ended: {:?}", event);
            }
            StakingEvent::RelayRoundInitialized(event) => {
                log::warn!("Relay round initialized: {:?}", event);
                // TODO: handle
            }
        }
        Ok(())
    }

    fn update_vote_state(
        &self,
        pubkey: &UInt256,
        shard_accounts: &ShardAccountsMap,
        addr: &UInt256,
    ) -> Result<()> {
        let contract = shard_accounts.find_account(addr)?.context("lol")?;
        let relay_round = RelayRoundContract(&contract);

        let mut vote_state = self.vote_state.write().new_round;
        if relay_round.relay_keys()?.items.contains(&pubkey) {
            vote_state = VoteStateType::InRound;
        } else {
            vote_state = VoteStateType::NotInRound;
        }

        Ok(())
    }
}

/// Whether Relay is able to vote in each of round
#[derive(Debug, Copy, Clone, Default)]
struct VoteState {
    current_round: VoteStateType,
    new_round: VoteStateType,
}

#[derive(Debug, Copy, Clone)]
enum VoteStateType {
    None,
    InRound,
    NotInRound,
}

impl Default for VoteStateType {
    fn default() -> Self {
        VoteStateType::None
    }
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

type StakingEventsTx<T> = mpsc::UnboundedSender<T>;
type StakingEventsRx<T> = mpsc::UnboundedReceiver<T>;
