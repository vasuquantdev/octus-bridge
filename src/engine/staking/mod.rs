use std::sync::Arc;

use anyhow::{Context, Result};
use nekoton_abi::UnpackAbiPlain;
use parking_lot::RwLock;
use tokio::sync::mpsc;
use ton_types::UInt256;

use crate::engine::keystore::UnsignedMessage;
use crate::engine::ton_contracts::staking_contract::events::{
    election_ended, election_started, relay_round_initialized,
};
use crate::engine::ton_contracts::user_data::become_relay_next_round;
use crate::engine::ton_contracts::*;
use crate::engine::ton_subscriber::*;
use crate::engine::EngineContext;
use crate::utils::*;

pub struct Staking {
    context: Arc<EngineContext>,
    staking_observer: Arc<StakingObserver<StakingEvents>>,
    vote_state: Arc<RwLock<VoteState>>,
}

impl Staking {
    pub async fn new(context: Arc<EngineContext>, staking_account: UInt256) -> Result<Arc<Self>> {
        let (staking_events_tx, staking_events_rx) = mpsc::unbounded_channel();

        let staking = Arc::new(Self {
            context,
            staking_observer: Arc::new(StakingObserver(staking_events_tx)),
            vote_state: Arc::new(Default::default()),
        });

        staking.start_listening_staking_events(staking_events_rx);

        todo!()
    }

    fn start_listening_staking_events(
        self: &Arc<Self>,
        mut staking_events_rx: StakingEventsRx<StakingEvents>,
    ) {
        let staking = Arc::downgrade(self);
        let user_data = todo!();
        let pubkey = todo!();
        tokio::spawn(async move {
            while let Some(event) = staking_events_rx.recv().await {
                let staking = match staking.upgrade() {
                    Some(staking) => staking,
                    None => break,
                };
                let shard_accounts = match staking.context.get_all_shard_accounts().await {
                    Ok(shard_accounts) => shard_accounts,
                    Err(e) => {
                        log::error!("Failed to get shard accounts: {:?}", e);
                        continue;
                    }
                };
                match event {
                    StakingEvents::ElectionStarted(a) => {
                        let message = UnsignedMessage::new(become_relay_next_round(), user_data);
                        staking.context.deliver_message(message);
                    }
                    StakingEvents::RelayRoundInitialized(a) => {}
                }
            }

            staking_events_rx.close();
            while staking_events_rx.recv().await.is_some() {}
        });
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

/// Listener for staking transactions
///
/// **Registered only for staking account**
struct StakingObserver<T>(StakingEventsTx<T>);

enum StakingEvents {
    ElectionStarted(ElectionStartedEvent),
    ElectionEnded(ElectionEndedEvent),
    RelayRoundInitialized(RelayRoundInitializedEvent),
}

impl<T> TransactionsSubscription for StakingObserver<T>
where
    T: ReadFromTransaction + std::fmt::Debug + Send + Sync,
{
    fn handle_transaction(&self, ctx: TxContext<'_>) -> Result<()> {
        let event = T::read_from_transaction(&ctx);

        log::info!(
            "Got transaction on account {}: {:?}",
            ctx.account.to_hex_string(),
            event
        );

        // Send event to event manager if it exist
        if let Some(event) = event {
            self.0.send((*ctx.account, event)).ok();
        }

        Ok(())
    }
}

macro_rules! impl_read_from {
    ($contract:expr, $id:ty) => {
        impl ReadFromTransaction for $id {
            fn read_from_transaction(ctx: &TxContext<'_>) -> Option<Self> {
                let event = $contract;
                let mut res = None;
                ctx.iterate_events(|id, body| {
                    if id == event.id {
                        res = match event
                            .decode_input(body)
                            .and_then(|tokens| tokens.unpack().map_err(anyhow::Error::from))
                        {
                            Ok(parsed) => Some(parsed),
                            Err(e) => {
                                log::error!("Failed to parse staking event: {:?}", e);
                                None
                            }
                        };
                        return;
                    }
                });
                res
            }
        }
    };
}

macro_rules! parse_tokens {
    ($res:expr,$fun:expr, $body:expr, $matched:expr) => {
        $res = match $fun
            .decode_input($body)
            .and_then(|tokens| tokens.unpack().map_err(anyhow::Error::from))
        {
            Ok(parsed) => Some($matched(parsed)),
            Err(e) => {
                log::error!("Failed to parse staking event: {:?}", e);
                None
            }
        };
        return;
    };
}

impl ReadFromTransaction for StakingEvents {
    fn read_from_transaction(ctx: &TxContext<'_>) -> Option<Self> {
        let start = election_started();
        let end = election_ended();
        let round_init = relay_round_initialized();
        let mut res = None;
        ctx.iterate_events(|id, body| {
            if id == start.id {
                parse_tokens!(res, start, body, StakingEvents::ElectionStarted);
            } else if id == end.id {
                parse_tokens!(res, end, body, StakingEvents::ElectionEnded);
            } else if id == round_init.id {
                parse_tokens!(res, end, body, StakingEvents::RelayRoundInitialized);
            }
        });
        res
    }
}

impl_read_from!(
    super::ton_contracts::user_data::relay_membership_requested_event(),
    RelayMembershipRequestedEvent
);
type StakingEventsTx<T> = mpsc::UnboundedSender<T>;
type StakingEventsRx<T> = mpsc::UnboundedReceiver<T>;
