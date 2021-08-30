use std::sync::Arc;

use anyhow::Result;
use nekoton_abi::*;
use parking_lot::RwLock;
use tokio::sync::mpsc;
use ton_types::UInt256;

use crate::engine::ton_contracts::*;
use crate::engine::ton_subscriber::*;
use crate::engine::EngineContext;
use crate::utils::*;

pub struct Staking {
    context: Arc<EngineContext>,
    staking_observer: Arc<StakingObserver>,
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

    fn start_listening_staking_events(self: &Arc<Self>, mut staking_events_rx: StakingEventsRx) {
        let staking = Arc::downgrade(self);

        tokio::spawn(async move {
            while let Some(event) = staking_events_rx.recv().await {
                let staking = match staking.upgrade() {
                    Some(staking) => staking,
                    None => break,
                };

                // let shard_accounts = match staking.get_all_shard_accounts().await {
                //     Ok(shard_accounts) => shard_accounts,
                //     Err(e) => {
                //         log::error!("Failed to get shard accounts: {:?}", e);
                //         continue;
                //     }
                // };
                //
                // if let Err(e) = update_vote_state(
                //     &shard_accounts,
                //     &event.round_addr,
                //     &mut staking.vote_state.write().new_round,
                // ) {
                //     log::error!("Failed to update vote state: {:?}", e);
                //     continue;
                // }
                //
                // let vote_state = staking.vote_state.clone();
                // let deadline = Instant::now()
                //     + Duration::from_secs(
                //         (event.round_start_time - Utc::now().timestamp() as u32) as u64,
                //     );
                // run_vote_state_timer(deadline, vote_state);
            }

            staking_events_rx.close();
            while staking_events_rx.recv().await.is_some() {}
        });
    }

    // async fn init_vote_state(&self) -> Result<()> {
    //     let shard_accounts = self.get_all_shard_accounts().await?;
    //
    //     let contract = shard_accounts
    //         .find_account(&self.staking_account)?
    //         .ok_or(EngineError::StakingAccountNotFound)?;
    //     let staking = StakingContract(&contract);
    //
    //     let round_num = staking.current_relay_round()?;
    //     let current_time = Utc::now().timestamp() as u128;
    //     let current_relay_round_start_time = staking.current_relay_round_start_time()?;
    //
    //     let current_relay_round_addr;
    //     if current_time < current_relay_round_start_time {
    //         current_relay_round_addr = staking.get_relay_round_address(round_num - 1)?;
    //
    //         let new_relay_round_addr = staking.get_relay_round_address(round_num)?;
    //         update_vote_state(
    //             &shard_accounts,
    //             &new_relay_round_addr,
    //             &mut self.vote_state.write().new_round,
    //         )?;
    //
    //         let vote_state = self.vote_state.clone();
    //         let deadline = Instant::now()
    //             + Duration::from_secs((current_relay_round_start_time - current_time) as u64);
    //         run_vote_state_timer(deadline, vote_state);
    //     } else {
    //         current_relay_round_addr = staking.get_relay_round_address(round_num)?;
    //     }
    //     update_vote_state(
    //         &shard_accounts,
    //         &current_relay_round_addr,
    //         &mut self.vote_state.write().current_round,
    //     )?;
    //
    //     Ok(())
    // }
}

// /// Timer task that switch round state
// fn run_vote_state_timer(deadline: Instant, vote_state: Arc<RwLock<VoteState>>) {
//     tokio::spawn(async move {
//         sleep_until(deadline).await;
//
//         vote_state.write().current_round = vote_state.read().new_round;
//         vote_state.write().new_round = VoteStateType::None;
//     });
// }
//
// fn update_vote_state(
//     shard_accounts: &ShardAccountsMap,
//     addr: &UInt256,
//     vote_state: &mut VoteStateType,
// ) -> Result<()> {
//     // Test key
//     let relay_pubkey =
//         UInt256::from_str("e4e82dd4c0df20b0467b1cf48320f4921796c6c8f76ff5999f91ad9175186635")
//             .unwrap();
//
//     let contract = shard_accounts
//         .find_account(addr)?
//         .ok_or(EngineError::RelayRoundAccountNotFound)?;
//     let relay_round = RelayRoundContract(&contract);
//
//     if relay_round.relay_keys()?.items.contains(&relay_pubkey) {
//         *vote_state = VoteStateType::InRound;
//     } else {
//         *vote_state = VoteStateType::NotInRound;
//     }
//
//     Ok(())
// }

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
struct StakingObserver(StakingEventsTx);

impl TransactionsSubscription for StakingObserver {
    fn handle_transaction(&self, ctx: TxContext<'_>) -> Result<()> {
        let relay_round_initialized = staking_contract::events::relay_round_initialized();

        // Parse all outgoing messages and search proper event
        let mut event: Option<RelayRoundInitializedEvent> = None;
        ctx.iterate_events(|id, body| {
            if id == relay_round_initialized.id {
                match relay_round_initialized
                    .decode_input(body)
                    .and_then(|tokens| tokens.unpack().map_err(anyhow::Error::from))
                {
                    Ok(parsed) => event = Some(parsed),
                    Err(e) => {
                        log::error!("Failed to parse staking event: {:?}", e);
                    }
                }
            }
        });

        log::info!("Got transaction on staking: {:?}", event);

        // Send event to event manager if it exist
        if let Some(event) = event {
            self.0.send(event).ok();
        }

        // Done
        Ok(())
    }
}

type StakingEventsTx = mpsc::UnboundedSender<RelayRoundInitializedEvent>;
type StakingEventsRx = mpsc::UnboundedReceiver<RelayRoundInitializedEvent>;
