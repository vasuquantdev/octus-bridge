use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use nekoton_abi::UnpackAbiPlain;
use parking_lot::{Mutex, RwLock};
use tokio::sync::mpsc;
use tokio::sync::Notify;
use ton_types::UInt256;

use crate::engine::keystore::*;
use crate::engine::ton_contracts::staking_contract::get_relay_round_address;
use crate::engine::ton_contracts::*;
use crate::engine::ton_subscriber::*;
use crate::engine::EngineContext;
use crate::utils::*;

pub struct Staking {
    context: Arc<EngineContext>,

    staking_account: UInt256,
    staking_observer: Arc<AccountObserver<StakingEvent>>,

    user_data_events_tx: AccountEventsTx<UserDataEvent>,
    user_data_observer: Mutex<Option<Arc<AccountObserver<UserDataEvent>>>>,
    user_data: UserData,
    vote_state: Arc<RwLock<VoteState>>,
}

macro_rules! impl_waiter {
    ($name:ident, $ty:ident) => {
        #[derive(Default)]
        struct $name {
            inner: Mutex<$ty>,
            waker: Notify,
            flag: AtomicBool,
        }

        impl UserData {
            fn init(&self, address: UInt256) {
                *self.inner.lock() = address;
                self.waker.notify_one();
                self.flag.store(true, Ordering::Release);
            }
            async fn get(&self) -> UInt256 {
                if self.flag.load(Ordering::Acquire) {
                    *self.inner.lock()
                } else {
                    self.waker.notified().await;
                    *self.inner.lock()
                }
            }
        }
    };
}

#[derive(Default)]
struct UserData {
    inner: Mutex<UInt256>,
    waker: Notify,
    flag: AtomicBool,
}
impl UserData {
    fn init(&self, address: UInt256) {
        *self.inner.lock() = address;
        self.waker.notify_one();
        self.flag.store(true, Ordering::Release);
    }
    async fn get(&self) -> UInt256 {
        if self.flag.load(Ordering::Acquire) {
            *self.inner.lock()
        } else {
            self.waker.notified().await;
            *self.inner.lock()
        }
    }
}

impl Staking {
    pub async fn new(context: Arc<EngineContext>, staking_account: UInt256) -> Result<Arc<Self>> {
        let (staking_events_tx, staking_events_rx) = mpsc::unbounded_channel();
        let (user_data_events_tx, user_data_events_rx) = mpsc::unbounded_channel();

        let staking = Arc::new(Self {
            context,
            staking_account,
            staking_observer: AccountObserver::new(&staking_events_tx),
            user_data_events_tx,
            user_data_observer: Default::default(),
            user_data: Default::default(),
            vote_state: Arc::new(Default::default()),
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

        staking.clone().start_searching_user_data().await?;

        Ok(staking)
    }

    async fn start_searching_user_data(self: Arc<Self>) -> Result<()> {
        let staking_contract = self
            .context
            .ton_subscriber
            .wait_contract_state(self.staking_account)
            .await?;

        let user_data_account = StakingContract(&staking_contract)
            .get_user_data_address(&self.context.staker_address)?;
        log::info!("UserData account: {:x}", user_data_account);

        let staking = Arc::downgrade(&self);

        tokio::spawn(async move {
            log::info!("Searching UserData...");
            loop {
                let staking = match staking.upgrade() {
                    Some(staking) => staking,
                    None => return,
                };
                let ton_subscriber = &staking.context.ton_subscriber;

                let user_data_state =
                    match ton_subscriber.wait_contract_state(user_data_account).await {
                        Ok(contract) => contract,
                        Err(e) => {
                            log::error!("Failed to find UserData: {:?}", e);
                            tokio::time::sleep(Duration::from_secs(1)).await;
                            continue;
                        }
                    };

                log::info!("Found UserData");
                staking.user_data.init(user_data_account);
                break;
            }
        });
        Ok(())
    }

    async fn process_staking_event(
        self: Arc<Self>,
        (_, event): (UInt256, StakingEvent),
    ) -> Result<()> {
        match event {
            StakingEvent::ElectionStarted(event) => {
                let user_data = self.user_data.get().await;
                let message =
                    UnsignedMessage::new(user_data_contract::become_relay_next_round(), user_data);

                let staking = self.clone();
                tokio::spawn(async move {
                    staking
                        .context
                        .deliver_message(staking.staking_observer.clone(), message);
                });
            }
            StakingEvent::ElectionEnded(event) => {
                log::warn!("Election ended: {:?}", event);
                self.update_vote_state(
                    self.context.keystore.ton.public_key(),
                    &self.context.get_all_shard_accounts().await?,
                );
            }
            StakingEvent::RelayRoundInitialized(event) => {
                log::warn!("Relay round initialized: {:?}", event);
                // TODO: handle
            }
        }
        Ok(())
    }

    async fn get_relay_round_address(&self, round: u32) -> Result<UInt256> {
        let getter = get_relay_round_address();
        let shard_map = self.context.get_all_shard_accounts().await?;
        let staking_account: ExistingContract = shard_map
            .find_account(&self.staking_account)?
            .context("Staking account not found")?;
        let staking_account = StakingContract(&staking_account);
        staking_account.get_relay_round_address(round)
    }

    async fn process_user_data_event(
        self: Arc<Self>,
        (_, event): (UInt256, UserDataEvent),
    ) -> Result<()> {
        match event {
            UserDataEvent::RelayMembershipRequested(data) => {
                self.staking_state.applied_round(data.round_num);
            }
        }
        Ok(())
    }

    fn update_vote_state(&self, pubkey: &UInt256, shard_accounts: &ShardAccountsMap) -> Result<()> {
        let contract = shard_accounts
            .find_account(self)?
            .context("Failed getting relay round contract")?;
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
