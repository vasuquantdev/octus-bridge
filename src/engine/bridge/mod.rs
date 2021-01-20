mod eth_events_handler;
mod event_transport;
mod semaphore;
mod ton_events_handler;

use relay_eth::ws::{EthListener, SyncedHeight};
use relay_ton::contracts::*;
use relay_ton::transport::*;

use crate::config::RelayConfig;
use crate::crypto::key_managment::*;
use crate::db::*;
use crate::models::*;
use crate::prelude::*;

use self::eth_events_handler::*;
use self::event_transport::*;
use self::semaphore::*;
use self::ton_events_handler::*;

mod prelude;
mod utils;

pub async fn make_bridge(
    db: Db,
    config: RelayConfig,
    key_data: KeyData,
) -> Result<Arc<Bridge>, Error> {
    let ton_transport = config.ton_settings.transport.make_transport().await?;

    let ton_contract_address =
        MsgAddressInt::from_str(&*config.ton_settings.bridge_contract_address.0)
            .map_err(|e| Error::msg(e.to_string()))?;

    let relay_contract_address =
        MsgAddressInt::from_str(&*config.ton_settings.relay_contract_address.0)
            .map_err(|e| Error::msg(e.to_string()))
            .and_then(|address| match address {
                MsgAddressInt::AddrStd(addr) => Ok(addr),
                MsgAddressInt::AddrVar(_) => Err(anyhow!("Unsupported relay address")),
            })?;

    let (bridge_contract, bridge_contract_events) =
        make_bridge_contract(ton_transport.clone(), ton_contract_address).await?;

    let relay_contract = make_relay_contract(
        ton_transport.clone(),
        relay_contract_address,
        key_data.ton.keypair(),
        bridge_contract,
    )
    .await?;

    let eth_listener = Arc::new(
        EthListener::new(
            Url::parse(&config.eth_settings.node_address)
                .map_err(|e| Error::new(e).context("Bad url for eth_config provided"))?,
            db.clone(),
            config.eth_settings.tcp_connection_count,
        )
        .await?,
    );

    let eth_signer = key_data.eth.clone();
    let eth_queue = EthQueue::new(&db)?;
    let scanning_state = ScanningState::new(&db)?;

    let ton = Arc::new(
        EventTransport::new(
            &db,
            ton_transport.clone(),
            scanning_state.clone(),
            relay_contract.clone(),
            config.ton_settings.clone(),
        )
        .await?,
    );
    let eth = Arc::new(
        EventTransport::new(
            &db,
            ton_transport.clone(),
            scanning_state.clone(),
            relay_contract.clone(),
            config.ton_settings.clone(),
        )
        .await?,
    );

    let bridge = Arc::new(Bridge {
        eth_listener,
        ton_transport,
        relay_contract,
        eth_signer,
        eth_queue,
        scanning_state,
        ton,
        eth,
        configs_state: Arc::new(Default::default()),
    });

    tokio::spawn({
        let bridge = bridge.clone();
        async move { bridge.run(bridge_contract_events).await }
    });

    Ok(bridge)
}

pub struct Bridge {
    eth_listener: Arc<EthListener>,

    ton_transport: Arc<dyn Transport>,
    relay_contract: Arc<RelayContract>,

    eth_signer: EthSigner,
    eth_queue: EthQueue,
    scanning_state: ScanningState,

    ton: Arc<EventTransport<TonEventConfigurationContract>>,
    eth: Arc<EventTransport<EthEventConfigurationContract>>,

    configs_state: Arc<RwLock<ConfigsState>>,
}

impl Bridge {
    async fn run<T>(self: Arc<Self>, mut bridge_contract_events: T) -> Result<(), Error>
    where
        T: Stream<Item = BridgeContractEvent> + Send + Unpin + 'static,
    {
        log::info!(
            "Bridge started. Relay account: {}",
            self.relay_contract.address()
        );

        // Subscribe to bridge events
        tokio::spawn({
            let bridge = self.clone();

            async move {
                while let Some(event) = bridge_contract_events.next().await {
                    match event {
                        BridgeContractEvent::EventConfigurationCreationEnd {
                            id,
                            address,
                            active: true,
                            event_type: EventType::ETH,
                        } => {
                            tokio::spawn(
                                bridge
                                    .clone()
                                    .subscribe_to_eth_events_configuration(id, address, None),
                            );
                        }
                        BridgeContractEvent::EventConfigurationCreationEnd {
                            id,
                            address,
                            active: true,
                            event_type: EventType::TON,
                        } => {
                            tokio::spawn(
                                bridge
                                    .clone()
                                    .subscribe_to_ton_events_configuration(id, address),
                            );
                        }
                        _ => {
                            // TODO: handle other events
                        }
                    }
                }
            }
        });

        // Get all configs before now
        let known_contracts = self
            .relay_contract
            .bridge()
            .get_active_event_configurations()
            .await
            .expect("Failed to get known event configurations"); // TODO: is it really a fatal error?

        // Wait for all existing configuration contracts subscriptions to start ETH part properly
        let semaphore = Semaphore::new(known_contracts.len());
        for active_configuration in known_contracts.into_iter() {
            match active_configuration.event_type {
                EventType::ETH => {
                    tokio::spawn(self.clone().subscribe_to_eth_events_configuration(
                        active_configuration.id,
                        active_configuration.address,
                        Some(semaphore.clone()),
                    ));
                }
                EventType::TON => {
                    tokio::spawn(self.clone().subscribe_to_ton_events_configuration(
                        active_configuration.id,
                        active_configuration.address,
                    ));
                    semaphore.notify().await;
                }
            }
        }

        // Wait until all initial subscriptions done
        log::trace!("waiting semaphore");
        semaphore.wait().await;
        log::trace!("semaphore done");

        // Restart sending for all enqueued confirmations
        self.eth.retry_pending();

        // Subscribe for ETH blocks and events
        let mut eth_events_rx = self.eth_listener.start().await?;

        // Spawn pending confirmations queue processing
        tokio::spawn(self.clone().watch_pending_confirmations());

        // Enqueue new events from ETH
        while let Some(event) = eth_events_rx.next().await {
            let event: relay_eth::ws::Event = match event {
                Ok(event) => event,
                Err(e) => {
                    log::error!("Failed parsing data from ethereum stream: {:?}", e);
                    continue;
                }
            };

            tokio::spawn(self.clone().process_eth_event(event));
        }

        // Done
        Ok(())
    }

    ///Sets eth height
    pub async fn change_eth_height(&self, height: u64) -> Result<(), Error> {
        let actual_height = self.eth_listener.get_synced_height().await?.as_u64();
        if actual_height < height {
            return Err(anyhow::anyhow!(
                "Height provided by user is higher, then actual eth height. Cowardly refusing"
            ));
        }
        self.eth_listener.change_eth_height(height)?;
        Ok(())
    }

    /// Restart voting for failed transactions
    pub fn retry_failed(&self) {
        self.eth.retry_failed();
        self.ton.retry_failed();
    }

    pub fn ton_relay_address(&self) -> MsgAddrStd {
        self.relay_contract.address().clone()
    }

    pub fn eth_pubkey(&self) -> secp256k1::PublicKey {
        self.eth_signer.pubkey()
    }

    pub async fn get_event_configurations(
        &self,
    ) -> Result<Vec<(BigUint, EthEventConfiguration)>, anyhow::Error> {
        let state = self.configs_state.read().await;
        Ok(state.eth_configs_map.values().cloned().collect())
    }

    pub async fn vote_for_ethereum_event_configuration(
        &self,
        configuration_id: BigUint,
        voting: Voting,
    ) -> Result<(), anyhow::Error> {
        self.relay_contract
            .vote_for_event_configuration_creation(configuration_id, voting)
            .await?;
        Ok(())
    }

    fn check_suspicious_event(self: Arc<Self>, event: EthEventVotingData) {
        async fn check_event(
            configs: &ConfigsState,
            check_result: Result<(Address, Vec<u8>), Error>,
            event: &EthEventVotingData,
        ) -> Result<(), Error> {
            let (address, data) = check_result?;
            match configs.address_topic_map.get(&address) {
                None => Err(anyhow!(
                    "We have no info about {} to get abi. Rejecting transaction",
                    address
                )),
                Some((_, eth_abi, ton_abi)) => {
                    // Decode event data
                    let got_tokens: Vec<ethabi::Token> = utils::parse_eth_event_data(
                        &eth_abi,
                        &ton_abi,
                        event.event_data.clone(),
                    )
                    .map_err(|e| e.context("Failed decoding other relay data as eth types"))?;

                    let expected_tokens = ethabi::decode(eth_abi, &data).map_err(|e| {
                        Error::from(e).context(
                            "Can not verify data, that other relay sent. Assuming it's fake.",
                        )
                    })?;

                    if got_tokens == expected_tokens {
                        Ok(())
                    } else {
                        Err(anyhow!(
                            "Decoded tokens are not equal with that other relay sent"
                        ))
                    }
                }
            }
        }

        tokio::spawn(async {
            let configs = self.configs_state.read().await.clone();
            let eth_listener = self.eth_listener.clone();
            async move {
                let check_result = eth_listener
                    .check_transaction(event.event_transaction)
                    .await;
                if let Err(e) = {
                    match check_event(&configs, check_result, &event).await {
                        Ok(_) => {
                            log::info!("Confirming transaction. Hash: {}", event.event_transaction);
                            self.eth
                                .enqueue_vote(EventTransaction::Confirm(event))
                                .await
                        }
                        Err(e) => {
                            log::warn!("Rejection: {:?}", e);
                            self.eth.enqueue_vote(EventTransaction::Reject(event)).await
                        }
                    }
                } {
                    log::error!("Critical error while spawning vote: {:?}", e)
                }
            }
            .await
        });
    }

    async fn watch_pending_confirmations(self: Arc<Self>) {
        log::debug!("Started watch_unsent_eth_ton_transactions");
        loop {
            let synced_block = match self.eth_listener.get_synced_height().await {
                Ok(a) => a,
                Err(e) => {
                    log::error!("CRITICAL error: {}", e);
                    continue;
                }
            };
            log::debug!("New block: {:?}", synced_block);

            let prepared_blocks = self
                .eth_queue
                .get_prepared_blocks(synced_block.as_u64())
                .await;
            for (entry, event) in prepared_blocks {
                let block_number = event.event_block_number;
                log::debug!(
                    "Found unconfirmed data in block {}: {}",
                    block_number,
                    hex::encode(&event.event_transaction)
                );
                self.clone().check_suspicious_event(event);
                entry.remove().expect("Fatal db error");
            }
            if let SyncedHeight::Synced(a) = synced_block {
                let bad_blocks = self.eth_queue.get_bad_blocks(a).await;
                for (entry, event) in bad_blocks {
                    let block_number = event.event_block_number;
                    log::debug!(
                        "Found suspicious data in block {}: {}",
                        block_number,
                        hex::encode(&event.event_transaction)
                    );
                    self.clone().check_suspicious_event(event);
                    entry.remove().expect("Fatal db error");
                }
            }
            tokio::time::delay_for(Duration::from_secs(10)).await;
        }
    }

    async fn process_eth_event(self: Arc<Self>, event: relay_eth::ws::Event) {
        log::info!(
            "Received event from address: {}. Tx hash: {}.",
            &event.address,
            &event.tx_hash
        );

        // Extend event info
        let (configuration_id, ethereum_event_blocks_to_confirm, topic_tokens) = {
            let state = self.configs_state.read().await;

            // Find suitable event configuration
            let (config_addr, event_config) = match state.eth_configs_map.get(&event.address) {
                Some(data) => data,
                None => {
                    log::error!("FATAL ERROR. Failed mapping event_configuration with address");
                    return;
                }
            };

            // Decode event data
            let decoded_data: Option<Result<Vec<ethabi::Token>, _>> = event
                .topics
                .iter()
                .map(|topic_id| state.topic_abi_map.get(topic_id))
                .filter_map(|x| x)
                .map(|x| ethabi::decode(x, &event.data))
                // Taking first element, cause topics and abi shouldn't overlap more than once
                .next();

            let topic_tokens = match decoded_data {
                Some(a) => match a {
                    Ok(a) => a,
                    Err(e) => {
                        log::error!("Failed decoding data from event: {}", e);
                        return;
                    }
                },
                None => {
                    log::error!("No data from event could be parsed");
                    return;
                }
            };

            (
                config_addr.clone(),
                event_config
                    .event_blocks_to_confirm
                    .to_u64()
                    .unwrap_or_else(u64::max_value),
                topic_tokens,
            )
        };

        // Prepare confirmation

        let ton_data: Vec<_> = topic_tokens
            .into_iter()
            .map(utils::map_eth_to_ton)
            .collect();
        let event_data = match utils::pack_token_values(ton_data) {
            Ok(a) => a,
            Err(e) => {
                log::error!("Failed mapping ton_data to cell: {:?}", e);
                return;
            }
        };

        let prepared_data = EthEventVotingData {
            event_transaction: event.tx_hash,
            event_index: event.event_index,
            event_data,
            event_block_number: event.block_number,
            event_block: event.block_hash,
            configuration_id,
        };

        let target_block_number = event.block_number + ethereum_event_blocks_to_confirm;

        log::info!(
            "Inserting transaction for block {} with queue number: {}",
            event.block_number,
            target_block_number
        );
        self.eth_queue
            .insert(target_block_number, &prepared_data)
            .await
            .expect("Fatal db error");
    }

    /// Creates a listener for TON event votes in TON and its target contract
    async fn subscribe_to_ton_events_configuration(
        self: Arc<Self>,
        configuration_id: BigUint,
        address: MsgAddressInt,
    ) {
        let _handler = match TonEventsHandler::new(
            self.ton.clone(),
            self.eth_signer.clone(),
            configuration_id,
            address,
        )
        .await
        {
            Ok(handler) => handler,
            Err(e) => {
                log::error!("Failed to subscribe to TON events configuration: {:?}", e);
                return;
            }
        };

        // TODO: insert to map
        future::pending().await
    }

    /// Creates a listener for ETH event votes in TON
    async fn subscribe_to_eth_events_configuration(
        self: Arc<Self>,
        configuration_id: BigUint,
        address: MsgAddressInt,
        semaphore: Option<Semaphore>,
    ) {
        let handler = match EthEventsHandler::uninit(
            self.eth.clone(),
            self.eth_queue.clone(),
            configuration_id.clone(),
            address,
        )
        .await
        {
            Ok(handler) => handler,
            Err(e) => {
                log::error!("Failed to subscribe to ETH events configuration: {:?}", e);
                return;
            }
        };

        // Insert configuration and start listening ETH events
        self.subscribe_to_eth_topic(configuration_id, handler.details())
            .await;

        let _handler = handler.start().await;

        log::trace!("notifying semaphore: {}", semaphore.is_some());
        semaphore.try_notify().await;

        // TODO: insert to map
        future::pending().await
    }

    /// Registers topic for specified address in ETH
    async fn subscribe_to_eth_topic(
        &self,
        configuration_id: BigUint,
        details: &EthEventConfiguration,
    ) {
        let mut configs_state = self.configs_state.write().await;
        configs_state.set_configuration(configuration_id, details);

        if let Some((topic, _, _)) = configs_state.address_topic_map.get(&details.event_address) {
            self.eth_listener
                .add_topic(details.event_address, *topic)
                .await;
        }
    }

    /// Removes topic from specified address in ETH
    async fn unsubscribe_from_eth_topic(&self, details: &EthEventConfiguration) {
        let mut configs_state = self.configs_state.write().await;
        configs_state.remove_event_address(&details.event_address);

        self.eth_listener
            .unsubscribe_from_address(&details.event_address)
            .await;
    }
}

#[derive(Debug, Clone, Default)]
pub struct ConfigsState {
    pub eth_addr: HashSet<Address>,
    pub address_topic_map: HashMap<Address, EthTopicItem>,
    pub topic_abi_map: HashMap<H256, Vec<ethabi::ParamType>>,
    pub eth_configs_map: HashMap<Address, (BigUint, EthEventConfiguration)>,
}

type EthTopicItem = (H256, Vec<ethabi::ParamType>, Vec<ton_abi::ParamType>);

impl ConfigsState {
    fn set_configuration(
        &mut self,
        configuration_id: BigUint,
        configuration: &EthEventConfiguration,
    ) {
        let (topic_hash, eth_abi, ton_abi) =
            match utils::parse_eth_abi(&configuration.common.event_abi) {
                Ok(a) => a,
                Err(e) => {
                    log::error!("Failed parsing abi: {:?}", e);
                    return;
                }
            };

        self.eth_addr.insert(configuration.event_address.clone());
        self.address_topic_map.insert(
            configuration.event_address.clone(),
            (topic_hash, eth_abi.clone(), ton_abi),
        );
        self.topic_abi_map.insert(topic_hash, eth_abi);
        self.eth_configs_map.insert(
            configuration.event_address,
            (configuration_id, configuration.clone()),
        );
    }

    fn remove_event_address(&mut self, event_address: &Address) {
        self.eth_addr.remove(event_address);
        self.address_topic_map.remove(event_address);
        self.eth_configs_map.remove(event_address);
    }
}

#[async_trait]
impl ConfigurationContract for EthEventConfigurationContract {
    type Details = EthEventConfiguration;
    type EventContract = EthEventContract;
    type ReceivedVote = EthEventReceivedVote;
    type EventTransaction = EthEventTransaction;

    async fn make_config_contract(
        transport: Arc<dyn Transport>,
        account: MsgAddressInt,
        bridge_address: MsgAddressInt,
    ) -> ContractResult<(Arc<Self>, EventsRx<<Self as ContractWithEvents>::Event>)> {
        make_eth_event_configuration_contract(transport, account, bridge_address).await
    }

    async fn make_event_contract(transport: Arc<dyn Transport>) -> Arc<Self::EventContract> {
        make_eth_event_contract(transport).await
    }

    fn make_voting_stats(
        db: &Db,
    ) -> Result<VotingStats<<Self::ReceivedVote as ReceivedVote>::VoteWithData>, Error> {
        EthVotingStats::new(db)
    }

    fn make_votes_queue(db: &Db) -> Result<VotesQueue<Self::EventTransaction>, Error> {
        EthEventVotesQueue::new(db)
    }

    fn address(&self) -> &MsgAddressInt {
        self.address()
    }

    fn validate(&self, details: &Self::Details) -> Result<(), Error> {
        utils::validate_ethereum_event_configuration(details)
    }

    async fn compute_event_address(
        &self,
        init_data: <Self::EventContract as EventContract>::InitData,
    ) -> ContractResult<MsgAddrStd> {
        self.compute_event_address(init_data).await
    }

    async fn get_details(&self) -> ContractResult<Self::Details> {
        self.get_details().await
    }
}

#[async_trait]
impl ConfigurationContract for TonEventConfigurationContract {
    type Details = TonEventConfiguration;
    type EventContract = TonEventContract;
    type ReceivedVote = TonEventReceivedVote;
    type EventTransaction = TonEventTransaction;

    async fn make_config_contract(
        transport: Arc<dyn Transport>,
        account: MsgAddressInt,
        bridge_address: MsgAddressInt,
    ) -> ContractResult<(Arc<Self>, EventsRx<<Self as ContractWithEvents>::Event>)> {
        make_ton_event_configuration_contract(transport, account, bridge_address).await
    }

    async fn make_event_contract(transport: Arc<dyn Transport>) -> Arc<Self::EventContract> {
        make_ton_event_contract(transport).await
    }

    fn make_voting_stats(
        db: &Db,
    ) -> Result<VotingStats<<Self::ReceivedVote as ReceivedVote>::VoteWithData>, Error> {
        TonVotingStats::new(db)
    }

    fn make_votes_queue(db: &Db) -> Result<VotesQueue<Self::EventTransaction>, Error> {
        TonEventVotesQueue::new(db)
    }

    fn address(&self) -> &MsgAddressInt {
        self.address()
    }

    fn validate(&self, config: &Self::Details) -> Result<(), Error> {
        let _ = serde_json::from_str::<SwapBackEventAbi>(&config.common.event_abi)
            .map_err(|e| Error::new(e).context("Bad SwapBack event ABI"))?;
        Ok(())
    }

    async fn compute_event_address(
        &self,
        init_data: <Self::EventContract as EventContract>::InitData,
    ) -> ContractResult<MsgAddrStd> {
        self.compute_event_address(init_data).await
    }

    async fn get_details(&self) -> ContractResult<Self::Details> {
        self.get_details().await
    }
}

#[async_trait]
impl EventContract for EthEventContract {
    type Details = EthEventDetails;
    type InitData = EthEventInitData;

    async fn get_details(&self, address: &MsgAddrStd) -> ContractResult<Self::Details> {
        self.get_details(address.clone()).await
    }
}

#[async_trait]
impl EventContract for TonEventContract {
    type Details = TonEventDetails;
    type InitData = TonEventInitData;

    async fn get_details(&self, address: &MsgAddrStd) -> ContractResult<Self::Details> {
        self.get_details(address.clone()).await
    }
}

fn make_confirmed_ton_event_transaction(
    event_id: u32,
    configuration_id: BigUint,
    event: SwapBackEvent,
    signature: Vec<u8>,
) -> Result<TonEventTransaction, Error> {
    let event_data = utils::pack_event_data_into_cell(event_id, &event.tokens)?;

    Ok(TonEventTransaction::Confirm(SignedEventVotingData {
        data: TonEventVotingData {
            event_transaction: event.event_transaction,
            event_transaction_lt: event.event_transaction_lt,
            event_index: event.event_index,
            event_data,
            configuration_id,
        },
        signature,
    }))
}

#[async_trait]
impl EventTransactionExt for EthEventTransaction {
    type InitData = <EthEventContract as EventContract>::InitData;

    #[inline]
    fn configuration_id(&self) -> &BigUint {
        match self {
            Self::Confirm(data) => &data.configuration_id,
            Self::Reject(data) => &data.configuration_id,
        }
    }

    #[inline]
    fn kind(&self) -> Voting {
        match self {
            Self::Confirm(_) => Voting::Confirm,
            Self::Reject(_) => Voting::Reject,
        }
    }

    fn init_data(&self) -> Self::InitData {
        match self.clone() {
            Self::Confirm(data) => data.into(),
            Self::Reject(data) => data.into(),
        }
    }

    async fn send(&self, bridge: Arc<RelayContract>) -> ContractResult<()> {
        let id = self.configuration_id().clone();
        let data = self.init_data();
        match self.kind() {
            Voting::Confirm => bridge.confirm_ethereum_event(id, data).await,
            Voting::Reject => bridge.reject_ethereum_event(id, data).await,
        }
    }
}

impl std::fmt::Display for EthEventTransaction {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let transaction = match self {
            Self::Confirm(data) => &data.event_transaction,
            Self::Reject(data) => &data.event_transaction,
        };
        f.write_fmt(format_args!("Vote for ETH transaction {}", transaction))
    }
}

#[async_trait]
impl EventTransactionExt for TonEventTransaction {
    type InitData = <TonEventContract as EventContract>::InitData;

    #[inline]
    fn configuration_id(&self) -> &BigUint {
        match self {
            Self::Confirm(signed) => &signed.data.configuration_id,
            Self::Reject(data) => &data.configuration_id,
        }
    }

    #[inline]
    fn kind(&self) -> Voting {
        match self {
            Self::Confirm(_) => Voting::Confirm,
            Self::Reject(_) => Voting::Reject,
        }
    }

    fn init_data(&self) -> Self::InitData {
        match self {
            Self::Confirm(signed) => signed.data.clone().into(),
            Self::Reject(data) => data.clone().into(),
        }
    }

    async fn send(&self, bridge: Arc<RelayContract>) -> ContractResult<()> {
        match self.clone() {
            Self::Confirm(SignedEventVotingData { data, signature }) => {
                let id = data.configuration_id.clone();
                let data = data.into();
                bridge.confirm_ton_event(id, data, signature).await
            }
            Self::Reject(data) => {
                let id = data.configuration_id.clone();
                let data = data.into();
                bridge.reject_ton_event(id, data).await
            }
        }
    }
}

impl std::fmt::Display for TonEventTransaction {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let transaction = match self {
            Self::Confirm(signed) => &signed.data.event_transaction,
            Self::Reject(data) => &data.event_transaction,
        };
        f.write_fmt(format_args!(
            "Vote for TON transaction {}",
            hex::encode(transaction.as_slice())
        ))
    }
}
