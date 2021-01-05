use super::errors::*;
use super::prelude::*;
use crate::prelude::*;

pub trait Contract: Send + Sync + 'static {
    fn abi(&self) -> &Arc<ton_abi::Contract>;
}

pub trait ContractWithEvents: Contract + Sized {
    type Event: TryFrom<(Self::EventKind, Vec<Token>), Error = ContractError> + Send;
    type EventKind: for<'a> TryFrom<&'a str, Error = ContractError> + Send + Copy;

    fn events_map(&self) -> HashMap<u32, (Self::EventKind, AbiEvent)> {
        make_events_map::<Self>(self.abi())
    }

    fn parse_event(
        events_map: &HashMap<u32, (Self::EventKind, AbiEvent)>,
        body: &SliceData,
    ) -> ContractResult<Self::Event> {
        let event_id = body
            .read_method_id()
            .map_err(|e| ContractError::InvalidEvent {
                reason: e.to_string(),
            })?;

        let (kind, event) = events_map
            .get(&event_id)
            .ok_or(ContractError::UnknownEvent)?;

        event
            .decode_input(body.clone())
            .map_err(|e| ContractError::InvalidEvent {
                reason: e.to_string(),
            })
            .and_then(move |data| Self::Event::try_from((*kind, data)))
    }
}

pub fn start_processing_events<T>(
    contract: &Arc<T>,
    mut events_rx: RawEventsRx,
) -> EventsRx<T::Event>
where
    T: ContractWithEvents,
{
    let events_map = contract.events_map();

    let this = Arc::downgrade(contract);
    let (tx, rx) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        while let Some(body) = events_rx.next().await {
            let _this = match this.upgrade() {
                Some(this) => this,
                _ => return,
            };

            let event = match T::parse_event(&events_map, &body) {
                Ok(event) => event,
                Err(e) => {
                    log::error!("event processing error. {:?}", e);
                    continue;
                }
            };

            if tx.send(event).is_err() {
                return;
            }
        }
    });

    rx
}

pub fn make_events_map<T>(abi: &AbiContract) -> HashMap<u32, (T::EventKind, AbiEvent)>
where
    T: ContractWithEvents,
{
    abi.events()
        .iter()
        .map(|(key, event)| {
            let kind = T::EventKind::try_from(key.as_str()).unwrap();
            (event.get_id(), (kind, event.clone()))
        })
        .collect::<HashMap<_, _>>()
}
