use super::errors::*;
use super::prelude::*;
use crate::prelude::*;
use crate::transport::*;

pub trait Contract: Send + Sync + 'static {
    fn abi(&self) -> &Arc<ton_abi::Contract>;
}

pub trait ContractWithEvents: Contract {
    type Event: TryFrom<(Self::EventKind, Vec<Token>), Error = ContractError> + Send;
    type EventKind: for<'a> TryFrom<&'a str, Error = ContractError> + Send + Copy + Clone;

    fn subscription(&self) -> &Arc<dyn AccountSubscription>;

    fn events_map(&self) -> HashMap<u32, (Self::EventKind, AbiEvent)> {
        self.abi()
            .events()
            .iter()
            .map(|(key, event)| {
                let kind = Self::EventKind::try_from(key.as_str()).unwrap();
                (event.get_id(), (kind, event.clone()))
            })
            .collect::<HashMap<_, _>>()
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

    fn events(self: &Arc<Self>) -> mpsc::UnboundedReceiver<Self::Event> {
        let events_map = self.events_map();

        let mut events_rx = self.subscription().events();
        let this = Arc::downgrade(self);
        let (tx, rx) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            while let Some(event) = events_rx.recv().await {
                let _this = match this.upgrade() {
                    Some(this) => this,
                    _ => return,
                };

                if let AccountEvent::OutboundEvent(body) = event {
                    let event = match Self::parse_event(&events_map, body.as_ref()) {
                        Ok(event) => event,
                        Err(e) => {
                            log::error!("event processing error. {}", e);
                            continue;
                        }
                    };

                    if tx.send(event).is_err() {
                        return;
                    }
                };
            }
        });

        rx
    }
}
