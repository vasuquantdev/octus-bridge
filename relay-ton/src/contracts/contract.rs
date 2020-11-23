use super::errors::*;
use super::prelude::*;
use crate::prelude::*;
use crate::transport::*;

pub trait Contract: Send + Sync + 'static {
    type Event: TryFrom<(Self::EventKind, Vec<Token>), Error = ContractError> + Send;
    type EventKind: for<'a> TryFrom<&'a str, Error = ContractError> + Send + Copy + Clone;

    fn contract(&self) -> &Arc<ton_abi::Contract>;

    fn transport(&self) -> &Arc<dyn AccountSubscription>;

    fn message(&self, name: &str) -> ContractResult<MessageBuilder>;

    fn events(self: &Arc<Self>) -> mpsc::UnboundedReceiver<Self::Event> {
        let contract = self.contract().clone();

        let mut events_rx = self.transport().events();
        let this = Arc::downgrade(self);
        let (tx, rx) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            let events_map = contract
                .events()
                .iter()
                .map(|(key, event)| {
                    let kind = Self::EventKind::try_from(key.as_str()).unwrap();
                    (event.get_id(), (kind, event))
                })
                .collect::<HashMap<_, _>>();

            while let Some(event) = events_rx.recv().await {
                let _this = match this.upgrade() {
                    Some(this) => this,
                    _ => return,
                };

                if let AccountEvent::OutboundEvent(body) = event {
                    let event_id = match body.read_method_id() {
                        Ok(id) => id,
                        Err(_) => {
                            log::error!("got invalid event body");
                            continue;
                        }
                    };

                    let (kind, event) = match events_map.get(&event_id) {
                        Some(&info) => info,
                        None => {
                            log::error!("got unknown event");
                            continue;
                        }
                    };

                    let data = match event
                        .decode_input(body.as_ref().clone())
                        .map_err(|_| ContractError::InvalidInput)
                        .and_then(move |data| Self::Event::try_from((kind, data)))
                    {
                        Ok(tokens) => tokens,
                        Err(e) => {
                            log::error!("failed to decode event. {}", e);
                            continue;
                        }
                    };

                    if tx.send(data).is_err() {
                        return;
                    }
                };
            }
        });

        rx
    }
}
