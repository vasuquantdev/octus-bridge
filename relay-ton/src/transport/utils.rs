use ton_abi::Function;
use ton_block::{
    CommonMsgInfo, Deserializable, ExternalInboundMessageHeader, Message, Transaction,
};

use super::errors::*;
use crate::models::*;
use crate::prelude::*;

pub struct PendingMessage<T> {
    data: T,
    abi: Arc<Function>,
    tx: Option<oneshot::Sender<TransportResult<ContractOutput>>>,
}

impl<T> PendingMessage<T> {
    pub fn new(
        data: T,
        abi: Arc<Function>,
        tx: oneshot::Sender<TransportResult<ContractOutput>>,
    ) -> Self {
        Self {
            data,
            abi,
            tx: Some(tx),
        }
    }

    pub fn abi(&self) -> &Function {
        self.abi.as_ref()
    }

    pub fn data(&self) -> &T {
        &self.data
    }

    pub fn set_result(mut self, result: TransportResult<ContractOutput>) {
        if let Some(tx) = self.tx.take() {
            let _ = tx.send(result);
        }
    }
}

impl<T> Drop for PendingMessage<T> {
    fn drop(&mut self) {
        if let Some(tx) = self.tx.take() {
            let _ = tx.send(Err(TransportError::MessageUnreached));
        }
    }
}

pub trait ExecutableMessage {
    fn dest(&self) -> &MsgAddressInt;
    fn encode(self) -> ton_block::Message;
}

impl ExecutableMessage for ExternalMessage {
    fn dest(&self) -> &MsgAddressInt {
        &self.dest
    }

    fn encode(self) -> Message {
        let message_header = ExternalInboundMessageHeader {
            dst: self.dest.clone(),
            ..Default::default()
        };

        let mut msg = Message::with_ext_in_header(message_header);
        if let Some(body) = self.body {
            msg.set_body(body);
        }
        msg
    }
}

impl ExecutableMessage for InternalMessage {
    fn dest(&self) -> &MsgAddressInt {
        &self.dest
    }

    fn encode(self) -> Message {
        let message_header = ton_block::InternalMessageHeader::with_addresses(
            self.header.src,
            self.dest,
            self.header.value.into(),
        );

        let mut msg = Message::with_int_header(message_header);
        if let Some(body) = self.body {
            msg.set_body(body);
        }
        msg
    }
}

#[allow(dead_code)]
pub fn parse_transaction(raw: &[u8]) -> TransportResult<(Transaction, UInt256)> {
    let cell =
        ton_types::deserialize_tree_of_cells(&mut std::io::Cursor::new(raw)).map_err(|e| {
            TransportError::FailedToParseTransaction {
                reason: e.to_string(),
            }
        })?;
    let hash = cell.hash(0);

    Transaction::construct_from(&mut cell.into())
        .map(|transaction| (transaction, hash))
        .map_err(|e| TransportError::FailedToParseAccountState {
            reason: e.to_string(),
        })
}

pub fn parse_transaction_messages(transaction: &Transaction) -> TransportResult<Vec<Message>> {
    let mut messages = Vec::new();
    transaction
        .out_msgs
        .iterate_slices(|slice| {
            if let Ok(message) = slice.reference(0).and_then(Message::construct_from_cell) {
                messages.push(message);
            }
            Ok(true)
        })
        .map_err(|e| TransportError::FailedToParseTransaction {
            reason: e.to_string(),
        })?;
    Ok(messages)
}

pub struct MessageProcessingParams<'a, T> {
    pub event_transaction: &'a UInt256,
    pub event_transaction_lt: u64,
    pub abi_function: Option<&'a Function>,
    pub events_tx: Option<&'a EventsTx<T>>,
}

pub fn process_out_messages<'a, T>(
    messages: &'a [Message],
    params: MessageProcessingParams<'a, T>,
) -> TransportResult<ContractOutput>
where
    T: PrepareEvent,
{
    let mut output = None;

    for (i, msg) in messages.iter().enumerate() {
        if !matches!(msg.header(), CommonMsgInfo::ExtOutMsgInfo(_)) {
            continue;
        }

        let body = msg.body().ok_or_else(|| TransportError::ExecutionError {
            reason: "output message has not body".to_string(),
        })?;

        match (&params.abi_function, &params.events_tx) {
            (Some(abi_function), _)
                if output.is_none()
                    && abi_function
                        .is_my_output_message(body.clone(), false)
                        .map_err(|e| TransportError::ExecutionError {
                            reason: e.to_string(),
                        })? =>
            {
                let tokens = abi_function.decode_output(body, false).map_err(|e| {
                    TransportError::ExecutionError {
                        reason: e.to_string(),
                    }
                })?;

                output = Some(ContractOutput {
                    transaction_id: None,
                    tokens,
                });
            }
            (_, Some(events_tx)) => {
                let _ = events_tx.send(<T as PrepareEvent>::prepare_event(
                    body,
                    params.event_transaction,
                    params.event_transaction_lt,
                    i as u32,
                ));
            }
            _ => {
                log::debug!("Unknown");
            }
        }
    }

    match (params.abi_function, output) {
        (Some(abi_function), _) if !abi_function.has_output() => Ok(Default::default()),
        (Some(_), Some(output)) => Ok(output),
        (None, _) => Ok(Default::default()),
        _ => Err(TransportError::ExecutionError {
            reason: "no external output messages".to_owned(),
        }),
    }
}

pub trait PrepareEvent: Sized + Send + Sync + 'static {
    fn prepare_event(
        event_data: SliceData,
        event_transaction: &UInt256,
        event_transaction_lt: u64,
        event_index: u32,
    ) -> Self;
}

impl PrepareEvent for SliceData {
    fn prepare_event(
        event_data: SliceData,
        _event_transaction: &UInt256,
        _event_transaction_lt: u64,
        _event_index: u32,
    ) -> Self {
        event_data
    }
}

impl PrepareEvent for FullEventInfo {
    fn prepare_event(
        event_data: SliceData,
        event_transaction: &UInt256,
        event_transaction_lt: u64,
        event_index: u32,
    ) -> Self {
        Self {
            event_transaction: event_transaction.clone(),
            event_transaction_lt,
            event_index,
            event_data,
        }
    }
}
