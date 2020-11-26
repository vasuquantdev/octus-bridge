use ton_abi::Function;
use ton_block::{
    CommonMsgInfo, Deserializable, ExternalInboundMessageHeader, Message, Transaction,
};

use super::errors::*;
use crate::models::*;
use crate::prelude::*;
use crate::transport::AccountEvent;

pub fn encode_external_message(message: ExternalMessage) -> Message {
    let mut message_header = ExternalInboundMessageHeader::default();
    message_header.dst = message.dest.clone();

    let mut msg = Message::with_ext_in_header(message_header);
    if let Some(body) = message.body {
        msg.set_body(body);
    }
    msg
}

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

pub struct MessageProcessingParams<'a> {
    pub abi_function: Option<&'a Function>,
    pub events_tx: Option<&'a watch::Sender<AccountEvent>>,
}

pub fn process_out_messages<'a>(
    messages: &'a [Message],
    params: MessageProcessingParams<'a>,
) -> TransportResult<ContractOutput> {
    let mut output = None;

    for msg in messages {
        if !matches!(msg.header(), CommonMsgInfo::ExtOutMsgInfo(_)) {
            continue;
        }

        let body = msg.body().ok_or_else(|| TransportError::ExecutionError {
            reason: "output message has not body".to_string(),
        })?;

        match (&params.abi_function, &params.events_tx) {
            (Some(abi_function), _) if output.is_none() => {
                if abi_function
                    .is_my_output_message(body.clone(), false)
                    .map_err(|e| TransportError::ExecutionError {
                        reason: e.to_string(),
                    })?
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
            }
            (_, Some(events_tx)) => {
                let _ = events_tx.broadcast(AccountEvent::OutboundEvent(Arc::new(body)));
            }
            _ => {}
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
