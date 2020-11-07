use ton_block::{ExternalInboundMessageHeader, Message, Serializable};
use ton_sdk::NodeClient;

use super::errors::*;
use super::Transport;
use crate::models::*;
use crate::prelude::*;

pub struct GraphQlTransport {
    node_client: NodeClient,
}

#[async_trait]
impl Transport for GraphQlTransport {
    async fn get_account_state(&self, addr: &MsgAddrStd) -> TransportResult<Option<AccountState>> {
        todo!()
    }

    async fn send_message(&self, message: ExternalMessage) -> TransportResult<ContractOutput> {
        let mut message_header = ExternalInboundMessageHeader::default();
        message_header.dst = message.dest;

        let mut msg = Message::with_ext_in_header(message_header);
        if let Some(body) = message.body {
            msg.set_body(body);
        }

        let cells = msg
            .write_to_new_cell()
            .map_err(|_| TransportError::FailedToSerialize)?
            .into();

        let serialized =
            ton_types::serialize_toc(&cells).map_err(|_| TransportError::FailedToSerialize)?;
        let hash = cells.repr_hash();

        todo!()
    }
}
