use ton_types::UInt256;

pub trait ReadFromTransaction: Sized {
    fn read_from_transaction(ctx: &TxContext<'_>) -> Option<Self>;
}

#[derive(Copy, Clone)]
pub struct TxContext<'a> {
    pub shard_accounts: &'a ton_block::ShardAccounts,
    pub block_info: &'a ton_block::BlockInfo,
    pub account: &'a UInt256,
    pub transaction_hash: &'a UInt256,
    pub transaction_info: &'a ton_block::TransactionDescrOrdinary,
    pub transaction: &'a ton_block::Transaction,
}

impl TxContext<'_> {
    pub fn in_msg(&self) -> Option<ton_block::Message> {
        match self
            .transaction
            .in_msg
            .as_ref()
            .map(|message| message.read_struct())
        {
            Some(Ok(message)) => Some(message),
            _ => None,
        }
    }

    pub fn in_msg_internal(&self) -> Option<ton_block::Message> {
        self.in_msg()
            .filter(|message| matches!(message.header(), ton_block::CommonMsgInfo::IntMsgInfo(_)))
    }

    pub fn in_msg_external(&self) -> Option<ton_block::Message> {
        self.in_msg()
            .filter(|message| matches!(message.header(), ton_block::CommonMsgInfo::ExtInMsgInfo(_)))
    }

    pub fn find_function_output(
        &self,
        function: &ton_abi::Function,
    ) -> Option<Vec<ton_abi::Token>> {
        let mut result = None;
        self.transaction
            .out_msgs
            .iterate(|ton_block::InRefValue(message)| {
                // Skip all messages except external outgoing
                if !matches!(message.header(), ton_block::CommonMsgInfo::ExtOutMsgInfo(_)) {
                    return Ok(true);
                }

                // Handle body if it exists
                let body = match message.body() {
                    Some(body) => body,
                    None => return Ok(true),
                };

                let function_id = nekoton_abi::read_function_id(&body)?;
                if function_id != function.output_id {
                    return Ok(true);
                }

                Ok(match function.decode_output(body, false) {
                    Ok(tokens) => {
                        result = Some(tokens);
                        false
                    }
                    Err(_) => true,
                })
            })
            .ok();
        result
    }

    pub fn iterate_events<F>(&self, mut f: F)
    where
        F: FnMut(u32, ton_types::SliceData),
    {
        self.transaction
            .out_msgs
            .iterate(|ton_block::InRefValue(message)| {
                // Skip all messages except external outgoing
                if !matches!(message.header(), ton_block::CommonMsgInfo::ExtOutMsgInfo(_)) {
                    return Ok(true);
                }

                // Handle body if it exists
                let body = match message.body() {
                    Some(body) => body,
                    None => return Ok(true),
                };

                // Parse function id
                if let Ok(function_id) = nekoton_abi::read_function_id(&body) {
                    f(function_id, body)
                }

                // Process all messages
                Ok(true)
            })
            .ok();
    }
}
