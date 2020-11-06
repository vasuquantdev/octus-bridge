mod bridge;
mod errors;

mod prelude {
    use std::collections::HashMap;

    use chrono::Utc;
    use ton_abi::{Contract, Function, Token, TokenValue};
    use ton_block::{ExternalInboundMessageHeader, Message, MsgAddressInt, Serializable};

    use super::errors::*;
    use crate::models::*;
    use ton_types::{UInt256, BuilderData};

    #[derive(Debug, Clone)]
    pub struct DefaultHeader {
        pub time: u64,
        pub expire: u32,
    }

    impl From<DefaultHeader> for HashMap<String, TokenValue> {
        fn from(header: DefaultHeader) -> Self {
            let mut result = HashMap::with_capacity(2);
            result.insert("time".to_string(), TokenValue::Time(header.time));
            result.insert("expire".to_string(), TokenValue::Expire(header.expire));
            result
        }
    }

    pub fn make_header(timeout_sec: u32) -> DefaultHeader {
        let time = Utc::now().timestamp_millis() as u64;
        let expire = ((time / 1000) + timeout_sec) as u32;
        DefaultHeader {
            time,
            expire,
        }
    }

    #[derive(Debug, Clone)]
    pub struct ContractConfig {
        pub account: MsgAddressInt,
        pub timeout_sec: u32,
    }

    pub struct MessageBuilder<'a> {
        config: &'a ContractConfig,
        function: &'a Function,
        input: Vec<Token>,
        run_local: bool,
    }

    impl<'a> MessageBuilder<'a> {
        pub fn empty(
            config: &'a ContractConfig,
            contract: &'a Contract,
            name: &str,
        ) -> ContractResult<Self> {
            Self::with_args(config, contract, name, 0)
        }

        pub fn with_args(
            config: &'a ContractConfig,
            contract: &'a Contract,
            name: &str,
            arg_count: usize,
        ) -> ContractResult<Self> {
            Ok(Self {
                config,
                function: contract.function(name).map_err(ContractError::InvalidAbi)?,
                input: Vec::with_capacity(arg_count),
            })
        }

        pub fn arg<T>(mut self, name: &str, value: T) -> Self
            where T: FunctionArg
        {
            self.input.push(Token::new(name, value.token_value()));
            self
        }

        pub fn build(self) -> ContractResult<ExternalMessage> {
            let header = make_header(self.config.timeout_sec);
            let encoded_input = self
                .function
                .encode_input(&header, &self.input, false, None)
                .map_err(|_| ContractError::InvalidInput)?;

            let mut message_header = ExternalInboundMessageHeader::default();
            message_header.dst = self.config.account.clone();

            let mut msg = Message::with_ext_in_header(message_header);
            msg.set_body(encoded_input.into());

            let cells = msg
                .write_to_new_cell()
                .map_err(|_| ContractError::FailedToSerialize)?
                .into();

            let serialized =
                ton_types::serialize_toc(cells).map_err(|_| ContractError::FailedToSerialize)?;
            let _hash = cells.repr_hash();

            Ok(ExternalMessage {
                dest: self.config.account.clone(),
                init: None,
                body: Some(encoded_input.into()),
                expires_in: 0,
            })
        }
    }

    impl FunctionArg for &str {
        fn token_value(self) -> TokenValue {
            TokenValue::Bytes(self.as_bytes().into())
        }
    }

    impl FunctionArg for AccountId {
        fn token_value(self) -> TokenValue {
            TokenValue::Address(self.into())
        }
    }

    impl FunctionArg for UInt256 {
        fn token_value(self) -> TokenValue {
            TokenValue::Uint(self.into())
        }
    }

    impl FunctionArg for BuilderData {
        fn token_value(self) -> TokenValue {
            TokenValue::Cell(self.into())
        }
    }

    impl FunctionArg for TokenValue {
        fn token_value(self) -> TokenValue {
            self
        }
    }

    pub trait FunctionArg {
        fn token_value(self) -> TokenValue;
    }

    impl<T> From<T> for TokenValue
        where T: FunctionArg
    {
        fn from(value: T) -> Self {
            value.token_value()
        }
    }
}
