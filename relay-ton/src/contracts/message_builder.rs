use ton_abi::{Contract, Function, Token, TokenValue};
use ton_block::MsgAddress;

use super::errors::*;
use crate::models::*;
use crate::prelude::*;
use crate::transport::{AccountSubscription, Transport};

impl From<ExternalMessageHeader> for HashMap<String, TokenValue> {
    fn from(header: ExternalMessageHeader) -> Self {
        let mut result = HashMap::with_capacity(2);
        result.insert("time".to_string(), TokenValue::Time(header.time));
        result.insert("expire".to_string(), TokenValue::Expire(header.expire));
        result
    }
}

pub fn make_header(timeout_sec: u32) -> ExternalMessageHeader {
    let time = Utc::now().timestamp_millis() as u64;
    let expire = ((time / 1000) + timeout_sec as u64) as u32;
    ExternalMessageHeader { time, expire }
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
            function: contract
                .function(name)
                .map_err(|_| ContractError::InvalidAbi)?,
            input: Vec::with_capacity(arg_count),
            run_local: false,
        })
    }

    pub fn arg<T>(mut self, name: &str, value: T) -> Self
    where
        T: FunctionArg,
    {
        self.input.push(Token::new(name, value.token_value()));
        self
    }

    pub fn run_local(mut self) -> Self {
        self.run_local = true;
        self
    }

    pub fn build(self) -> ContractResult<ExternalMessage> {
        let header = make_header(self.config.timeout_sec);
        let encoded_input = self
            .function
            .encode_input(&header.clone().into(), &self.input, false, None)
            .map_err(|_| ContractError::InvalidInput)?;

        Ok(ExternalMessage {
            dest: self.config.account.clone(),
            init: None,
            body: Some(encoded_input.into()),
            header: header.clone(),
            run_local: self.run_local,
        })
    }

    pub async fn send(self, transport: &dyn AccountSubscription) -> ContractResult<ContractOutput> {
        let function = Arc::new(self.function.clone());

        let output = transport.send_message(function, self.build()?).await?;
        Ok(output)
    }
}

impl FunctionArg for &str {
    fn token_value(self) -> TokenValue {
        TokenValue::Bytes(self.as_bytes().into())
    }
}

impl FunctionArg for AccountId {
    fn token_value(self) -> TokenValue {
        TokenValue::Address(MsgAddress::AddrStd(self.into()))
    }
}

impl FunctionArg for UInt256 {
    fn token_value(self) -> TokenValue {
        let number = num_bigint::BigUint::from_bytes_be(self.as_slice());

        TokenValue::Uint(ton_abi::Uint { number, size: 256 })
    }
}

impl FunctionArg for BuilderData {
    fn token_value(self) -> TokenValue {
        TokenValue::Cell(self.into())
    }
}

impl FunctionArg for ton_types::Cell {
    fn token_value(self) -> TokenValue {
        TokenValue::Cell(self)
    }
}

impl FunctionArg for TokenValue {
    fn token_value(self) -> TokenValue {
        self
    }
}

impl<T> FunctionArg for &T
where
    T: Clone + FunctionArg,
{
    fn token_value(self) -> TokenValue {
        self.clone().token_value()
    }
}

pub trait FunctionArg {
    fn token_value(self) -> TokenValue;
}
