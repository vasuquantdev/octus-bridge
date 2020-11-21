use ton_abi::{Contract, Function, Token, TokenValue};
use ton_block::MsgAddress;

use super::errors::*;
use super::prelude::*;
use crate::models::*;
use crate::prelude::*;
use crate::transport::*;

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

pub enum MessageType {
    Signed,
    Unsigned,
}

pub struct MessageBuilder<'a> {
    config: &'a ContractConfig,
    function: &'a Function,
    transport: &'a dyn AccountSubscription,
    keypair: &'a Keypair,
    input: Vec<Token>,
    run_local: bool,
}

impl<'a> MessageBuilder<'a> {
    pub fn new(
        config: &'a ContractConfig,
        contract: &'a Contract,
        transport: &'a dyn AccountSubscription,
        keypair: &'a Keypair,
        name: &str,
    ) -> ContractResult<Self> {
        let function = contract
            .function(name)
            .map_err(|_| ContractError::InvalidAbi)?;
        let input = Vec::with_capacity(function.inputs.len());

        Ok(Self {
            config,
            function,
            transport,
            keypair,
            input,
            run_local: false,
        })
    }

    pub fn arg<T>(mut self, value: T) -> Self
    where
        T: FunctionArg,
    {
        let name = &self.function.inputs[self.input.len()].name;
        self.input.push(Token::new(name, value.token_value()));
        self
    }

    #[allow(dead_code)]
    pub fn mark_local(mut self) -> Self {
        self.run_local = true;
        self
    }

    pub fn build(self, message_type: MessageType) -> ContractResult<ExternalMessage> {
        let header = make_header(self.config.timeout_sec);
        let encoded_input = self
            .function
            .encode_input(
                &header.clone().into(),
                &self.input,
                false,
                match message_type {
                    MessageType::Signed => Some(self.keypair),
                    MessageType::Unsigned => None,
                },
            )
            .map_err(|_| ContractError::InvalidInput)?;

        Ok(ExternalMessage {
            dest: self.config.account.clone(),
            init: None,
            body: Some(encoded_input.into()),
            header,
            run_local: self.run_local,
        })
    }

    pub async fn run_local(self) -> ContractResult<ContractOutput> {
        let output = self
            .transport
            .run_local(self.function, self.build(MessageType::Unsigned)?)
            .await?;
        Ok(output)
    }

    pub async fn send(self) -> ContractResult<ContractOutput> {
        let function = Arc::new(self.function.clone());
        let output = self
            .transport
            .send_message(function, self.build(MessageType::Signed)?)
            .await?;
        Ok(output)
    }
}

impl FunctionArg for &str {
    fn token_value(self) -> TokenValue {
        TokenValue::Bytes(self.as_bytes().into())
    }
}

impl FunctionArg for Vec<u8> {
    fn token_value(self) -> TokenValue {
        TokenValue::Bytes(self)
    }
}

impl FunctionArg for AccountId {
    fn token_value(self) -> TokenValue {
        TokenValue::Address(MsgAddress::AddrStd(self.into()))
    }
}

impl FunctionArg for MsgAddrStd {
    fn token_value(self) -> TokenValue {
        TokenValue::Address(MsgAddress::AddrStd(self))
    }
}

impl FunctionArg for UInt256 {
    fn token_value(self) -> TokenValue {
        num_bigint::BigUint::from_bytes_be(self.as_slice()).token_value()
    }
}

impl FunctionArg for BigUint {
    fn token_value(self) -> TokenValue {
        TokenValue::Uint(ton_abi::Uint {
            number: self,
            size: 256,
        })
    }
}

impl FunctionArg for u8 {
    fn token_value(self) -> TokenValue {
        TokenValue::Uint(ton_abi::Uint {
            number: BigUint::from(self),
            size: 8,
        })
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

impl<T> FunctionArg for Vec<T>
where
    T: StandaloneToken + FunctionArg,
{
    fn token_value(self) -> TokenValue {
        TokenValue::Array(self.into_iter().map(FunctionArg::token_value).collect())
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
