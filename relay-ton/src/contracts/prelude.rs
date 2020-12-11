use num_traits::ToPrimitive;
pub use ton_abi::{Token, TokenValue};
pub use ton_types::Cell;

use super::errors::*;
pub use super::message_builder::{
    BigUint128, BigUint256, FunctionArg, FunctionArgsGroup, MessageBuilder, SignedMessageBuilder,
};
use super::utils::*;
pub use super::{Contract, ContractWithEvents};
use crate::models::*;
use crate::prelude::*;

pub trait IgnoreOutput: Sized {
    fn ignore_output(self) -> Result<(), ContractError> {
        Ok(())
    }
}

impl IgnoreOutput for ContractOutput {}

impl ContractOutput {
    pub fn into_parser(self) -> ContractOutputParser<impl Iterator<Item = Token>> {
        ContractOutputParser(self.tokens.into_iter())
    }

    pub fn parse_first<T>(self) -> ContractResult<T>
    where
        TokenValue: ParseToken<T>,
    {
        self.into_parser().parse_next()
    }

    pub fn parse_all<T>(self) -> ContractResult<T>
    where
        T: TryFrom<Self, Error = ContractError>,
    {
        self.try_into()
    }

    pub fn hash(&self) -> ContractResult<UInt256> {
        pack_tokens(&self.tokens).map(|data| data.hash(0))
    }
}

pub struct ContractOutputParser<I>(I);

impl<I: Iterator<Item = Token>> ContractOutputParser<I> {
    pub fn parse_next<T>(&mut self) -> ContractResult<T>
    where
        TokenValue: ParseToken<T>,
    {
        self.0.next().try_parse()
    }
}

pub trait ParseToken<T> {
    fn try_parse(self) -> ContractResult<T>;
}

impl ParseToken<MsgAddrStd> for TokenValue {
    fn try_parse(self) -> ContractResult<MsgAddrStd> {
        match self {
            TokenValue::Address(ton_block::MsgAddress::AddrStd(address)) => Ok(address),
            _ => Err(ContractError::InvalidAbi),
        }
    }
}

impl ParseToken<Cell> for TokenValue {
    fn try_parse(self) -> ContractResult<Cell> {
        match self {
            TokenValue::Cell(cell) => Ok(cell),
            _ => Err(ContractError::InvalidAbi),
        }
    }
}

impl ParseToken<Vec<u8>> for TokenValue {
    fn try_parse(self) -> ContractResult<Vec<u8>> {
        match self {
            TokenValue::Bytes(bytes) => Ok(bytes),
            _ => Err(ContractError::InvalidAbi),
        }
    }
}

impl ParseToken<BigUint> for TokenValue {
    fn try_parse(self) -> ContractResult<BigUint> {
        match self {
            TokenValue::Uint(data) => Ok(data.number),
            _ => Err(ContractError::InvalidAbi),
        }
    }
}

impl ParseToken<UInt256> for TokenValue {
    fn try_parse(self) -> ContractResult<UInt256> {
        match self {
            TokenValue::Uint(data) => Ok(data.number.to_bytes_be().into()),
            _ => Err(ContractError::InvalidAbi),
        }
    }
}

impl ParseToken<UInt128> for TokenValue {
    fn try_parse(self) -> ContractResult<UInt128> {
        match self {
            TokenValue::Uint(data) => Ok(data.number.to_bytes_be().into()),
            _ => Err(ContractError::InvalidAbi),
        }
    }
}

impl ParseToken<u8> for TokenValue {
    fn try_parse(self) -> ContractResult<u8> {
        ParseToken::<BigUint>::try_parse(self)?
            .to_u8()
            .ok_or(ContractError::InvalidAbi)
    }
}

impl ParseToken<bool> for TokenValue {
    fn try_parse(self) -> ContractResult<bool> {
        match self {
            TokenValue::Bool(confirmed) => Ok(confirmed),
            _ => Err(ContractError::InvalidAbi),
        }
    }
}

impl<T> ParseToken<T> for Option<Token>
where
    TokenValue: ParseToken<T>,
{
    fn try_parse(self) -> ContractResult<T> {
        match self {
            Some(token) => token.value.try_parse(),
            None => Err(ContractError::InvalidAbi),
        }
    }
}

impl<T> ParseToken<T> for Option<TokenValue>
where
    TokenValue: ParseToken<T>,
{
    fn try_parse(self) -> ContractResult<T> {
        match self {
            Some(value) => value.try_parse(),
            None => Err(ContractError::InvalidAbi),
        }
    }
}

impl<T> ParseToken<Vec<T>> for TokenValue
where
    T: StandaloneToken,
    TokenValue: ParseToken<T>,
{
    fn try_parse(self) -> ContractResult<Vec<T>> {
        match self {
            TokenValue::Array(tokens) | TokenValue::FixedArray(tokens) => tokens,
            _ => return Err(ContractError::InvalidAbi),
        }
        .into_iter()
        .map(ParseToken::try_parse)
        .collect()
    }
}

impl<T> ParseToken<T> for Token
where
    TokenValue: ParseToken<T>,
{
    fn try_parse(self) -> ContractResult<T> {
        self.value.try_parse()
    }
}

pub trait StandaloneToken {}
impl StandaloneToken for MsgAddrStd {}
impl StandaloneToken for ethereum_types::Address {}
impl StandaloneToken for AccountId {}
impl StandaloneToken for UInt256 {}
impl StandaloneToken for UInt128 {}
impl StandaloneToken for BigUint {}
impl StandaloneToken for bool {}
impl StandaloneToken for Vec<u8> {}

pub trait ReadMethodId {
    type Error;

    fn read_method_id(&self) -> Result<u32, Self::Error>;
}

impl ReadMethodId for SliceData {
    type Error = failure::Error;

    fn read_method_id(&self) -> Result<u32, Self::Error> {
        let mut value: u32 = 0;
        for i in 0..4 {
            value |= (self.get_byte(8 * i)? as u32) << (8 * (3 - i));
        }
        Ok(value)
    }
}

#[macro_export]
macro_rules! define_event{
    ($event_name:ident, $event_kind:ident, { $($name:ident $({ $($data:tt)* })?),+$(,)? }) => {
        #[derive(Debug, Clone)]
        pub enum $event_name {
            $($name $({ $($data)* })?),+,
        }

        #[derive(Debug, Clone, Copy, Ord, PartialOrd, Eq, PartialEq, Hash)]
        pub enum $event_kind {
            $($name),+,
        }

        impl $event_kind {
            pub fn name(&self) -> &'static str {
                match self {
                    $(<$event_kind>::$name => stringify!($name)),*,
                }
            }
        }

        impl std::convert::TryFrom<&str> for $event_kind {
            type Error = ContractError;

            fn try_from(s: &str) -> ContractResult<Self> {
                match s {
                    $(stringify!($name) => Ok(Self::$name)),*,
                    _ => Err(ContractError::InvalidAbi)
                }
            }
        }
    };
}
