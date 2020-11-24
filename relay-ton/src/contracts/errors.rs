use thiserror::Error;

use crate::transport::errors::*;

#[derive(Debug, Error)]
pub enum ContractError {
    #[error("invalid address")]
    InvalidAddress,
    #[error("invalid contract ABI")]
    InvalidAbi,
    #[error("invalid contract function input")]
    InvalidInput,
    #[error("invalid string")]
    InvalidString,
    #[error("unknown event")]
    UnknownEvent,
    #[error("transport error")]
    TransportError(#[from] TransportError),
}

pub type ContractResult<T> = Result<T, ContractError>;
