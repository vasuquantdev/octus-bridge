use nekoton_parser::abi::UnpackerError;
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
    #[error("invalid event. {reason}")]
    InvalidEvent { reason: String },
    #[error("transport error")]
    TransportError(#[from] TransportError),
    #[error("unpack token error")]
    UnpackerError(#[from] UnpackerError),
    #[error("invalid eth address")]
    InvalidEthAddress,
}

pub type ContractResult<T> = Result<T, ContractError>;
