use thiserror::Error;

use crate::transport::errors::*;

#[derive(Debug, Error)]
pub enum ContractError {
    #[error("invalid contract ABI")]
    InvalidAbi,
    #[error("invalid contract function input")]
    InvalidInput,
    #[error("transport error")]
    TransportError(#[from] TransportError),
}

pub type ContractResult<T> = Result<T, ContractError>;
