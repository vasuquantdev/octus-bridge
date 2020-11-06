use thiserror::Error;

#[derive(Debug, Error)]
pub enum ContractError {
    #[error("invalid contract ABI")]
    InvalidAbi,
    InvalidInput,
    FailedToSerialize,
    MessageUnreached,
    AccountNotFound,
    ExecutionError,
}

pub type ContractResult<T> = Result<T, ContractError>;
