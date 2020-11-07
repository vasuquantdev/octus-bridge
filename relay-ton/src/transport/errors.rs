use thiserror::Error;

#[derive(Debug, Error)]
pub enum TransportError {
    #[error("failed to serialize BOC")]
    FailedToSerialize,
    #[error("message was not found before expiration time")]
    MessageUnreached,
    #[error("account was not found")]
    AccountNotFound,
    #[error("contract execution error")]
    ExecutionError,
}

pub type TransportResult<T> = Result<T, TransportError>;
