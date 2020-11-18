use thiserror::Error;

#[derive(Debug, Error)]
pub enum TransportError {
    #[error("failed to initialize. {reason}")]
    FailedToInitialize { reason: String },
    #[error("failed to serialize BOC")]
    FailedToSerialize,
    #[error("api request failed. {reason}")]
    ApiFailure { reason: String },
    #[error("failed to fetch transaction. {reason}")]
    FailedToFetchTransaction { reason: String },
    #[error("failed to parse block. {reason}")]
    FailedToParseBlock { reason: String },
    #[error("failed to fetch block. {reason}")]
    FailedToFetchBlock { reason: String },
    #[error("no latest blocks found")]
    NoBlocksFound,
    #[error("failed to fetch account state. {reason}")]
    FailedToFetchAccountState { reason: String },
    #[error("failed to parse account state. {reason}")]
    FailedToParseAccountState { reason: String },
    #[error("failed to parse transaction. {reason}")]
    FailedToParseTransaction { reason: String },
    #[error("failed to send message. {reason}")]
    FailedToSendMessage { reason: String },
    #[error("message was not found before expiration time")]
    MessageUnreached,
    #[error("account was not found")]
    AccountNotFound,
    #[error("contract execution error. {reason}")]
    ExecutionError { reason: String },
}

pub type TransportResult<T> = Result<T, TransportError>;
