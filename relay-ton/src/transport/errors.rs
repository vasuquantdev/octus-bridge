use thiserror::Error;

#[derive(Debug, Error)]
pub enum TransportError {}

pub type TransportResult<T> = Result<T, TransportError>;
