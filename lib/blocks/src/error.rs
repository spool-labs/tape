//! Error types for block parsing.

/// Error type for block/transaction parsing.
#[derive(Debug, thiserror::Error)]
pub enum ParseError {
    #[error("invalid instruction data")]
    InvalidData,

    #[error("invalid public key")]
    InvalidPubkey,

    #[error("invalid transaction id")]
    InvalidTxId,

    #[error("missing account: {0}")]
    MissingAccount(&'static str),

    #[error("deserialization failed: {0}")]
    Deserialization(String),

    #[error("event/instruction mismatch: {0}")]
    EventMismatch(&'static str),

    #[error("invalid event data")]
    InvalidEvent,
}
