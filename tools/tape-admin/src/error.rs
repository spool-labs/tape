use tape_api::program::prelude::TapeError;
use thiserror::Error;

/// Extract a typed `TapeError` from an `RpcError` if the underlying cause is a
/// custom program error from the tapedrive program. Returns `None` for all
/// other error shapes (transport, deserialization, non-tape program, etc.).
///
/// Centralizes the one unavoidable string parse so callers match on typed
/// variants instead of hex substrings.
pub fn as_tape_error(error: &rpc::RpcError) -> Option<TapeError> {
    let message = format!("{error:?}");
    let marker = "custom program error: 0x";
    let idx = message.find(marker)?;
    let rest = &message[idx + marker.len()..];
    let hex: String = rest
        .chars()
        .take_while(|c| c.is_ascii_hexdigit())
        .collect();
    if hex.is_empty() {
        return None;
    }
    let code = u32::from_str_radix(&hex, 16).ok()?;
    TapeError::try_from(code).ok()
}

#[derive(Debug, Error)]
pub enum Error {
    #[error("rpc error: {0}")]
    Rpc(#[from] rpc::RpcError),

    #[error("io error at {path}: {source}")]
    Io {
        path: String,
        #[source]
        source: std::io::Error,
    },

    #[error("keypair: {0}")]
    Keypair(String),

    #[error("invalid network address: {0}")]
    Address(String),

    #[error("subprocess failed: {0}")]
    Subprocess(String),

    #[error("bls: {0}")]
    Bls(String),

    #[error("invalid input: {0}")]
    Invalid(String),

    #[error("{0}")]
    Other(String),
}

pub type Result<T> = std::result::Result<T, Error>;
