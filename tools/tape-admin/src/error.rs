use thiserror::Error;

// Re-export the shared typed-error extractor so existing callers in this
// crate don't need to change their import path.
pub use tape_cli_common::tape_error::{as_tape_error, is_already_initialized_runtime};

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

    #[error("config: {0}")]
    Config(String),

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

impl From<tape_cli_common::ConfigError> for Error {
    fn from(e: tape_cli_common::ConfigError) -> Self {
        Error::Config(e.to_string())
    }
}

pub type Result<T> = std::result::Result<T, Error>;
