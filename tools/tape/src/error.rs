use thiserror::Error;
use tape_cli_common::ConfigError;

#[derive(Debug, Error)]
pub enum Error {
    #[error("io error at {path}: {source}")]
    Io {
        path: String,
        #[source]
        source: std::io::Error,
    },

    #[error("config error: {0}")]
    Config(String),

    #[error("keypair: {0}")]
    Keypair(String),

    #[error("no active tape; run `tape use <path>` or pass `--tape <path>`")]
    NoActiveCassette,

    #[error("sdk: {0}")]
    Sdk(String),

    #[error("rpc: {0}")]
    Rpc(#[from] rpc::RpcError),

    #[error("invalid input: {0}")]
    Invalid(String),

    #[error("{0}")]
    Other(String),
}

impl From<ConfigError> for Error {
    fn from(e: ConfigError) -> Self {
        Error::Config(e.to_string())
    }
}

pub type Result<T> = std::result::Result<T, Error>;
