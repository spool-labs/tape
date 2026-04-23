use thiserror::Error;

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

    #[error("no active cassette — run `tape use <path>` or pass `--cassette <path>`")]
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

pub type Result<T> = std::result::Result<T, Error>;
