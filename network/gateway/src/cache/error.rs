use std::error::Error;
use std::fmt::{self, Display, Formatter};

#[derive(Debug)]
pub enum GatewayCacheError {
    Store(String),
    Codec(String),
    State(String),
}

impl Display for GatewayCacheError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Store(message) => write!(f, "store error: {message}"),
            Self::Codec(message) => write!(f, "codec error: {message}"),
            Self::State(message) => write!(f, "cache state error: {message}"),
        }
    }
}

impl Error for GatewayCacheError {}
