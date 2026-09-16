//! Config that cannot be used

use std::io;
use std::path::PathBuf;

use thiserror::Error;

/// A config file or secret that cannot be used
#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("reading {path}: {source}")]
    Read {
        path: PathBuf,
        #[source]
        source: io::Error,
    },

    #[error("parsing {path}: {source}")]
    Parse {
        path: PathBuf,
        #[source]
        source: serde_yaml::Error,
    },

    #[error("secret file {0} is empty")]
    EmptySecret(PathBuf),

    #[error("invalid config: {0}")]
    Invalid(String),
}
