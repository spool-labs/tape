//! Records that cannot be read or written

use std::path::PathBuf;

use tape_crypto::address::Address;
use thiserror::Error;

use crate::types::ids::TrackId;

/// A records operation that failed
#[derive(Debug, Error)]
pub enum RecordsError {
    #[error("opening records {path}: {source}")]
    Open {
        path: PathBuf,
        #[source]
        source: rusqlite::Error,
    },

    #[error("records need WAL journaling, sqlite reports `{0}`")]
    Journal(String),

    #[error("sqlite: {0}")]
    Sqlite(#[from] rusqlite::Error),

    #[error("stored {column} `{value}` is not valid")]
    Corrupt { column: &'static str, value: String },

    #[error("an entry's action does not apply to its target")]
    InvalidEntry,

    #[error("tape {0} is not in the list")]
    UnknownTape(Address),

    #[error("track row {0} is not being written")]
    NotWriting(TrackId),

    #[error("records connection lock is poisoned")]
    Poisoned,

    #[error("records task failed: {0}")]
    Task(String),
}
