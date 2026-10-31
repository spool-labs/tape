//! Errors from reading and writing the block store

use rusqlite::Error as SqlError;
use thiserror::Error;
use tokio::task::JoinError;

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("database error: {0}")]
    Database(#[from] SqlError),

    #[error("a database lock is poisoned")]
    Poisoned,

    #[error("database task failed: {0}")]
    Task(#[from] JoinError),
}

pub type StoreResult<Value> = Result<Value, StoreError>;
