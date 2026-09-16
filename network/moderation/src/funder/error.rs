//! Upkeep that could not be done

use tape_crypto::address::Address;
use thiserror::Error;

use crate::chain::error::ChainError;
use crate::publisher::error::PublishError;
use crate::records::error::RecordsError;

/// A funder run, rack creation or new blacklist tape that failed
#[derive(Debug, Error)]
pub enum FunderError {
    #[error(transparent)]
    Records(#[from] RecordsError),

    #[error(transparent)]
    Chain(#[from] ChainError),

    #[error(transparent)]
    Publish(#[from] PublishError),

    #[error("no reserved blacklist rack; run create-rack first")]
    NoRack,

    #[error("the list already has rack {0}")]
    AlreadyCreated(Address),

    #[error("the rack or its first blacklist tape is not declared and named yet; run create-rack again")]
    Unfinished,

    #[error("key task failed: {0}")]
    Task(String),
}
