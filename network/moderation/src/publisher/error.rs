//! Tracks that could not be written or deleted

use tape_blacklist::error::BlacklistError;
use tape_core::types::TrackNumber;
use tape_crypto::address::Address;
use tape_rack::error::RackError;
use thiserror::Error;

use crate::chain::error::ChainError;
use crate::records::error::RecordsError;
use crate::types::ids::TrackCount;

/// A flush that stopped on an error; the next flush settles what it left against the chain
#[derive(Debug, Error)]
pub enum PublishError {
    #[error(transparent)]
    Records(#[from] RecordsError),

    #[error("rendering blacklist entries: {0}")]
    Entries(#[from] BlacklistError),

    #[error("rendering rack records: {0}")]
    RackRecords(#[from] RackError),

    #[error("reading tape {tape}: {source}")]
    Read {
        tape: Address,
        #[source]
        source: ChainError,
    },

    #[error("writing a track on tape {tape}: {source}")]
    Write {
        tape: Address,
        #[source]
        source: ChainError,
    },

    #[error("deleting track {track}: {source}")]
    Delete {
        track: Address,
        #[source]
        source: ChainError,
    },

    #[error("tape {0} is missing on chain")]
    MissingTape(Address),

    #[error("{track_number} on tape {tape} landed, but no peer holds it yet")]
    NotVisible {
        tape: Address,
        track_number: TrackNumber,
    },

    #[error("tape {tape} holds {chain} tracks, but records account for {recorded}")]
    TrackCount {
        tape: Address,
        recorded: TrackCount,
        chain: TrackCount,
    },

    #[error("the header on tape {tape} landed at {track_number}, not track 0")]
    HeaderNotFirst {
        tape: Address,
        track_number: TrackNumber,
    },

    #[error("rack {0} has no track left for another record")]
    RackFull(Address),

    #[error("rendered tracks do not match what they hold")]
    Mismatch,
}
