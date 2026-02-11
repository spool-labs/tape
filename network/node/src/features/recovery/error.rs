use tape_store::error::TapeStoreError;
use tape_store::types::Pubkey;

#[derive(Debug, thiserror::Error)]
pub enum RecoveryError {
    #[error("storage error: {0}")]
    Storage(#[from] TapeStoreError),

    #[error("no committee members available")]
    NoCommittee,

    #[error("not enough helpers: needed {needed}, available {available}")]
    NotEnoughHelpers { needed: usize, available: usize },

    #[error("repair failed: {0}")]
    RepairFailed(String),

    #[error("skipped (already have enough slices)")]
    Skipped,

    #[error("node client error: {0}")]
    NodeClient(String),

    #[error("slicer error: {0}")]
    Slicer(String),

    #[error("merkle proof verification failed for position {position}")]
    InvalidProof { position: usize },

    #[error("inconsistency detected for track {track}")]
    InconsistencyProof { track: Pubkey },
}
