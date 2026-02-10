use tape_store::error::TapeStoreError;

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

    #[error("node client error: {0}")]
    NodeClient(String),

    #[error("slicer error: {0}")]
    Slicer(String),
}
