//! Type definitions for tape-store

pub mod chain;
pub mod ids;
pub mod keys;
pub mod slice;

// Re-export commonly used types
pub use chain::{Committee, CommitteeMember, Tape, Track};
pub use ids::{EpochNumber, Hash, NodeId, Pubkey, TapeNumber, TrackNumber};
pub use keys::{GcKey, RecoveryKey, SliceKey, SpoolKey, TapeKey, TrackKey};
pub use slice::{
    AssignmentStatus, Compression, SliceMeta, SliceState, SliceStatus, SyncPhase, SyncProgress,
};
