//! Type definitions for tape-store

pub mod chain;
mod impls;
pub mod keys;
pub mod slice;

// Re-export core types
pub use tape_core::types::{EpochNumber, NodeId, TapeNumber, TrackNumber};
pub use tape_crypto::Hash;

// Re-export storage wrapper types
// Pubkey is a wincode-serializable wrapper around solana_program::pubkey::Pubkey.
// Convert from Solana's Pubkey using `.into()`.
pub use impls::Pubkey;

// Re-export storage types
pub use chain::{CommitteeData, CommitteeMemberData, TapeData, TrackData};
pub use keys::{GcKey, RecoveryKey, SliceKey, SpoolKey, TapeKey, TrackKey};
pub use slice::{
    AssignmentStatus, Compression, SliceMeta, SliceState, SliceStatus, SyncPhase, SyncProgress,
};
