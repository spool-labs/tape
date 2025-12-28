//! Type definitions for tape-store

mod impls;
pub mod keys;

// Re-export core types
pub use tape_core::types::{EpochNumber, NodeId, TapeNumber, TrackNumber};
pub use tape_crypto::Hash;

// Re-export storage wrapper types
// Pubkey is a wincode-serializable wrapper around solana_program::pubkey::Pubkey.
// Convert from Solana's Pubkey using `.into()`.
pub use impls::Pubkey;

// Re-export keys
pub use keys::{GcKey, RecoveryKey, SliceKey, SpoolKey, TapeKey, TrackKey};

// Re-export types from ops module
pub use crate::ops::{
    AssignmentStatus, CommitteeData, Compression, SliceMeta, SliceState, SliceStatus, SyncPhase,
    SyncProgress, TapeData, TrackData,
};
