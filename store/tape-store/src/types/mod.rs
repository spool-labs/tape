//! Type definitions for tape-store

mod impls;
pub mod keys;

// Re-export core types
pub use tape_core::types::{EpochNumber, NodeId};
pub use tape_crypto::Hash;

// Re-export storage wrapper types
// Pubkey is a wincode-serializable wrapper around solana_program::pubkey::Pubkey.
// Convert from Solana's Pubkey using `.into()`.
pub use impls::Pubkey;

// Re-export keys
pub use keys::{GcKey, SliceKey, SpoolKey};

// Re-export types from ops module
pub use crate::ops::{
    CommitteeCache, CommitteeMemberInfo, HandoffInfo, RecoveryInfo, SliceMeta, SpoolState,
    SpoolStatus, TrackInfo, MERKLE_HEIGHT,
};
