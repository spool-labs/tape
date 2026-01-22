//! Type definitions for tape-store
//!
//! This module provides all the types used throughout the tape-store crate:
//! - Enums: NodeStatus, SpoolStatus, SliceType, EncodingType
//! - Keys: SpoolEpochKey, SliceKey, PendingRecoveryKey, EpochKey, UnitKey
//! - Values: SliceInfo, TapeInfo, TrackInfo, SyncProgress, PrimarySliceData, RecoverySliceData
//! - Wrappers: Pubkey, CommitteeCache, CommitteeMemberInfo

mod enums;
mod impls;
pub mod keys;
mod values;

// Re-export core types used throughout the crate
pub use tape_core::types::{EpochNumber, NodeId, SlotNumber};
pub use tape_crypto::Hash;

// Re-export enum types
pub use enums::{EncodingType, NodeStatus, SliceType, SpoolStatus};

// Re-export key types
pub use keys::{EpochKey, PendingRecoveryKey, SliceKey, SpoolEpochKey, UnitKey};

// Re-export value types
pub use values::{
    PrimarySliceData, RecoverySliceData, SliceInfo, SyncProgress, TapeInfo, TrackInfo,
};

// Re-export wrapper types
pub use impls::{CommitteeCache, CommitteeMemberInfo, Pubkey};
