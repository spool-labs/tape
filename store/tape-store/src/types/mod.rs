//! Type definitions for tape-store
//!
//! This module provides all the types used throughout the tape-store crate:
//! - Enums: NodeStatus, SpoolStatus, ObjectInfo
//! - Keys: EpochKey, UnitKey, SpoolIndexKey, SliceKey
//! - Values: TapeInfo, TrackInfo, NodeInfo
//! - Wrappers: Pubkey

mod enums;
mod impls;
pub mod keys;
mod values;

// Re-export core types used throughout the crate
pub use tape_core::types::{ChunkIndex, EpochNumber, SlotNumber};
pub use tape_crypto::Hash;

// Re-export enum types
pub use enums::{NodeStatus, ObjectInfo, SpoolStatus};

// Re-export key types
pub use keys::{EpochKey, EventLogKey, SliceKey, SpoolIndexKey, UnitKey};

// Re-export value types
pub use values::{InvalidationProof, NodeInfo, TapeInfo, TrackInfo};

// Re-export wrapper types
pub use impls::Pubkey;
