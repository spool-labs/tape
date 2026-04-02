//! Type definitions for tape-store
//!
//! This module provides all the types used throughout the tape-store crate:
//! - Enums: NodeStatus, SpoolState, ObjectInfo
//! - Keys: EpochKey, UnitKey, SpoolIndexKey, SliceKey, TrackLookupKey
//! - Values: TapeInfo, PackedTrack
//! - Wrappers: Pubkey

mod enums;
mod impls;
pub mod keys;
mod values;

// Re-export core types used throughout the crate
pub use tape_core::spooler::SpoolGroup;
pub use tape_core::types::{ChunkIndex, EpochNumber, NodeId, SlotNumber, TrackNumber};
pub use tape_crypto::Hash;

// Re-export enum types
pub use enums::ObjectInfo;
pub use tape_core::system::{NodeStatus, SpoolState, SpoolStatus};

// Re-export key types
pub use keys::{EpochKey, EventLogKey, SliceKey, SpoolIndexKey, TrackLookupKey, UnitKey};

// Re-export value types
pub use values::{InvalidationProof, SliceValue, TapeInfo};
pub use tape_core::track::blob::BlobInfo;
pub use tape_core::track::data::TrackData;
pub use tape_core::track::types::PackedTrack;

// Re-export wrapper types
pub use impls::Pubkey;
