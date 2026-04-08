//! Type definitions for tape-store
//!
//! This module provides all the types used throughout the tape-store crate:
//! - Enums: NodeStatus, SpoolState, ObjectInfo
//! - Keys: EpochKey, UnitKey, SpoolIndexKey, SliceKey, TrackLookupKey
//! - Values: TapeInfo, PackedTrack
mod enums;
pub mod keys;
mod values;

// Re-export enum types
pub use enums::ObjectInfo;

// Re-export key types
pub use keys::{
    EpochKey, EventLogKey, SliceKey, SnapshotGroupKey, SnapshotSliceKey, SpoolIndexKey,
    TrackLookupKey, UnitKey,
};

// Re-export value types
pub use values::{InvalidationProof, SliceValue, TapeInfo};
