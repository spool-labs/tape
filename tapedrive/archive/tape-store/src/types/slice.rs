//! Slice storage types

use super::ids::{EpochNumber, Hash, Pubkey};
use serde::{Deserialize, Serialize};
use wincode_derive::{SchemaRead, SchemaWrite};

/// Metadata for a slice
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct SliceMeta {
    pub len: u32,
    pub leaf_hash: Hash,
    pub content_digest: Hash,
    pub compression: Compression,
    pub last_verified_at: i64,
    pub flags: u8,
}

/// Compression algorithm used for slice data
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
#[repr(u8)]
pub enum Compression {
    None = 0,
    Lz4 = 1,
    Zstd = 2,
}

/// State tracking for a slice including ownership and lifecycle
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct SliceState {
    pub current_epoch: EpochNumber,
    pub status: SliceStatus,
    pub prev_owner: Pubkey,
    pub current_owner: Pubkey,
    pub next_owner: Pubkey,
    pub repair_from: Pubkey,
    pub repair_last_attempt: i64,
    pub repair_retries: u16,
    pub handoff_to: Pubkey,
    pub handoff_last_attempt: i64,
    pub handoff_retries: u16,
    pub gc_at: i64,
    pub last_state_change: i64,
}

/// Status of a slice in its lifecycle
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
#[repr(u8)]
pub enum SliceStatus {
    Unknown = 0,
    Required = 1,
    Present = 2,
    Verified = 3,
    RepairingFromPeer = 4,
    Uploading = 5,
    HandoffPending = 6,
    HandoffComplete = 7,
    Deletable = 8,
}

/// Assignment status for a spool
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
#[repr(u8)]
pub enum AssignmentStatus {
    None = 0,
    Active = 1,
    ActiveSync = 2,
    ActiveRecover = 3,
    LockedToMove = 4,
}

/// Sync progress for a spool
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct SyncProgress {
    pub last_synced_track_id: u64,
    pub phase: SyncPhase,
}

/// Sync phase for a spool
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
#[repr(u8)]
pub enum SyncPhase {
    Idle = 0,
    Ingesting = 1,
    Repairing = 2,
}
