//! Type definitions for tape-store
//!
//! This module provides all the types used throughout the tape-store crate:
//! - Enums: NodeStatus, ObjectInfo
//! - Keys: EpochKey, UnitKey, SliceKey, TrackLookupKey, vote/snapshot keys
//! - Values: TapeInfo, PackedTrack, snapshot artifacts
//! - The missing_list: Entry, EntryKey and what an attempt records
mod enums;
pub mod keys;
pub mod missing;
pub mod slice;
mod values;

// Re-export enum types
pub use enums::{
    AuditDecision, AuditOp, CredentialScope, CredentialStatus, ObjectInfo, PolicyAction,
    PolicyEffect, SystemObjectKind,
};

// Re-export key types
pub use keys::{
    AuditKey, EpochKey, EventLogKey, LedgerReservationKey,
    MultipartPartChunkKey, MultipartPartKey, PendingWriteChunkKey, PendingWriteKey,
    ObjectListKey, PolicyRuleKey, RemovalKey, SliceKey, SnapshotArtifactKey,
    TrackLookupKey, UnitKey, VoteSigKey,
};

pub use missing::{
    Attempt, AttemptResult, Entry, EntryKey, FailureCount, Method, PackedEntry, WallTime,
};

// Re-export value types
pub use values::{
    AuditEntry, AuthState, BudgetLimits, CountedTrack, Credential, CredentialCaps, InvalidationProof,
    LedgerEntry, LedgerReservation, MultipartPart, MultipartPartChunk, MultipartUpload,
    MULTIPART_CHUNK_BYTES, ObjectListEntry, ObjectMetadata, PendingOp, PendingState, PendingWrite,
    PendingWriteChunk, PENDING_WRITE_CHUNK_BYTES, PolicyRule, SliceWrite,
    SnapshotArtifact, TapeInfo,
};
