//! Type definitions for tape-store
//!
//! This module provides all the types used throughout the tape-store crate:
//! - Enums: NodeStatus, SpoolState, ObjectInfo
//! - Keys: EpochKey, UnitKey, SpoolIndexKey, SliceKey, TrackLookupKey, vote/snapshot keys
//! - Values: TapeInfo, PackedTrack, snapshot artifacts
mod enums;
pub mod keys;
pub mod slice;
mod values;

// Re-export enum types
pub use enums::{
    AuditDecision, AuditOp, CredentialScope, CredentialStatus, ObjectInfo, PolicyAction,
    PolicyEffect, SystemObjectKind,
};

// Re-export key types
pub use keys::{
    AuditKey, ChallengeRoundKey, EpochKey, EventLogKey, LedgerReservationKey,
    MultipartPartChunkKey, MultipartPartKey, PeerRecordKey, PendingWriteKey,
    ObjectListKey, PolicyRuleKey, SliceKey, SnapshotArtifactKey, SpoolIndexKey, TrackLookupKey,
    TrackSampleKey, UnitKey, VoteSigKey,
};

// Re-export value types
pub use values::{
    AuditEntry, AuthState, BudgetLimits, Credential, CredentialCaps, InvalidationProof,
    LedgerEntry, LedgerReservation, MultipartPart, MultipartPartChunk, MultipartUpload,
    MULTIPART_CHUNK_BYTES, ObjectListEntry, ObjectMetadata, PendingOp, PendingState, PendingWrite,
    PendingWriteData, PolicyRule, SliceWrite,
    SnapshotArtifact, TapeInfo, TrackSample,
};
