//! Type definitions for tape-store
//!
//! This module provides all the types used throughout the tape-store crate:
//! - Enums: NodeStatus, SpoolState, ObjectInfo
//! - Keys: EpochKey, UnitKey, SpoolIndexKey, SliceKey, TrackLookupKey, vote/snapshot keys
//! - Values: TapeInfo, PackedTrack, snapshot artifacts
mod enums;
pub mod keys;
mod values;

// Re-export enum types
pub use enums::{
    AuditDecision, AuditOp, CredentialScope, CredentialStatus, ObjectInfo, PolicyAction,
    PolicyEffect, SystemObjectKind,
};

// Re-export key types
pub use keys::{
    AuditKey, EpochKey, EventLogKey, LedgerReservationKey, MultipartPartKey, ObjectListKey,
    PolicyRuleKey, SliceKey, SnapshotArtifactKey, SpoolIndexKey, TrackLookupKey, UnitKey, VoteSigKey,
};

// Re-export value types
pub use values::{
    AuditEntry, AuthState, BudgetLimits, Credential, CredentialCaps, InvalidationProof,
    LedgerEntry, LedgerReservation, MultipartPart, MultipartPartData, MultipartUpload,
    ObjectListEntry, ObjectMetadata, PolicyRule, SliceValue, SnapshotArtifact, TapeInfo,
};
