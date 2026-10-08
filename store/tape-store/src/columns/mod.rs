//! Column family definitions for tape-store
//!
//! This module defines the active column families:
//!
//! ## Metadata Columns
//! - `meta`: Node configuration and metadata (String -> Vec<u8>)
//! - `tape`: Tape metadata (Address -> TapeInfo)
//! - `track`: Canonical compressed-track catalog (Address -> PackedTrack)
//! - `track_lookup`: Tape-local ordered index ((tape, tape_number, track_number) -> Address)
//! - `track_data`: Local track payload data (Address -> BlobData)
//! - `object_info`: Object metadata (Address -> ObjectInfo)
//! - `object_metadata`: Named-object reverse lookup (Address -> ObjectMetadata)
//! - `removal_log`: Tapes closed or rewound in an epoch ((tape, tape_number, epoch) -> TrackNumber)
//!
//! ## Sync Columns
//! - `sync_cursor`: Last processed slot (UnitKey -> SlotNumber)
//! - `gc`: GC progress tracking (String -> EpochNumber)
//!
//! ## Spool Columns (NOT epoch-namespaced)
//! - `spool_status`: Spool status (SpoolIndexKey -> SpoolStatus)
//! - `spool_pending_repair`: Pending repair (SliceKey -> ready slot)
//! - `spool_pending_recovery`: Pending recovery (SliceKey -> ())
//!
//! ## Slice Data Column (BlobDB)
//! - `slice`: Slice data, sidecar and payload under one key (SliceKey -> stored slice)
//!
//! ## Event Log Column
//! - `event_log`: Per-epoch replayable events (EventLogKey -> CapturedEvent)
//!
//! ## Vote Coordination Columns
//! - `vote_sig`: Per-group BLS signatures keyed by vote candidate and signer
//!
//! ## Snapshot Coordination Columns
//! - `snapshot_artifact`: Local build artifacts retained until snapshot finalization
//!
//! ## S3 Write-Authorization Columns
//! - `credential`: S3 write credentials keyed by access key id (String -> Credential)
//! - `policy_rule`: Ordered write-authorization policy rules (PolicyRuleKey -> PolicyRule)
//! - `auth_state`: Write-authorization control state singleton (UnitKey -> AuthState)
//! - `audit_log`: Append-only authorize-decision log (AuditKey -> AuditEntry)
//! - `ledger`: Per-principal accounting ledger (Address -> LedgerEntry)
//! - `ledger_reservation`: Outstanding budget reservations (LedgerReservationKey -> LedgerReservation)
//! - `s3_multipart_upload`: In-flight multipart upload metadata (String -> MultipartUpload)
//! - `s3_multipart_part`: Buffered multipart part metadata (MultipartPartKey -> MultipartPart)
//! - `s3_multipart_part_data`: Buffered multipart part chunks (MultipartPartChunkKey -> MultipartPartChunk)
//! - `s3_pending_write`: Queued S3 writes awaiting the chain (PendingWriteKey -> PendingWrite)
//! - `s3_pending_write_data`: Queued object payload chunks (PendingWriteChunkKey -> PendingWriteChunk)

pub mod audit_log;
pub mod auth_state;
pub mod credential;
pub mod event_log;
pub mod gc;
pub mod ledger;
pub mod meta;
pub mod object_info;
pub mod object_list;
pub mod object_metadata;
pub mod policy;
pub mod removal_log;
pub mod s3_multipart;
pub mod s3_pending;
pub mod snapshot;
pub mod slice;
pub mod spool;
pub mod sync_cursor;
pub mod tape;
pub mod track;
pub mod track_data;
pub mod track_lookup;
pub mod vote;

// Re-export all column types
pub use audit_log::AuditLogCol;
pub use auth_state::AuthStateCol;
pub use credential::CredentialCol;
pub use event_log::EventLogCol;
pub use gc::GcCol;
pub use ledger::{LedgerCol, LedgerReservationCol};
pub use meta::MetaCol;
pub use object_info::ObjectInfoCol;
pub use object_list::ObjectListCol;
pub use object_metadata::ObjectMetadataCol;
pub use policy::PolicyRuleCol;
pub use removal_log::RemovalLogCol;
pub use s3_multipart::{S3MultipartPartCol, S3MultipartPartDataCol, S3MultipartUploadCol};
pub use s3_pending::{S3PendingWriteCol, S3PendingWriteDataCol};
pub use snapshot::SnapshotArtifactCol;
pub use slice::SliceCol;
pub use spool::{SpoolPendingRecoveryCol, SpoolPendingRepairCol, SpoolStatusCol};
pub use sync_cursor::{SyncCursor, SyncCursorCol};
pub use tape::TapeCol;
pub use track::TrackCol;
pub use track_data::TrackDataCol;
pub use track_lookup::TrackLookupCol;
pub use vote::VoteSigCol;

/// List of all column family names in the store.
pub const ALL_COLUMN_FAMILIES: &[&str] = &[
    "meta",
    "tape",
    "track",
    "track_lookup",
    "track_data",
    "object_info",
    "object_metadata",
    "object_list",
    "sync_cursor",
    "gc",
    "spool_status",
    "spool_pending_repair",
    "spool_pending_recovery",
    "slice",
    "event_log",
    "vote_sig",
    "snapshot_artifact",
    "credential",
    "policy_rule",
    "auth_state",
    "audit_log",
    "ledger",
    "ledger_reservation",
    "s3_multipart_upload",
    "s3_multipart_part",
    "s3_multipart_part_data",
    "s3_pending_write",
    "s3_pending_write_data",
    "removal_log",
];
