//! High-level operation traits for TapeStore
//!
//! ## Operation Traits
//!
//! - `MetaOps`: Node status, cluster hash, current epoch, node address, sync cursor, GC tracking
//! - `TapeOps`: Tape metadata
//! - `TrackOps`: Compressed-track catalog
//! - `TrackDataOps`: Local track payload data
//! - `TapeTrackScan`: Ordered walk of a tape's tracks with their payloads
//! - `ObjectInfoOps`: Object info (blacklisted, invalid, valid)
//! - `ObjectMetadataOps`: Named-object reverse lookup
//! - `RemovalOps`: Tapes closed or rewound in the current epoch
//! - `SpoolOps`: Spool rows, the missing_list and the owner_history (NOT epoch-namespaced)
//! - `SliceOps`: Slice data storage
//! - `CredentialOps`: S3 write credentials (put/get/revoke/list)
//! - `PolicyOps`: Write-authorization policy engine (rule CRUD + evaluate)
//! - `AuthStateOps`: Write-authorization control state (kill switch, policy version)
//! - `AuditOps`: Append-only write-authorization audit log (append/scan)
//! - `LedgerOps`: Per-principal accounting ledger (atomic reserve/commit/refund + TTL sweep)
//! - `MultipartOps`: Durable S3 multipart upload state (upload + part CRUD)
//! - `PendingWriteOps`: Durable queue of S3 writes waiting to reach the chain

mod audit_log;
mod auth_state;
mod credential;
mod event_log;
mod ledger;
mod meta;
mod object_info;
mod object_list;
mod object_metadata;
mod policy;
mod removal;
mod s3_multipart;
mod s3_pending;
mod snapshot;
mod slice;
mod spool;
mod tape;
mod track;
mod track_data;
mod track_scan;
mod vote;

// Re-export operation traits
pub use audit_log::AuditOps;
pub use auth_state::AuthStateOps;
pub use credential::CredentialOps;
pub use event_log::EventLogOps;
pub use ledger::{LedgerOps, ReserveOutcome, ReserveRequest};
pub use meta::MetaOps;
pub use object_info::ObjectInfoOps;
pub use object_list::{ObjectListOps, ObjectListPage};
pub use object_metadata::ObjectMetadataOps;
pub use policy::{PolicyDecision, PolicyOps};
pub use removal::{RemovalOps, object_has_left, track_has_left};
pub use s3_multipart::MultipartOps;
pub use s3_pending::PendingWriteOps;
pub use snapshot::SnapshotOps;
pub use slice::SliceOps;
pub use spool::{track_group_key, GroupTrack, SpoolOps, STORED_SPOOL_COUNT};
pub use tape::TapeOps;
pub use track::{TrackOps, tape_numbers_at, tape_track_numbers};
pub use track_data::TrackDataOps;
pub use track_scan::{ScannedTrack, TapeTrackScan};
pub use vote::VoteOps;
