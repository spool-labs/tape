//! Spool synchronization protocol.
//!
//! Types and handlers for syncing spool data between nodes during epoch transitions.

mod handler;
mod types;

pub use handler::{
    SpoolSyncHandler, SyncError, DEFAULT_BATCH_SIZE, DEFAULT_MAX_CONCURRENT_SYNCS,
};
pub use types::{
    SignedSyncRequest, SyncSlice, SyncSpoolRequest, SyncSpoolResponse, SyncSpoolRequestV1,
    TrackId, track_id_from_pubkey, track_id_to_pubkey,
};
