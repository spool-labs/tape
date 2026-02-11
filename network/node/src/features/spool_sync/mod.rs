//! Spool synchronization protocol.
//!
//! Types and handlers for syncing spool data between nodes during epoch transitions.

mod handler;
mod types;

pub use handler::{
    SpoolSyncHandler, SyncError, DEFAULT_BATCH_SIZE, DEFAULT_MAX_CONCURRENT_SYNCS,
};
pub use types::{
    SyncSlice, SyncSpoolRequest, SyncSpoolRequestV1, SyncSpoolResponse,
};
