//! Spool synchronization protocol.
//!
//! Types and handlers for syncing spool data between nodes during epoch transitions.

pub mod handler;
pub mod types;

pub use handler::{SpoolSyncHandler, SyncError};
pub use types::{SignedSyncRequest, SyncSlice, SyncSpoolRequest, SyncSpoolResponse};
