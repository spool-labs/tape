//! Task implementations — each submodule handles one `Task` variant.

mod spool_support;

pub mod advance_epoch;
pub mod advance_pool;
pub mod invalidate_track;
pub mod join_network;
pub mod recovery_scan;
pub mod spool_recovery;
pub mod spool_support;
pub mod spool_sync;
pub mod sync_epoch;
