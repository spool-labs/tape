pub mod deferral;
pub mod error;
pub mod decode;
pub mod scheduler;
pub mod spool;
pub mod worker;
pub(crate) mod helpers;
pub(crate) mod repair;
pub(crate) mod scan;

pub use deferral::LiveUploadDeferral;
pub use error::RecoveryError;
pub use scheduler::TrackSyncHandler;
pub use spool::start_spool_recovery;
pub use worker::recover_track_slice;

/// Maximum queued recovery tasks before backpressure.
pub(crate) const RECOVERY_TRACK_CONCURRENCY: usize = 1000;

/// Tracks scanned per DB page during recovery.
pub(crate) const SCAN_BATCH_SIZE: usize = 1000;
