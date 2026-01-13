//! Shared constants for e2e tests.

use std::time::Duration;

use tape_api::program::{EPOCH_DURATION, MIN_EPOCH_DURATION};

// Re-export from api
pub use tape_api::program::MIN_COMMITTEE_SIZE;

/// Wait duration for minimum epoch (low-quorum mode) + 1 second buffer.
pub const MIN_EPOCH_WAIT: Duration = Duration::from_secs(MIN_EPOCH_DURATION as u64 + 1);

/// Wait duration for full epoch (normal mode) + 1 second buffer.
pub const EPOCH_WAIT: Duration = Duration::from_secs(EPOCH_DURATION as u64 + 1);
