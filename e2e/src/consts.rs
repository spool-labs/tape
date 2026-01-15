//! Shared constants for e2e tests.

use std::time::Duration;

use tape_api::program::EPOCH_DURATION;

// Re-export from api
pub use tape_api::program::MIN_COMMITTEE_SIZE;

/// Wait duration for epoch + 1 second buffer.
pub const EPOCH_WAIT: Duration = Duration::from_secs(EPOCH_DURATION as u64 + 1);
