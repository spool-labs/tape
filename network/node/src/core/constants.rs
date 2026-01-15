//! Centralized constants for the storage node.
//!
//! All magic numbers and configuration defaults should be defined here
//! to make them discoverable and maintainable.

use std::time::Duration;

// Metrics constants

/// Latency buckets for node operations (in seconds).
/// Used for Prometheus histogram metrics.
pub const LATENCY_BUCKETS: &[f64] = &[
    0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0,
];

// Spool sync constants

/// Default batch size for sync requests.
pub const DEFAULT_BATCH_SIZE: usize = 1000;

/// Default max concurrent sync operations.
pub const DEFAULT_MAX_CONCURRENT_SYNCS: usize = 4;

// Network sync (FSM) constants

/// Polling interval for epoch advancement monitoring.
pub const EPOCH_ADVANCE_POLL_INTERVAL: Duration = Duration::from_secs(1);

/// Compute units required for AdvanceEpoch instruction.
/// AdvanceEpoch performs committee rotation and spool reallocation which
/// requires significant computation, especially with many nodes.
pub const ADVANCE_EPOCH_COMPUTE_UNITS: u32 = 1_400_000;

/// Compute units required for AdvancePool instruction.
/// AdvancePool calculates rewards based on committee size and spool assignment,
/// which can exceed the default 200k CU limit with larger committees.
pub const ADVANCE_POOL_COMPUTE_UNITS: u32 = 400_000;

// Orchestrator constants

/// Signal channel capacity (small - only FSM wake-up signals).
pub const SIGNAL_CHANNEL_CAPACITY: usize = 32;

// Challenge constants

/// Default interval between challenge rounds (in seconds).
pub const DEFAULT_CHALLENGE_INTERVAL_SECS: u64 = 60;

// Recovery constants

/// Recovery polling interval.
pub const RECOVERY_POLL_INTERVAL: Duration = Duration::from_secs(5);

/// Maximum recovery attempts before giving up.
pub const MAX_RECOVERY_ATTEMPTS: u8 = 10;

// Block processing constants

/// Default polling interval for Solana blocks (Solana slot time).
pub const DEFAULT_POLL_INTERVAL_MS: u64 = 400;

/// Maximum slots to process per iteration.
pub const MAX_SLOTS_PER_BATCH: u64 = 100;
