//! Supervisor — centralized task scheduler with retry, cancellation, and concurrency limits.
//!
//! The supervisor owns:
//! - A `BinaryHeap` of due times for retry scheduling (scales to millions of entries)
//! - A `JoinSet` tracking all spawned worker futures
//! - Per-category `Semaphore`s for concurrency limits
//! - Per-task `CancellationToken`s for cancellation
//!
//! A single scheduler loop does `sleep_until(next_due)`, pops due items, acquires
//! the appropriate semaphore, and dispatches to workers. On retryable failure,
//! `BackoffConfig` computes the next delay and the item is pushed back to the heap.

use std::sync::Arc;

use store::Store;

use crate::core::NodeContext;

/// Identifies a scheduled or running task.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum TaskKey {
    /// Advance the on-chain epoch.
    AdvanceEpoch,
    /// Sync this node's epoch state on-chain.
    SyncEpoch,
    /// Join the network on-chain.
    JoinNetwork,
    /// Advance a staking pool on-chain.
    AdvancePool,
    /// Register a snapshot commitment on-chain.
    RegisterSnapshot,
    /// Certify a snapshot with BLS aggregate on-chain.
    CertifySnapshot,
    /// Invalidate a track on-chain.
    InvalidateTrack { track: [u8; 32] },
    /// Sync a spool from a peer.
    SpoolSync { spool: u16 },
    /// Scan for missing slices in a spool.
    RecoveryScan { spool: u16 },
    /// Recover missing slices for a spool.
    SpoolRecovery { spool: u16 },
    /// Build a snapshot for the current epoch.
    SnapshotBuild,
    /// Certify a snapshot by collecting BLS signatures.
    SnapshotCertify,
    /// Bootstrap from a snapshot (new node joining).
    SnapshotBootstrap,
    /// Refresh cached on-chain state.
    RefreshOnchainState,
}

/// Result of a completed task, returned to the reconciler.
#[derive(Debug)]
pub enum TaskResult {
    /// Task completed successfully.
    Success(TaskKey),
    /// Task failed with a retryable error.
    RetryableError(TaskKey, String),
    /// Task failed permanently.
    PermanentError(TaskKey, String),
}

/// Centralized task scheduler.
pub struct Supervisor<S: Store> {
    context: Arc<NodeContext<S>>,
}

impl<S: Store> Supervisor<S> {
    pub fn new(context: Arc<NodeContext<S>>) -> Self {
        Self { context }
    }
}
