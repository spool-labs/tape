//! Task execution — dispatches each `TaskKey` to its implementation module.

mod recovery_scan;
mod snapshot;
mod spool_recovery;
mod spool_sync;

#[cfg(feature = "rpc")]
mod advance_epoch;
#[cfg(feature = "rpc")]
mod advance_pool;
#[cfg(feature = "rpc")]
mod invalidate_track;
#[cfg(feature = "rpc")]
mod join_network;
#[cfg(feature = "rpc")]
mod refresh_onchain_state;
#[cfg(feature = "rpc")]
mod sync_epoch;

use std::sync::Arc;

use store::Store;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

use crate::core::NodeContext;
use crate::supervisor::{TaskKey, TaskOutcome};

/// Execute a single task to completion.
///
/// Acquires the concurrency semaphore, checks for cancellation, then
/// dispatches to the appropriate task module.
pub async fn execute_task<S: Store>(
    context: Arc<NodeContext<S>>,
    key: TaskKey,
    cancel: CancellationToken,
    semaphore: Arc<Semaphore>,
) -> (TaskKey, TaskOutcome) {
    let _permit = match semaphore.acquire().await {
        Ok(p) => p,
        Err(_) => return (key, TaskOutcome::Permanent("semaphore closed".into())),
    };

    if cancel.is_cancelled() {
        return (key, TaskOutcome::Success);
    }

    let outcome = match &key {
        TaskKey::RefreshOnchainState => {
            #[cfg(feature = "rpc")]
            { refresh_onchain_state::run(context, cancel).await }
            #[cfg(not(feature = "rpc"))]
            { TaskOutcome::Success }
        }
        TaskKey::AdvanceEpoch => {
            #[cfg(feature = "rpc")]
            { advance_epoch::run(context, cancel).await }
            #[cfg(not(feature = "rpc"))]
            { TaskOutcome::Success }
        }
        TaskKey::SyncEpoch => {
            #[cfg(feature = "rpc")]
            { sync_epoch::run(context, cancel).await }
            #[cfg(not(feature = "rpc"))]
            { TaskOutcome::Success }
        }
        TaskKey::JoinNetwork => {
            #[cfg(feature = "rpc")]
            { join_network::run(context, cancel).await }
            #[cfg(not(feature = "rpc"))]
            { TaskOutcome::Success }
        }
        TaskKey::AdvancePool => {
            #[cfg(feature = "rpc")]
            { advance_pool::run(context, cancel).await }
            #[cfg(not(feature = "rpc"))]
            { TaskOutcome::Success }
        }
        TaskKey::SpoolSync { spool } => {
            spool_sync::run(context, *spool, cancel).await
        }
        TaskKey::SpoolRecovery { spool } => {
            spool_recovery::run(context, *spool, cancel).await
        }
        TaskKey::RecoveryScan { spool } => {
            recovery_scan::run(context, *spool, cancel).await
        }
        TaskKey::InvalidateTrack { track } => {
            #[cfg(feature = "rpc")]
            { invalidate_track::run(context, *track, cancel).await }
            #[cfg(not(feature = "rpc"))]
            { let _ = track; TaskOutcome::Success }
        }
        TaskKey::SnapshotBuild
        | TaskKey::SnapshotCertify
        | TaskKey::SnapshotBootstrap
        | TaskKey::RegisterSnapshot
        | TaskKey::CertifySnapshot => snapshot::run_stub(&key),
    };

    (key, outcome)
}
