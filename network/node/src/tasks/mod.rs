//! Task execution — dispatches each `TaskKey` to its implementation module.

mod advance_epoch;
mod advance_pool;
mod invalidate_track;
mod join_network;
mod recovery_scan;
mod refresh_onchain_state;
mod snapshot;
mod spool_recovery;
mod spool_sync;
mod sync_epoch;

use std::sync::Arc;

use rpc::Rpc;
use store::Store;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

use crate::core::NodeContext;
use crate::supervisor::{TaskKey, TaskOutcome};

/// Execute a single task to completion.
///
/// Acquires the concurrency semaphore, checks for cancellation, then
/// dispatches to the appropriate task module.
pub async fn execute_task<S: Store, R: Rpc>(
    context: Arc<NodeContext<S, R>>,
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
            refresh_onchain_state::run(context, cancel).await
        }
        TaskKey::AdvanceEpoch => {
            advance_epoch::run(context, cancel).await
        }
        TaskKey::SyncEpoch => {
            sync_epoch::run(context, cancel).await
        }
        TaskKey::JoinNetwork => {
            join_network::run(context, cancel).await
        }
        TaskKey::AdvancePool => {
            advance_pool::run(context, cancel).await
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
            invalidate_track::run(context, *track, cancel).await
        }
        TaskKey::SnapshotBuild => {
            snapshot::run_build(context, cancel).await
        }
        TaskKey::SnapshotCertify => {
            snapshot::run_certify(context, cancel).await
        }
        TaskKey::RegisterSnapshot => {
            snapshot::run_register(context, cancel).await
        }
        TaskKey::CertifySnapshot => {
            snapshot::run_certify_onchain(context, cancel).await
        }
        TaskKey::SnapshotBootstrap => {
            snapshot::run_bootstrap(context, cancel).await
        }
    };

    (key, outcome)
}
