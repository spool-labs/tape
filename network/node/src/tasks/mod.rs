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
use std::time::Instant;

use rpc::Rpc;
use rpc::RpcError;
use store::Store;
use tape_store::ops::MetaOps;
use tape_api::errors::{ProgramError, TapeError};
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

use crate::runtime::NodeContext;
use crate::runtime::PeerHandle;
use crate::supervisor::{TaskKey, TaskOutcome};

/// Execute a single task to completion.
///
/// Acquires the concurrency semaphore, checks for cancellation, then
/// dispatches to the appropriate task module.
pub async fn execute_task<S: Store, R: Rpc>(
    context: Arc<NodeContext<S, R>>,
    peer_handle: PeerHandle,
    key: TaskKey,
    cancel: CancellationToken,
    semaphore: Arc<Semaphore>,
) -> (TaskKey, TaskOutcome) {
    let started_at = Instant::now();
    tracing::trace!(task = ?key, "task execution started");
    let _permit = match semaphore.acquire().await {
        Ok(p) => p,
        Err(_) => {
            tracing::trace!(task = ?key, "task execution aborted: semaphore closed");
            return (key, TaskOutcome::Permanent("semaphore closed".into()));
        }
    };

    if cancel.is_cancelled() {
        tracing::trace!(task = ?key, "task execution skipped: already canceled");
        return (key, TaskOutcome::Success);
    }

    // Each epoch-scoped key is pinned to its scheduled chain epoch.
    // If the node has already advanced/lagged, skip stale tx/submission.
    if let Some(task_epoch) = key.scheduled_epoch() {
        if let Ok(Some(chain_epoch)) = context.store.get_chain_epoch() {
            if task_epoch != chain_epoch {
                tracing::trace!(
                    task = ?key,
                    scheduled_epoch = task_epoch.0,
                    chain_epoch = chain_epoch.0,
                    "task execution skipped: stale epoch"
                );
                return (key, TaskOutcome::Success);
            }
        }
    }

    let outcome = match &key {
        TaskKey::RefreshOnchainState => {
            refresh_onchain_state::run(context, peer_handle, cancel).await
        }
        TaskKey::AdvanceEpoch { .. } => {
            advance_epoch::run(context, cancel).await
        }
        TaskKey::SyncEpoch { .. } => {
            sync_epoch::run(context, cancel).await
        }
        TaskKey::JoinNetwork { .. } => {
            join_network::run(context, cancel).await
        }
        TaskKey::AdvancePool { .. } => {
            advance_pool::run(context, cancel).await
        }
        TaskKey::SpoolSync { spool } => {
            spool_sync::run(context, peer_handle, *spool, cancel).await
        }
        TaskKey::SpoolRecovery { spool } => {
            spool_recovery::run(context, peer_handle, *spool, cancel).await
        }
        TaskKey::RecoveryScan { spool } => {
            recovery_scan::run(context, *spool, cancel).await
        }
        TaskKey::InvalidateTrack { track } => {
            invalidate_track::run(context, *track, cancel).await
        }
        TaskKey::SnapshotBuild { .. } => {
            snapshot::run_build(context, peer_handle, cancel).await
        }
        TaskKey::SnapshotCollect { .. } => {
            snapshot::run_collect(context, peer_handle, cancel).await
        }
        TaskKey::RegisterSnapshot { .. } => {
            snapshot::run_register(context, peer_handle, cancel).await
        }
        TaskKey::SnapshotSubmit { .. } => {
            snapshot::run_submit(context, peer_handle, cancel).await
        }
        TaskKey::SnapshotBootstrap => {
            snapshot::run_bootstrap(context, peer_handle, cancel).await
        }
    };

    let duration_ms = started_at.elapsed().as_millis() as u64;
    tracing::trace!(
        task = ?key,
        outcome = ?outcome,
        duration_ms,
        "task execution completed"
    );
    tracing::Span::current().record("duration_ms", duration_ms);
    (key, outcome)
}

/// Try to decode a typed TapeError from an RPC transaction error.
pub(crate) fn parse_tape_error(err: &RpcError) -> Option<TapeError> {
    let RpcError::Transaction(msg) = err else {
        return None;
    };
    match ProgramError::from_error_string(msg) {
        Some(ProgramError::Tape(e)) => Some(e),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::parse_tape_error;
    use rpc::RpcError;
    use tape_api::errors::TapeError;

    #[test]
    fn parse_hex() {
        let err = RpcError::Transaction("custom program error: 0x52".to_string());
        assert_eq!(parse_tape_error(&err), Some(TapeError::AlreadyAdvanced));
    }

    #[test]
    fn parse_decimal() {
        let err = RpcError::Transaction("TransactionError::InstructionError(0, Custom(81))".to_string());
        assert_eq!(parse_tape_error(&err), Some(TapeError::AlreadySynced));
    }

    #[test]
    fn parse_already_certified() {
        let err = RpcError::Transaction("custom program error: 0x74".to_string());
        assert_eq!(parse_tape_error(&err), Some(TapeError::AlreadyCertified));
    }

    #[test]
    fn skip_non_tx() {
        let err = RpcError::Request("boom".to_string());
        assert_eq!(parse_tape_error(&err), None);
    }
}
