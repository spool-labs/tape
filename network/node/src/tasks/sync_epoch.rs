//! SyncEpoch — submit epoch sync attestation on-chain.

use std::sync::Arc;
use std::time::Duration;

use rpc::Rpc;
use store::Store;
use tape_api::errors::TapeError;
use tape_store::ops::SpoolOps;
use tape_store::types::SpoolStatus;
use tokio_util::sync::CancellationToken;

use crate::chain::submit_sync_epoch;
use crate::core::{NodeContext, require_epoch};
use crate::TaskOutcome;
use rpc_client::parse_tape_error;

const SYNC_EPOCH_PENDING_DELAY: Duration = Duration::from_secs(30);

pub async fn run<S: Store, R: Rpc>(
    context: Arc<NodeContext<S, R>>,
    cancel: CancellationToken,
) -> TaskOutcome {
    let epoch = match require_epoch(&context.chain_state) {
        Ok(e) => e,
        Err(outcome) => return outcome,
    };

    let mut owned_spools: Vec<u16> = match context.store.iter_all_spools() {
        Ok(spools) => spools
            .into_iter()
            .filter(|(_, status)| !matches!(status, SpoolStatus::LockedToMove))
            .map(|(id, _)| id)
            .collect(),
        Err(e) => return TaskOutcome::Retryable(format!("iter spools: {e}")),
    };
    owned_spools.sort_unstable();

    if cancel.is_cancelled() {
        return TaskOutcome::Success;
    }

    let result = tokio::select! {
        r = submit_sync_epoch(&context, epoch, &owned_spools) => r,
        _ = cancel.cancelled() => return TaskOutcome::Success,
    };
    match result {
        Ok(sig) => {
            tracing::info!(%sig, epoch = epoch.as_u64(), "sync_epoch submitted");
            TaskOutcome::Success
        }
        Err(ref e) => match parse_tape_error(e) {
            Some(TapeError::AlreadySynced) => {
                tracing::info!("sync_epoch already completed");
                TaskOutcome::Success
            }
            Some(TapeError::BadEpochState)
            | Some(TapeError::NotInCommittee)
            | Some(TapeError::BadSpoolHash)
            | Some(TapeError::BadEpochId) => {
                tracing::debug!(error = %e, "sync_epoch waiting for protocol state");
                TaskOutcome::Pending(SYNC_EPOCH_PENDING_DELAY)
            }
            _ => {
                tracing::warn!(error = %e, "sync_epoch submission failed");
                TaskOutcome::Retryable(format!("sync_epoch: {e}"))
            }
        },
    }
}
