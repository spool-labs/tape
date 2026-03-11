//! SyncEpoch — submit epoch sync attestation on-chain.

use std::sync::Arc;
use std::time::Duration;

use rpc::{Rpc, RpcError};
use store::Store;
use tape_protocol::Api;
use tape_api::errors::TapeError;
use tape_store::ops::SpoolOps;
use tokio_util::sync::CancellationToken;

use crate::chain::submit_sync_epoch;
use crate::core::NodeContext;
use crate::TaskOutcome;
use rpc_client::parse_tape_error;

const SYNC_EPOCH_PENDING_DELAY: Duration = Duration::from_secs(5);

pub async fn run<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: Arc<NodeContext<Db, Cluster, Blockchain>>,
    cancel: CancellationToken,
) -> TaskOutcome {
    let state = ctx.state();
    if state.epoch.is_zero() {
        return TaskOutcome::Retryable("no current epoch".into());
    }
    let epoch = state.epoch;

    let spool_states = match ctx.store.iter_all_spools() {
        Ok(spools) => spools,
        Err(e) => return TaskOutcome::Retryable(format!("iter spools: {e}")),
    };

    let mut owned_spools: Vec<u16> = Vec::new();
    let mut pending_spools = Vec::new();

    for (id, state) in spool_states {
        if state.is_locked() {
            continue;
        }
        if state.is_active() {
            owned_spools.push(id);
        } else {
            pending_spools.push((id, state));
        }
    }
    owned_spools.sort_unstable();

    if !pending_spools.is_empty() {
        tracing::debug!(
            pending = pending_spools.len(),
            ?pending_spools,
            "sync_epoch waiting for spool handoff to complete"
        );
        return TaskOutcome::Pending(SYNC_EPOCH_PENDING_DELAY);
    }

    if cancel.is_cancelled() {
        return TaskOutcome::Success;
    }

    let result = tokio::select! {
        r = submit_sync_epoch(&ctx, epoch, &owned_spools) => r,
        _ = cancel.cancelled() => return TaskOutcome::Success,
    };

    let had_error = result.is_err();
    let outcome = match result {
        Ok(sig) => {
            tracing::info!(%sig, epoch = epoch.as_u64(), "sync_epoch submitted");
            TaskOutcome::Success
        }
        Err(ref e) => classify_sync_epoch_error(e),
    };

    match &outcome {
        TaskOutcome::Success if had_error => {
            tracing::info!("sync_epoch already completed");
        }
        TaskOutcome::Pending(_) => {
            tracing::debug!("sync_epoch waiting for protocol state");
        }
        TaskOutcome::Retryable(err) => {
            tracing::warn!(error = %err, "sync_epoch submission failed");
        }
        _ => {}
    }

    outcome
}

fn classify_sync_epoch_error(err: &RpcError) -> TaskOutcome {
    match parse_tape_error(err) {
        Some(TapeError::AlreadySynced) => TaskOutcome::Success,
        Some(TapeError::BadEpochState) => TaskOutcome::Pending(SYNC_EPOCH_PENDING_DELAY),
        Some(TapeError::NotInCommittee)
        | Some(TapeError::BadSpoolHash)
        | Some(TapeError::BadEpochId) => TaskOutcome::Permanent(format!("sync_epoch: {err}")),
        _ => TaskOutcome::Retryable(format!("sync_epoch: {err}")),
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    fn tx_error(code: u32) -> RpcError {
        RpcError::Transaction(format!("custom program error: 0x{code:x}"))
    }

    #[test]
    fn already_synced_is_success() {
        let out = classify_sync_epoch_error(&tx_error(TapeError::AlreadySynced as u32));
        assert!(matches!(out, TaskOutcome::Success));
    }

    #[test]
    fn bad_epoch_state_is_pending() {
        let out = classify_sync_epoch_error(&tx_error(TapeError::BadEpochState as u32));
        assert!(matches!(out, TaskOutcome::Pending(delay) if delay == SYNC_EPOCH_PENDING_DELAY));
    }

    #[test]
    fn membership_and_hash_errors_are_permanent() {
        let codes = [
            TapeError::NotInCommittee as u32,
            TapeError::BadSpoolHash as u32,
            TapeError::BadEpochId as u32,
        ];
        for code in codes {
            let out = classify_sync_epoch_error(&tx_error(code));
            assert!(matches!(out, TaskOutcome::Permanent(_)));
        }
    }

    #[test]
    fn unexpected_state_is_retryable() {
        let out = classify_sync_epoch_error(&tx_error(TapeError::UnexpectedState as u32));
        assert!(matches!(out, TaskOutcome::Retryable(_)));
    }

    #[test]
    fn request_error_is_retryable() {
        let out = classify_sync_epoch_error(&RpcError::Request("timeout".into()));
        assert!(matches!(out, TaskOutcome::Retryable(_)));
    }
}
