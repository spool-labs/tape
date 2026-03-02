//! AdvanceEpoch — submit the advance_epoch instruction on-chain.

use std::sync::Arc;
use std::time::Duration;

use rpc::{Rpc, RpcError};
use rpc_client::parse_tape_error;
use store::Store;
use tape_api::errors::TapeError;
use tokio_util::sync::CancellationToken;

use crate::chain::submit_advance_epoch;
use crate::core::NodeContext;
use crate::TaskOutcome;

const ADVANCE_EPOCH_PENDING_DELAY: Duration = Duration::from_secs(5);

fn classify_advance_epoch_error(err: &RpcError) -> TaskOutcome {
    match parse_tape_error(err) {
        Some(TapeError::TooSoon)
        | Some(TapeError::InsufficientCommittee)
        | Some(TapeError::SnapshotIncomplete)
        | Some(TapeError::BadEpochState)
        | Some(TapeError::UnexpectedState) => TaskOutcome::Pending(ADVANCE_EPOCH_PENDING_DELAY),
        Some(TapeError::BadSchedule) => TaskOutcome::Permanent(format!("advance_epoch: {err}")),
        _ => TaskOutcome::Retryable(format!("advance_epoch: {err}")),
    }
}

pub async fn run<S: Store, R: Rpc>(
    context: Arc<NodeContext<S, R>>,
    cancel: CancellationToken,
) -> TaskOutcome {
    let result = tokio::select! {
        r = submit_advance_epoch(&context) => r,
        _ = cancel.cancelled() => return TaskOutcome::Success,
    };
    match result {
        Ok(sig) => {
            tracing::info!(%sig, "advance_epoch submitted");
            TaskOutcome::Success
        }
        Err(ref e) => classify_advance_epoch_error(e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tx_error(code: u32) -> RpcError {
        RpcError::Transaction(format!("custom program error: 0x{code:x}"))
    }

    #[test]
    fn too_soon_is_pending() {
        let out = classify_advance_epoch_error(&tx_error(TapeError::TooSoon as u32));
        assert!(matches!(out, TaskOutcome::Pending(delay) if delay == ADVANCE_EPOCH_PENDING_DELAY));
    }

    #[test]
    fn insufficient_committee_is_pending() {
        let out = classify_advance_epoch_error(&tx_error(TapeError::InsufficientCommittee as u32));
        assert!(matches!(out, TaskOutcome::Pending(delay) if delay == ADVANCE_EPOCH_PENDING_DELAY));
    }

    #[test]
    fn snapshot_incomplete_is_pending() {
        let out = classify_advance_epoch_error(&tx_error(TapeError::SnapshotIncomplete as u32));
        assert!(matches!(out, TaskOutcome::Pending(delay) if delay == ADVANCE_EPOCH_PENDING_DELAY));
    }

    #[test]
    fn bad_epoch_state_is_pending() {
        let out = classify_advance_epoch_error(&tx_error(TapeError::BadEpochState as u32));
        assert!(matches!(out, TaskOutcome::Pending(delay) if delay == ADVANCE_EPOCH_PENDING_DELAY));
    }

    #[test]
    fn bad_schedule_is_permanent() {
        let out = classify_advance_epoch_error(&tx_error(TapeError::BadSchedule as u32));
        assert!(matches!(out, TaskOutcome::Permanent(_)));
    }

    #[test]
    fn unexpected_state_is_pending() {
        let out = classify_advance_epoch_error(&tx_error(TapeError::UnexpectedState as u32));
        assert!(matches!(out, TaskOutcome::Pending(delay) if delay == ADVANCE_EPOCH_PENDING_DELAY));
    }

    #[test]
    fn request_error_is_retryable() {
        let out = classify_advance_epoch_error(&RpcError::Request("timeout".into()));
        assert!(matches!(out, TaskOutcome::Retryable(_)));
    }
}
