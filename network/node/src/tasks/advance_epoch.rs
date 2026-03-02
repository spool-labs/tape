//! AdvanceEpoch — submit the advance_epoch instruction on-chain.

use std::sync::Arc;
use std::time::Duration;

use rpc::Rpc;
use rpc_client::parse_tape_error;
use store::Store;
use tape_api::errors::TapeError;
use tokio_util::sync::CancellationToken;

use crate::chain::submit_advance_epoch;
use crate::core::NodeContext;
use crate::TaskOutcome;

const ADVANCE_EPOCH_PENDING_DELAY: Duration = Duration::from_secs(30);

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
        Err(ref e) => match parse_tape_error(e) {
            Some(TapeError::TooSoon)
            | Some(TapeError::InsufficientCommittee)
            | Some(TapeError::SnapshotIncomplete)
            | Some(TapeError::BadEpochState) => {
                TaskOutcome::Pending(ADVANCE_EPOCH_PENDING_DELAY)
            }
            Some(TapeError::BadSchedule) => {
                TaskOutcome::Permanent(format!("advance_epoch: {e}"))
            }
            _ => TaskOutcome::Retryable(format!("advance_epoch: {e}")),
        },
    }
}
