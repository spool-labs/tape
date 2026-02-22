//! AdvanceEpoch — submit the advance_epoch instruction on-chain.

use std::sync::Arc;

use rpc::Rpc;
use store::Store;
use tokio_util::sync::CancellationToken;

use crate::chain::submit_advance_epoch;
use crate::core::NodeContext;
use crate::TaskOutcome;

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
        Err(e) => TaskOutcome::Retryable(format!("advance_epoch: {e}")),
    }
}
