//! AdvancePool — submit advance_pool instruction on-chain.

use std::sync::Arc;
use std::time::Duration;

use rpc::Rpc;
use store::Store;
use tape_api::errors::TapeError;
use tokio_util::sync::CancellationToken;

use crate::chain::submit_advance_pool;
use crate::core::NodeContext;
use crate::TaskOutcome;
use rpc_client::parse_tape_error;

const ADVANCE_POOL_PENDING_DELAY: Duration = Duration::from_secs(30);

pub async fn run<S: Store, R: Rpc>(
    context: Arc<NodeContext<S, R>>,
    cancel: CancellationToken,
) -> TaskOutcome {
    let result = tokio::select! {
        r = submit_advance_pool(&context) => r,
        _ = cancel.cancelled() => return TaskOutcome::Success,
    };
    match result {
        Ok(sig) => {
            tracing::info!(%sig, "advance_pool submitted");
            TaskOutcome::Success
        }
        Err(ref e) => match parse_tape_error(e) {
            Some(TapeError::AlreadyAdvanced) => {
                tracing::info!("advance_pool already completed");
                TaskOutcome::Success
            }
            Some(TapeError::BadEpochState) => TaskOutcome::Pending(ADVANCE_POOL_PENDING_DELAY),
            Some(TapeError::NoRewards) | Some(TapeError::RewardsOverflow) => {
                TaskOutcome::Permanent(format!("advance_pool: {e}"))
            }
            _ => TaskOutcome::Retryable(format!("advance_pool: {e}")),
        },
    }
}
