//! AdvancePool — submit advance_pool instruction on-chain.

use std::sync::Arc;
use std::time::Duration;

use rpc::{Rpc, RpcError};
use store::Store;
use tape_protocol::Api;
use tape_api::errors::TapeError;
use tokio_util::sync::CancellationToken;

use crate::chain::submit_advance_pool;
use crate::core::NodeContext;
use crate::TaskOutcome;
use rpc_client::parse_tape_error;

const ADVANCE_POOL_PENDING_DELAY: Duration = Duration::from_secs(5);

pub async fn run<Db: Store, Cluster: Api, Blockchain: Rpc>(
    context: Arc<NodeContext<Db, Cluster, Blockchain>>,
    cancel: CancellationToken,
) -> TaskOutcome {
    let result = tokio::select! {
        r = submit_advance_pool(&context) => r,
        _ = cancel.cancelled() => return TaskOutcome::Success,
    };
    let had_error = result.is_err();
    let outcome = match result {
        Ok(sig) => {
            tracing::info!(%sig, "advance_pool submitted");
            TaskOutcome::Success
        }
        Err(ref e) => classify_advance_pool_error(e),
    };

    if matches!(outcome, TaskOutcome::Success) && had_error {
        tracing::info!("advance_pool already completed");
    }

    outcome
}

fn classify_advance_pool_error(err: &RpcError) -> TaskOutcome {
    match parse_tape_error(err) {
        Some(TapeError::AlreadyAdvanced) => TaskOutcome::Success,
        Some(TapeError::BadEpochState) => TaskOutcome::Pending(ADVANCE_POOL_PENDING_DELAY),
        Some(TapeError::NoRewards) | Some(TapeError::RewardsOverflow) => {
            TaskOutcome::Permanent(format!("advance_pool: {err}"))
        }
        _ => TaskOutcome::Retryable(format!("advance_pool: {err}")),
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    fn tx_error(code: u32) -> RpcError {
        RpcError::Transaction(format!("custom program error: 0x{code:x}"))
    }

    #[test]
    fn already_advanced_is_success() {
        let out = classify_advance_pool_error(&tx_error(TapeError::AlreadyAdvanced as u32));
        assert!(matches!(out, TaskOutcome::Success));
    }

    #[test]
    fn bad_epoch_state_is_pending() {
        let out = classify_advance_pool_error(&tx_error(TapeError::BadEpochState as u32));
        assert!(matches!(out, TaskOutcome::Pending(delay) if delay == ADVANCE_POOL_PENDING_DELAY));
    }

    #[test]
    fn no_rewards_is_permanent() {
        let out = classify_advance_pool_error(&tx_error(TapeError::NoRewards as u32));
        assert!(matches!(out, TaskOutcome::Permanent(_)));
    }

    #[test]
    fn rewards_overflow_is_permanent() {
        let out = classify_advance_pool_error(&tx_error(TapeError::RewardsOverflow as u32));
        assert!(matches!(out, TaskOutcome::Permanent(_)));
    }

    #[test]
    fn request_error_is_retryable() {
        let out = classify_advance_pool_error(&RpcError::Request("timeout".into()));
        assert!(matches!(out, TaskOutcome::Retryable(_)));
    }
}
