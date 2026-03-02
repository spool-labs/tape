//! JoinNetwork — submit join_network instruction on-chain.

use std::sync::Arc;
use std::time::Duration;

use rpc::{Rpc, RpcError};
use solana_sdk::signature::Signer;
use store::Store;
use tape_api::errors::TapeError;
use tokio_util::sync::CancellationToken;

use crate::chain::submit_join_network;
use crate::core::NodeContext;
use crate::TaskOutcome;
use rpc_client::parse_tape_error;

const JOIN_NETWORK_PENDING_DELAY: Duration = Duration::from_secs(5);

fn classify_join_network_error(
    err: &RpcError,
    joined_check: Option<Result<bool, String>>,
) -> TaskOutcome {
    match parse_tape_error(err) {
        Some(TapeError::UnexpectedState) => match joined_check {
            Some(Ok(true)) => TaskOutcome::Success,
            Some(Ok(false)) => TaskOutcome::Pending(JOIN_NETWORK_PENDING_DELAY),
            Some(Err(check_err)) => TaskOutcome::Retryable(format!(
                "join_network: {err}; verify committee_next failed: {check_err}"
            )),
            None => TaskOutcome::Retryable(format!("join_network: {err}")),
        },
        Some(TapeError::NodeStale) => TaskOutcome::Pending(JOIN_NETWORK_PENDING_DELAY),
        Some(TapeError::NotStaked) => TaskOutcome::Permanent(format!("join_network: {err}")),
        _ => TaskOutcome::Retryable(format!("join_network: {err}")),
    }
}

async fn already_joined<S: Store, R: Rpc>(context: &NodeContext<S, R>) -> Result<bool, String> {
    let authority = context.keypair.pubkey();
    let node = context
        .rpc
        .get_node(&authority)
        .await
        .map_err(|e| format!("get_node: {e}"))?;
    let system = context
        .rpc
        .get_system()
        .await
        .map_err(|e| format!("get_system: {e}"))?;
    Ok(system.committee_next.index_of(&node.id).is_some())
}

pub async fn run<S: Store, R: Rpc>(
    context: Arc<NodeContext<S, R>>,
    cancel: CancellationToken,
) -> TaskOutcome {
    let result = tokio::select! {
        r = submit_join_network(&context) => r,
        _ = cancel.cancelled() => return TaskOutcome::Success,
    };
    match result {
        Ok(sig) => {
            tracing::info!(%sig, "join_network submitted");
            TaskOutcome::Success
        }
        Err(ref e) => {
            let joined_check = if matches!(parse_tape_error(e), Some(TapeError::UnexpectedState)) {
                Some(already_joined(context.as_ref()).await)
            } else {
                None
            };
            classify_join_network_error(e, joined_check)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tx_error(code: u32) -> RpcError {
        RpcError::Transaction(format!("custom program error: 0x{code:x}"))
    }

    #[test]
    fn unexpected_state_joined_is_success() {
        let out = classify_join_network_error(
            &tx_error(TapeError::UnexpectedState as u32),
            Some(Ok(true)),
        );
        assert!(matches!(out, TaskOutcome::Success));
    }

    #[test]
    fn unexpected_state_not_joined_is_pending() {
        let out = classify_join_network_error(
            &tx_error(TapeError::UnexpectedState as u32),
            Some(Ok(false)),
        );
        assert!(matches!(out, TaskOutcome::Pending(delay) if delay == JOIN_NETWORK_PENDING_DELAY));
    }

    #[test]
    fn unexpected_state_check_failure_is_retryable() {
        let out = classify_join_network_error(
            &tx_error(TapeError::UnexpectedState as u32),
            Some(Err("boom".into())),
        );
        let TaskOutcome::Retryable(msg) = out else {
            panic!("expected retryable");
        };
        assert!(msg.contains("verify committee_next failed: boom"));
    }

    #[test]
    fn node_stale_is_pending() {
        let out = classify_join_network_error(&tx_error(TapeError::NodeStale as u32), None);
        assert!(matches!(out, TaskOutcome::Pending(delay) if delay == JOIN_NETWORK_PENDING_DELAY));
    }

    #[test]
    fn not_staked_is_permanent() {
        let out = classify_join_network_error(&tx_error(TapeError::NotStaked as u32), None);
        assert!(matches!(out, TaskOutcome::Permanent(_)));
    }

    #[test]
    fn request_error_is_retryable() {
        let out = classify_join_network_error(&RpcError::Request("timeout".into()), None);
        assert!(matches!(out, TaskOutcome::Retryable(_)));
    }
}
