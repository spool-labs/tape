//! JoinNetwork — submit join_network instruction on-chain.

use std::sync::Arc;

use rpc::Rpc;
use solana_sdk::signer::Signer;
use store::Store;
use tape_api::errors::TapeError;
use tape_api::instruction::build_join_network_ix;
use tape_api::program::tapedrive::node_pda;
use tokio_util::sync::CancellationToken;

use crate::core::NodeContext;
use crate::supervisor::TaskOutcome;
use crate::tasks::parse_tape_error;

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
    let pubkey = context.keypair.pubkey();
    let (node_address, _) = node_pda(pubkey);

    let ix = build_join_network_ix(pubkey, pubkey, node_address);

    let result = tokio::select! {
        r = context.rpc.send_instructions(&context.keypair, vec![ix]) => r,
        _ = cancel.cancelled() => return TaskOutcome::Success,
    };
    match result {
        Ok(sig) => {
            tracing::info!(%sig, "join_network submitted");
            TaskOutcome::Success
        }
        Err(ref e) => match parse_tape_error(e) {
            Some(TapeError::UnexpectedState) => {
                match already_joined(context.as_ref()).await {
                    Ok(true) => {
                        tracing::info!("join_network already completed");
                        TaskOutcome::Success
                    }
                    Ok(false) => TaskOutcome::Retryable(format!("join_network: {e}")),
                    Err(check_err) => {
                        TaskOutcome::Retryable(format!("join_network: {e}; verify committee_next failed: {check_err}"))
                    }
                }
            }
            _ => TaskOutcome::Retryable(format!("join_network: {e}")),
        },
    }
}
