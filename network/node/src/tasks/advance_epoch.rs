//! AdvanceEpoch — submit the advance_epoch instruction on-chain.

use std::sync::Arc;

use rpc::Rpc;
use solana_sdk::signer::Signer;
use store::Store;
use tape_api::instruction::build_advance_epoch_ix;
use tokio_util::sync::CancellationToken;

use crate::core::NodeContext;
use crate::supervisor::TaskOutcome;

pub async fn run<S: Store, R: Rpc>(
    context: Arc<NodeContext<S, R>>,
    cancel: CancellationToken,
) -> TaskOutcome {
    let pubkey = context.keypair.pubkey();
    let ix = build_advance_epoch_ix(pubkey, pubkey);

    let result = tokio::select! {
        r = context.rpc.send_instructions(&context.keypair, vec![ix]) => r,
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
