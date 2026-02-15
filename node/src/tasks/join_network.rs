//! JoinNetwork — submit join_network instruction on-chain.

use std::sync::Arc;

use solana_sdk::signer::Signer;
use store::Store;
use tape_api::instruction::build_join_network_ix;
use tape_api::program::tapedrive::node_pda;
use tokio_util::sync::CancellationToken;

use crate::core::NodeContext;
use crate::supervisor::TaskOutcome;

pub async fn run<S: Store>(
    context: Arc<NodeContext<S>>,
    cancel: CancellationToken,
) -> TaskOutcome {
    let _ = &cancel;
    let rpc = match context.rpc.as_ref() {
        Some(r) => r,
        None => return TaskOutcome::Permanent("no rpc client".into()),
    };

    let pubkey = context.keypair.pubkey();
    let (node_address, _) = node_pda(pubkey);

    let ix = build_join_network_ix(pubkey, pubkey, node_address);

    match rpc.send_instructions(&context.keypair, vec![ix]).await {
        Ok(sig) => {
            tracing::info!(%sig, "join_network submitted");
            TaskOutcome::Success
        }
        Err(e) => TaskOutcome::Retryable(format!("join_network: {e}")),
    }
}
