//! SyncEpoch — submit epoch sync attestation on-chain.

use std::sync::Arc;

use solana_sdk::signer::Signer;
use store::Store;
use tape_api::instruction::build_epoch_sync_ix;
use tape_api::program::tapedrive::node_pda;
use tape_store::ops::{MetaOps, SpoolOps};
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

    let epoch = match context.store.get_current_epoch() {
        Ok(Some(e)) => e,
        Ok(None) => return TaskOutcome::Retryable("no current epoch".into()),
        Err(e) => return TaskOutcome::Retryable(format!("get epoch: {e}")),
    };

    let owned_spools: Vec<u16> = match context.store.iter_all_spools() {
        Ok(spools) => spools.into_iter().map(|(id, _)| id).collect(),
        Err(e) => return TaskOutcome::Retryable(format!("iter spools: {e}")),
    };

    let pubkey = context.keypair.pubkey();
    let (node_address, _) = node_pda(pubkey);

    let ix = build_epoch_sync_ix(pubkey, pubkey, node_address, epoch, &owned_spools);

    match rpc.send_instructions(&context.keypair, vec![ix]).await {
        Ok(sig) => {
            tracing::info!(%sig, ?epoch, "sync_epoch submitted");
            TaskOutcome::Success
        }
        Err(e) => TaskOutcome::Retryable(format!("sync_epoch: {e}")),
    }
}
