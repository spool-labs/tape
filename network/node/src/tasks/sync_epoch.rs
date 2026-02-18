//! SyncEpoch — submit epoch sync attestation on-chain.

use std::sync::Arc;

use rpc::Rpc;
use solana_sdk::signer::Signer;
use store::Store;
use tape_api::errors::TapeError;
use tape_api::instruction::build_epoch_sync_ix;
use tape_api::program::tapedrive::node_pda;
use tape_store::ops::{MetaOps, SpoolOps};
use tokio_util::sync::CancellationToken;

use crate::core::NodeContext;
use crate::supervisor::TaskOutcome;
use crate::tasks::parse_tape_error;

pub async fn run<S: Store, R: Rpc>(
    context: Arc<NodeContext<S, R>>,
    cancel: CancellationToken,
) -> TaskOutcome {
    let epoch = match context.store.get_current_epoch() {
        Ok(Some(e)) => e,
        Ok(None) => return TaskOutcome::Retryable("no current epoch".into()),
        Err(e) => return TaskOutcome::Retryable(format!("get epoch: {e}")),
    };

    let mut owned_spools: Vec<u16> = match context.store.iter_all_spools() {
        Ok(spools) => spools.into_iter().map(|(id, _)| id).collect(),
        Err(e) => return TaskOutcome::Retryable(format!("iter spools: {e}")),
    };
    owned_spools.sort_unstable();

    if cancel.is_cancelled() {
        return TaskOutcome::Success;
    }

    let pubkey = context.keypair.pubkey();
    let node_id = context.node_id();
    let (node_address, _) = node_pda(pubkey);

    let ix = build_epoch_sync_ix(pubkey, pubkey, node_address, epoch, &owned_spools);

    let result = tokio::select! {
        r = context.rpc.send_instructions(&context.keypair, vec![ix]) => r,
        _ = cancel.cancelled() => return TaskOutcome::Success,
    };
    match result {
        Ok(sig) => {
            tracing::info!(%sig, ?epoch, node_id = node_id, "sync_epoch submitted");
            TaskOutcome::Success
        }
        Err(ref e) => match parse_tape_error(e) {
            Some(TapeError::AlreadySynced) => {
                tracing::info!(node_id = node_id, "sync_epoch already completed");
                TaskOutcome::Success
            }
            _ => {
                tracing::warn!(node_id = node_id, error = %e, "sync_epoch submission failed");
                TaskOutcome::Retryable(format!("sync_epoch: {e}"))
            }
        },
    }
}
