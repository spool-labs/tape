//! RefreshOnchainState — fetch and cache current on-chain state.

use std::sync::Arc;

use store::Store;
use tape_store::ops::MetaOps;
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

    // Fetch epoch account for current epoch number
    let epoch_account = match rpc.get_epoch().await {
        Ok(e) => e,
        Err(e) => return TaskOutcome::Retryable(format!("get_epoch: {e}")),
    };

    let epoch = epoch_account.id;
    if let Err(e) = context.store.set_current_epoch(epoch) {
        return TaskOutcome::Retryable(format!("set_current_epoch: {e}"));
    }

    tracing::info!(?epoch, "refreshed on-chain state");
    TaskOutcome::Success
}
