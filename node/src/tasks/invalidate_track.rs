//! InvalidateTrack — submit track invalidation on-chain.

use std::sync::Arc;

use solana_sdk::pubkey::Pubkey;
use store::Store;
use tokio_util::sync::CancellationToken;

use crate::core::NodeContext;
use crate::supervisor::TaskOutcome;

pub async fn run<S: Store>(
    context: Arc<NodeContext<S>>,
    track: Pubkey,
    cancel: CancellationToken,
) -> TaskOutcome {
    let _rpc = match context.rpc.as_ref() {
        Some(r) => r,
        None => return TaskOutcome::Permanent("no rpc client".into()),
    };
    let _ = (&track, &cancel);
    // TODO: build and send invalidate_track instruction
    TaskOutcome::Success
}
