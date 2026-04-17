//! Runs the quorum-sign + submit pipeline for a single snapshot group's
//! finalize step.
//!
//! Fires once every chunk we built for `(epoch, group)` has landed on-chain
//! (see `SnapshotManager::on_snapshot_written`). Signs
//! `SnapshotSignMessage(epoch, group)`, collects a 14-of-20 BLS quorum from
//! the other group members via `quorum::collect`, and calls
//! [`submit_sign_snapshot`]. Submission failures are logged — if another
//! group member raced us, the capture path surfaces the on-chain event
//! independently.

use std::sync::Arc;

use rpc::Rpc;
use store::Store;
use tape_core::cert::SnapshotSignMessage;
use tape_core::spooler::SpoolGroup;
use tape_core::types::EpochNumber;
use tape_protocol::api::GetSnapshotFinalizeSigReq;
use tape_protocol::Api;
use tape_retry::RetryConfig;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::chain::sign_snapshot::submit_sign_snapshot;
use crate::context::NodeContext;
use crate::core::peer_call::call_peer;
use crate::features::snapshot::debug_journal;
use crate::features::snapshot::quorum::{self, PeerSig, PerPeer};

pub async fn run<Db: Store + 'static, Cluster: Api + 'static, Blockchain: Rpc + 'static>(
    ctx: Arc<NodeContext<Db, Cluster, Blockchain>>,
    epoch: EpochNumber,
    group: SpoolGroup,
    cancel: CancellationToken,
) {
    let message = SnapshotSignMessage::new(epoch, group).to_bytes();

    let req = Arc::new(GetSnapshotFinalizeSigReq { epoch, group });
    let per_peer = make_per_peer(ctx.clone(), req);

    let Some(quorum) = quorum::collect(
        &ctx,
        epoch,
        group,
        None,
        None,
        &message,
        per_peer,
        cancel,
        "finalize",
    )
    .await
    else {
        return;
    };

    let node_id = ctx.node_id();
    match submit_sign_snapshot(&ctx, epoch, group, quorum.bitmap, quorum.signature).await {
        Ok(txid) => {
            info!(
                epoch = epoch.0,
                group = group.0,
                ?txid,
                "snapshot finalize: group signed"
            );
            debug_journal::submit(
                node_id,
                "finalize",
                epoch,
                group,
                None,
                &quorum.bitmap,
                &quorum.signature,
                Ok(()),
            );
        }
        Err(error) => {
            let msg = error.to_string();
            warn!(
                error = %error,
                epoch = epoch.0,
                group = group.0,
                "snapshot finalize: submit failed (likely raced)"
            );
            debug_journal::submit(
                node_id,
                "finalize",
                epoch,
                group,
                None,
                &quorum.bitmap,
                &quorum.signature,
                Err(&msg),
            );
        }
    }
}

fn make_per_peer<Db: Store + 'static, Cluster: Api + 'static, Blockchain: Rpc + 'static>(
    ctx: Arc<NodeContext<Db, Cluster, Blockchain>>,
    req: Arc<GetSnapshotFinalizeSigReq>,
) -> PerPeer {
    Arc::new(move |node_id, cancel| {
        let ctx = ctx.clone();
        let req = req.clone();
        Box::pin(async move {
            let res = call_peer(
                &ctx.peer_manager,
                RetryConfig::none(),
                node_id,
                Some(&cancel),
                || ctx.api.get_snapshot_finalize_sig(node_id, &req),
            )
            .await?;
            Ok(PeerSig {
                node_id: res.node_id,
                signature: res.signature,
            })
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn run_skips_when_cancelled() {
        use crate::context::test_utils::test_context;

        let ctx = test_context();
        let cancel = CancellationToken::new();
        cancel.cancel();

        run(ctx, EpochNumber(1), SpoolGroup(0), cancel).await;
    }
}
