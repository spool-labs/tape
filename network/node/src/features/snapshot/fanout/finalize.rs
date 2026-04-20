//! Push our own snapshot finalize partial to group peers (one per group).

use std::sync::Arc;

use rpc::Rpc;
use store::Store;
use tape_core::bft::is_supermajority;
use tape_core::erasure::SPOOL_GROUP_SIZE;
use tape_core::spooler::SpoolGroup;
use tape_core::types::EpochNumber;
use tape_protocol::api::{SignatureKind, SnapshotSigReq};
use tape_protocol::Api;
use tape_store::ops::SnapshotOps;
use tokio_util::sync::CancellationToken;
use tracing::trace;

use crate::context::NodeContext;
use crate::core::error::NodeError;
use crate::features::snapshot::utils::group_peers_without;

pub async fn fanout_finalize_sigs<Db, Cluster, Blockchain>(
    ctx: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    epoch: EpochNumber,
    cancel: &CancellationToken,
) -> Result<(), NodeError>
where
    Db: Store + 'static,
    Cluster: Api + 'static,
    Blockchain: Rpc + 'static,
{
    match cancel.run_until_cancelled(fanout_finalize_inner(ctx, epoch)).await {
        Some(result) => result,
        None => Ok(()),
    }
}

async fn fanout_finalize_inner<Db, Cluster, Blockchain>(
    ctx: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    epoch: EpochNumber,
) -> Result<(), NodeError>
where
    Db: Store + 'static,
    Cluster: Api + 'static,
    Blockchain: Rpc + 'static,
{
    let state = ctx.state();
    let me = ctx.node_id();

    let Some((member_index, _)) = state.find_member(me) else { return Ok(()); };

    for spool in state.member_spools(member_index) {
        let group = SpoolGroup::of(spool);
        let sigs = ctx
            .store
            .iter_snapshot_finalize_sigs(epoch, group)
            .map_err(|e| NodeError::Store(format!("iter_snapshot_finalize_sigs: {e}")))?;

        if is_supermajority(sigs.len() as u64, SPOOL_GROUP_SIZE as u64) {
            continue;
        }

        let Some((_, vote)) = sigs.into_iter().find(|(id, _)| *id == me) else {
            continue;
        };

        let peers = group_peers_without(&state, group, me);

        let req = SnapshotSigReq {
            node_id: me,
            kind: SignatureKind::Finalize,
            message: vote.message.to_vec(),
            signature: vote.signature,
        };

        for peer in &peers {
            if let Err(error) = ctx.api.snapshot_sig(*peer, &req).await {
                trace!(?error, %peer, %epoch, group = group.0, "fanout: finalize sig push failed");
            }
        }
    }

    Ok(())
}
