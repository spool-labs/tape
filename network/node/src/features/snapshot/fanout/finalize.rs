//! Push our own snapshot finalize vote to group peers (one per group).

use std::sync::Arc;

use rpc::Rpc;
use store::Store;
use tape_core::bft::is_supermajority;
use tape_core::erasure::GROUP_SIZE;
use tape_core::spooler::GroupIndex;
use tape_core::types::EpochNumber;
use tape_protocol::api::{SnapshotVoteKind, SnapshotVoteReq};
use tape_protocol::Api;
use tape_store::ops::SnapshotOps;
use tokio_util::sync::CancellationToken;
use tracing::trace;

use crate::context::NodeContext;
use crate::core::error::NodeError;
use crate::features::snapshot::utils::group_peers_without;

pub async fn fanout_finalize_votes<Db, Cluster, Blockchain>(
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
    let me = ctx.node_address();

    if state.find_member(me).is_none() {
        return Ok(());
    }

    for spool in state.member_spools(me) {
        let group = GroupIndex::containing(spool);
        let sigs = ctx
            .store
            .iter_snapshot_finalize_sigs(epoch, group)
            .map_err(|e| NodeError::Store(format!("iter_snapshot_finalize_sigs: {e}")))?;

        if is_supermajority(sigs.len() as u64, GROUP_SIZE as u64) {
            continue;
        }

        let Some((_, vote)) = sigs.into_iter().find(|(id, _)| *id == me) else {
            continue;
        };

        let peers = group_peers_without(&state, group, me);

        let req = SnapshotVoteReq {
            node_id: me,
            kind: SnapshotVoteKind::CompleteGroup,
            message: vote.message.to_vec(),
            signature: vote.signature,
        };

        for peer in &peers {
            if let Err(error) = ctx.api.snapshot_vote(*peer, &req).await {
                trace!(?error, %peer, %epoch, group = group.0, "fanout: finalize vote push failed");
            }
        }
    }

    Ok(())
}
