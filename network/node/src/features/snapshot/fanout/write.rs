//! Push our own snapshot write votes to group peers.
//!
//! Iterates every chunk we hold a vote for; skips chunks that have already
//! reached a supermajority.

use std::sync::Arc;

use rpc::Rpc;
use store::Store;
use tape_core::bft::is_supermajority;
use tape_core::erasure::SPOOL_GROUP_SIZE;
use tape_core::spooler::SpoolGroup;
use tape_core::types::EpochNumber;
use tape_protocol::api::{SnapshotVoteKind, SnapshotVoteReq};
use tape_protocol::Api;
use tape_store::ops::SnapshotOps;
use tokio_util::sync::CancellationToken;
use tracing::trace;

use crate::context::NodeContext;
use crate::core::error::NodeError;
use crate::features::snapshot::utils::group_peers_without;

pub async fn fanout_write_votes<Db, Cluster, Blockchain>(
    ctx: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    epoch: EpochNumber,
    cancel: &CancellationToken,
) -> Result<(), NodeError>
where
    Db: Store + 'static,
    Cluster: Api + 'static,
    Blockchain: Rpc + 'static,
{
    match cancel.run_until_cancelled(fanout_write_inner(ctx, epoch)).await {
        Some(result) => result,
        None => Ok(()),
    }
}

async fn fanout_write_inner<Db, Cluster, Blockchain>(
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
        let peers = group_peers_without(&state, group, me);

        let chunks = ctx
            .store
            .iter_snapshot_write_sigs(epoch, group)
            .map_err(|e| NodeError::Store(format!("iter_snapshot_write_sigs: {e}")))?;

        for chunk_sigs in chunks {
            if is_supermajority(chunk_sigs.votes.len() as u64, SPOOL_GROUP_SIZE as u64) {
                continue;
            }
            let Some((_, vote)) = chunk_sigs
                .votes
                .into_iter()
                .find(|(id, _)| *id == me)
            else {
                continue;
            };

            let req = SnapshotVoteReq {
                node_id: me,
                kind: SnapshotVoteKind::WriteChunk,
                message: vote.message.to_vec(),
                signature: vote.signature,
            };

            for peer in &peers {
                if let Err(error) = ctx.api.snapshot_vote(*peer, &req).await {
                    trace!(
                        ?error,
                        %peer,
                        %epoch,
                        group = group.0,
                        chunk = chunk_sigs.chunk.0,
                        "fanout: write vote push failed"
                    );
                }
            }
        }
    }

    Ok(())
}
