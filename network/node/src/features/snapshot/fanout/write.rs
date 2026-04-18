//! Push our own snapshot write partials to group peers.
//!
//! Iterates every chunk we hold a sig for; skips chunks that have already
//! reached a supermajority.

use std::sync::Arc;

use rpc::Rpc;
use store::Store;
use tape_core::bft::is_supermajority;
use tape_core::erasure::SPOOL_GROUP_SIZE;
use tape_core::types::EpochNumber;
use tape_protocol::api::{SignatureKind, SnapshotSigReq};
use tape_protocol::Api;
use tape_store::ops::SnapshotOps;
use tokio_util::sync::CancellationToken;
use tracing::trace;

use crate::context::NodeContext;
use crate::core::error::NodeError;
use crate::features::snapshot::utils::{
    bitmap_index_in_group, group_peers_without, local_groups,
};

pub async fn fanout_write_sigs<Db, Cluster, Blockchain>(
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

    for group in local_groups(&state, me) {
        let Some(my_index) = bitmap_index_in_group(&state, group, me) else {
            continue;
        };
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
                .find(|(i, _)| *i == my_index)
            else {
                continue;
            };

            let req = SnapshotSigReq {
                node_id: me,
                kind: SignatureKind::Write,
                message: vote.message.to_vec(),
                signature: vote.signature,
            };

            for peer in &peers {
                if let Err(error) = ctx.api.snapshot_sig(*peer, &req).await {
                    trace!(
                        ?error,
                        %peer,
                        %epoch,
                        group = group.0,
                        chunk = chunk_sigs.chunk.0,
                        "fanout: write sig push failed"
                    );
                }
            }
        }
    }

    Ok(())
}
