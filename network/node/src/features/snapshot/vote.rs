//! Create this node's snapshot votes from staged local artifacts.

use std::sync::Arc;

use rpc::Rpc;
use store::Store;
use tape_core::cert::{SnapshotSignMessage, SnapshotWriteMessage};
use tape_core::spooler::GroupIndex;
use tape_core::types::SpoolIndex;
use tape_core::types::EpochNumber;
use tape_protocol::Api;
use tape_store::ops::SnapshotOps;
use tape_store::types::{SnapshotFinalizeVote, SnapshotWriteVote};
use tokio_util::sync::CancellationToken;

use crate::context::NodeContext;
use crate::core::error::NodeError;

#[derive(Debug, Default)]
pub struct VoteSummary {
    pub write_votes: usize,
    pub finalize_votes: usize,
}

pub async fn create_snapshot_write_votes<Db, Cluster, Blockchain>(
    ctx: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    epoch: EpochNumber,
    cancel: &CancellationToken,
) -> Result<VoteSummary, NodeError>
where
    Db: Store + 'static,
    Cluster: Api + 'static,
    Blockchain: Rpc + 'static,
{
    let owned_ctx = ctx.clone();
    let task =
        tokio::task::spawn_blocking(move || create_snapshot_write_votes_blocking(&owned_ctx, epoch));

    tokio::select! {
        result = task => result
            .map_err(|e| NodeError::Store(format!("create_snapshot_write_votes task join: {e}")))?,
        _ = cancel.cancelled() =>
            Err(NodeError::Store(format!("create_snapshot_write_votes({epoch}): cancelled"))),
    }
}

fn create_snapshot_write_votes_blocking<Db, Cluster, Blockchain>(
    ctx: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    epoch: EpochNumber,
) -> Result<VoteSummary, NodeError>
where
    Db: Store + 'static,
    Cluster: Api + 'static,
    Blockchain: Rpc + 'static,
{
    let state = ctx.state();
    let me = ctx.node_address();

    if state.find_member(me).is_none() {
        return Ok(VoteSummary::default());
    }

    let groups = member_groups(&state.member_spools(me));
    let mut write_votes = 0usize;

    for group in groups {
        let artifacts = ctx
            .store
            .iter_snapshot_artifacts(epoch, group)
            .map_err(store_err("iter_snapshot_artifacts"))?;

        for (chunk, artifact) in artifacts {
            let write_message =
                SnapshotWriteMessage::new(epoch, group, chunk, artifact.blob.get_hash()).to_bytes();
            let write_sig = ctx
                .bls_sign(&write_message)
                .map_err(|e| NodeError::Store(format!("write bls_sign: {e:?}")))?;

            ctx.store
                .put_snapshot_write_sig(
                    epoch,
                    group,
                    chunk,
                    me,
                    &SnapshotWriteVote {
                        message: write_message,
                        signature: write_sig,
                    },
                )
                .map_err(store_err("put_snapshot_write_sig"))?;

            write_votes += 1;
        }
    }

    Ok(VoteSummary {
        write_votes,
        finalize_votes: 0,
    })
}

pub async fn create_snapshot_finalize_votes<Db, Cluster, Blockchain>(
    ctx: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    epoch: EpochNumber,
    cancel: &CancellationToken,
) -> Result<VoteSummary, NodeError>
where
    Db: Store + 'static,
    Cluster: Api + 'static,
    Blockchain: Rpc + 'static,
{
    let owned_ctx = ctx.clone();
    let task = tokio::task::spawn_blocking(move || {
        create_snapshot_finalize_votes_blocking(&owned_ctx, epoch)
    });

    tokio::select! {
        result = task => result
            .map_err(|e| NodeError::Store(format!("create_snapshot_finalize_votes task join: {e}")))?,
        _ = cancel.cancelled() =>
            Err(NodeError::Store(format!("create_snapshot_finalize_votes({epoch}): cancelled"))),
    }
}

fn create_snapshot_finalize_votes_blocking<Db, Cluster, Blockchain>(
    ctx: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    epoch: EpochNumber,
) -> Result<VoteSummary, NodeError>
where
    Db: Store + 'static,
    Cluster: Api + 'static,
    Blockchain: Rpc + 'static,
{
    let state = ctx.state();
    let me = ctx.node_address();

    if state.find_member(me).is_none() {
        return Ok(VoteSummary::default());
    }

    let groups = member_groups(&state.member_spools(me));
    let mut finalize_votes = 0usize;

    for group in groups {
        let finalize_message = SnapshotSignMessage::new(epoch, group).to_bytes();
        let finalize_sig = ctx
            .bls_sign(&finalize_message)
            .map_err(|e| NodeError::Store(format!("finalize bls_sign: {e:?}")))?;

        ctx.store
            .put_snapshot_finalize_sig(
                epoch,
                group,
                me,
                &SnapshotFinalizeVote {
                    message: finalize_message,
                    signature: finalize_sig,
                },
            )
            .map_err(store_err("put_snapshot_finalize_sig"))?;

        finalize_votes += 1;
    }

    Ok(VoteSummary {
        write_votes: 0,
        finalize_votes,
    })
}

fn member_groups(spools: &[SpoolIndex]) -> Vec<GroupIndex> {
    let mut groups = spools.iter().copied().map(GroupIndex::containing).collect::<Vec<_>>();
    groups.sort_by_key(|group| group.0);
    groups.dedup_by_key(|group| group.0);
    groups
}

fn store_err<E: std::fmt::Display>(op: &'static str) -> impl FnOnce(E) -> NodeError {
    move |e| NodeError::Store(format!("{op}: {e}"))
}
