//! Submit collected snapshot partials to the chain when a supermajority exists.
//!
//! These are the polled half of the push-based flow: the HTTP handler lands
//! partials into the store; on each heartbeat (and on relevant block events)
//! the manager calls these helpers, which scan for chunks/groups that have
//! crossed the supermajority threshold, aggregate the partials, and submit.

use std::sync::Arc;

use rpc::Rpc;
use store::Store;
use tape_core::bft::is_supermajority;
use tape_core::bls::BlsSignature;
use tape_core::erasure::SPOOL_GROUP_SIZE;
use tape_core::types::{EpochNumber, SpoolGroupBitmap};
use tape_protocol::Api;
use tape_store::ops::SnapshotOps;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

use crate::chain::{submit_sign_snapshot, submit_write_snapshot};
use crate::context::NodeContext;
use crate::core::error::NodeError;
use crate::features::snapshot::utils::local_groups;

/// For every group we're a member of, submit the `WriteSnapshot` instruction
/// for any chunk where we hold a supermajority and still have the local
/// artifact (we need the blob to build the transaction).
pub async fn submit_ready_writes<Db, Cluster, Blockchain>(
    ctx: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    epoch: EpochNumber,
    cancel: &CancellationToken,
) -> Result<(), NodeError>
where
    Db: Store + 'static,
    Cluster: Api + 'static,
    Blockchain: Rpc + 'static,
{
    match cancel.run_until_cancelled(submit_ready_writes_inner(ctx, epoch)).await {
        Some(result) => result,
        None => Ok(()),
    }
}

async fn submit_ready_writes_inner<Db, Cluster, Blockchain>(
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
        let chunks = ctx
            .store
            .iter_snapshot_write_sigs(epoch, group)
            .map_err(|e| NodeError::Store(format!("iter_snapshot_write_sigs: {e}")))?;

        for chunk_sigs in chunks {
            let chunk = chunk_sigs.chunk;
            if !is_supermajority(chunk_sigs.votes.len() as u64, SPOOL_GROUP_SIZE as u64) {
                continue;
            }

            let Some(artifact) = ctx
                .store
                .get_snapshot_artifact(epoch, group, chunk)
                .map_err(|e| NodeError::Store(format!("get_snapshot_artifact: {e}")))?
            else {
                // Another group member holds the blob; let them submit.
                continue;
            };

            let indices: Vec<usize> = chunk_sigs
                .votes
                .iter()
                .map(|(i, _)| *i as usize)
                .collect();
            let bitmap = SpoolGroupBitmap::from_indices(&indices, SPOOL_GROUP_SIZE);
            let partials: Vec<BlsSignature> = chunk_sigs
                .votes
                .into_iter()
                .map(|(_, vote)| vote.signature)
                .collect();
            let aggregate = BlsSignature::aggregate(&partials)
                .map_err(|e| NodeError::Store(format!("aggregate write sigs: {e:?}")))?;

            match submit_write_snapshot(ctx, epoch, group, chunk, bitmap, aggregate, &artifact.blob)
                .await
            {
                Ok(txid) => info!(
                    %epoch,
                    group = group.0,
                    chunk = chunk.0,
                    ?txid,
                    "snapshot: write submitted"
                ),
                Err(error) => debug!(
                    ?error,
                    %epoch,
                    group = group.0,
                    chunk = chunk.0,
                    "snapshot: write submit raced / failed"
                ),
            }
        }
    }

    Ok(())
}

/// For every group we're a member of, submit the `SignSnapshot` instruction
/// if the group has a supermajority of finalize partials.
pub async fn submit_ready_finalizes<Db, Cluster, Blockchain>(
    ctx: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    epoch: EpochNumber,
    cancel: &CancellationToken,
) -> Result<(), NodeError>
where
    Db: Store + 'static,
    Cluster: Api + 'static,
    Blockchain: Rpc + 'static,
{
    match cancel.run_until_cancelled(submit_ready_finalizes_inner(ctx, epoch)).await {
        Some(result) => result,
        None => Ok(()),
    }
}

async fn submit_ready_finalizes_inner<Db, Cluster, Blockchain>(
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
        let sigs = ctx
            .store
            .iter_snapshot_finalize_sigs(epoch, group)
            .map_err(|e| NodeError::Store(format!("iter_snapshot_finalize_sigs: {e}")))?;

        if !is_supermajority(sigs.len() as u64, SPOOL_GROUP_SIZE as u64) {
            continue;
        }

        let indices: Vec<usize> = sigs.iter().map(|(i, _)| *i as usize).collect();
        let bitmap = SpoolGroupBitmap::from_indices(&indices, SPOOL_GROUP_SIZE);
        let partials: Vec<BlsSignature> = sigs.into_iter().map(|(_, vote)| vote.signature).collect();
        let aggregate = BlsSignature::aggregate(&partials)
            .map_err(|e| NodeError::Store(format!("aggregate finalize sigs: {e:?}")))?;

        match submit_sign_snapshot(ctx, epoch, group, bitmap, aggregate).await {
            Ok(txid) => info!(
                %epoch,
                group = group.0,
                ?txid,
                "snapshot: finalize submitted"
            ),
            Err(error) => debug!(
                ?error,
                %epoch,
                group = group.0,
                "snapshot: finalize submit raced / failed"
            ),
        }
    }

    Ok(())
}
