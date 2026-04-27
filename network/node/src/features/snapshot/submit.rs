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

use crate::chain::{submit_reserve_snapshot, submit_sign_snapshot, submit_write_snapshot};
use crate::context::NodeContext;
use crate::core::chain_tx::{TxOutcome, submit_if_at_tip};
use crate::core::error::NodeError;
use crate::features::snapshot::utils::bitmap_index_in_group;
use tape_core::spooler::SpoolGroup;

/// Heartbeat-driven reserve attempt for `snapshot_epoch`. Skips if we've
/// already observed (or successfully submitted) a reserve for this epoch.
pub async fn submit_ready_reserves<Db, Cluster, Blockchain>(
    ctx: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    snapshot_epoch: EpochNumber,
    last_reserved: &mut Option<EpochNumber>,
    cancel: &CancellationToken,
) -> Result<(), NodeError>
where
    Db: Store + 'static,
    Cluster: Api + 'static,
    Blockchain: Rpc + 'static,
{
    if last_reserved.is_some_and(|reserved| reserved >= snapshot_epoch) {
        return Ok(());
    }

    if cancel.is_cancelled() {
        return Ok(());
    }

    let outcome = submit_if_at_tip(
        &ctx.ingest,
        submit_reserve_snapshot(ctx, snapshot_epoch),
    )
    .await;

    match outcome {
        TxOutcome::Confirmed(txid) => {
            info!(epoch = snapshot_epoch.0, ?txid, "snapshot: reserve submitted");
            *last_reserved = Some(match *last_reserved {
                Some(prev) => prev.max(snapshot_epoch),
                None => snapshot_epoch,
            });
        }
        TxOutcome::Program(err) => {
            debug!(epoch = snapshot_epoch.0, ?err, "snapshot: reserve raced / program error");
        }
        TxOutcome::Transport(err) => {
            debug!(epoch = snapshot_epoch.0, %err, "snapshot: reserve transport error");
        }
        TxOutcome::SkippedStale => {
            debug!(epoch = snapshot_epoch.0, "snapshot: reserve deferred, ingest stale");
        }
    }

    Ok(())
}

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

    let Some((member_index, _)) = state.find_member(me) else { return Ok(()); };

    for spool in state.member_spools(member_index) {
        let group = SpoolGroup::of(spool);
        let chunks = ctx
            .store
            .iter_snapshot_write_sigs(epoch, group)
            .map_err(|e| NodeError::Store(format!("iter_snapshot_write_sigs: {e}")))?;

        for chunk_sigs in chunks {
            let chunk = chunk_sigs.chunk;

            // Re-derive each vote's bitmap position from live committee state;
            // drop votes from signers no longer in this group.
            let mut indices: Vec<usize> = Vec::with_capacity(chunk_sigs.votes.len());
            let mut partials: Vec<BlsSignature> = Vec::with_capacity(chunk_sigs.votes.len());
            for (signer, vote) in chunk_sigs.votes {
                let Some(idx) = bitmap_index_in_group(&state, group, signer) else {
                    continue;
                };
                indices.push(idx as usize);
                partials.push(vote.signature);
            }

            if !is_supermajority(partials.len() as u64, SPOOL_GROUP_SIZE as u64) {
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

            let bitmap = SpoolGroupBitmap::from_indices(&indices, SPOOL_GROUP_SIZE);
            let aggregate = BlsSignature::aggregate(&partials)
                .map_err(|e| NodeError::Store(format!("aggregate write sigs: {e:?}")))?;

            let outcome = submit_if_at_tip(
                &ctx.ingest,
                submit_write_snapshot(ctx, epoch, group, chunk, bitmap, aggregate, &artifact.blob),
            )
            .await;

            match outcome {
                TxOutcome::Confirmed(txid) => {
                    info!(%epoch, group = group.0, chunk = chunk.0, ?txid, "snapshot: write submitted")
                }
                TxOutcome::Program(err) => {
                    debug!(?err, %epoch, group = group.0, chunk = chunk.0, "snapshot: write program error")
                }
                TxOutcome::Transport(err) => {
                    debug!(%err, %epoch, group = group.0, chunk = chunk.0, "snapshot: write transport error")
                }
                TxOutcome::SkippedStale => {
                    debug!(%epoch, group = group.0, chunk = chunk.0, "snapshot: write deferred, ingest stale")
                }
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

    let Some((member_index, _)) = state.find_member(me) else {
        return Ok(());
    };

    for spool in state.member_spools(member_index) {
        let group = SpoolGroup::of(spool);
        let sigs = ctx
            .store
            .iter_snapshot_finalize_sigs(epoch, group)
            .map_err(|e| NodeError::Store(format!("iter_snapshot_finalize_sigs: {e}")))?;

        // Re-derive each vote's bitmap position from committee state;
        // drop votes from signers no longer in this group.
        let mut indices: Vec<usize> = Vec::with_capacity(sigs.len());
        let mut partials: Vec<BlsSignature> = Vec::with_capacity(sigs.len());

        for (signer, vote) in sigs {
            let Some(idx) = bitmap_index_in_group(&state, group, signer) else {
                continue;
            };
            indices.push(idx as usize);
            partials.push(vote.signature);
        }

        if !is_supermajority(partials.len() as u64, SPOOL_GROUP_SIZE as u64) {
            continue;
        }

        let bitmap = SpoolGroupBitmap::from_indices(&indices, SPOOL_GROUP_SIZE);
        let aggregate = BlsSignature::aggregate(&partials)
            .map_err(|e| NodeError::Store(format!("aggregate finalize sigs: {e:?}")))?;

        let outcome = submit_if_at_tip(
            &ctx.ingest,
            submit_sign_snapshot(ctx, epoch, group, bitmap, aggregate),
        )
        .await;

        match outcome {
            TxOutcome::Confirmed(txid) => {
                info!(%epoch, group = group.0, ?txid, "snapshot: finalize submitted")
            }
            TxOutcome::Program(err) => {
                debug!(?err, %epoch, group = group.0, "snapshot: finalize program error")
            }
            TxOutcome::Transport(err) => {
                debug!(%err, %epoch, group = group.0, "snapshot: finalize transport error")
            }
            TxOutcome::SkippedStale => {
                debug!(%epoch, group = group.0, "snapshot: finalize deferred, ingest stale")
            }
        }
    }

    Ok(())
}
