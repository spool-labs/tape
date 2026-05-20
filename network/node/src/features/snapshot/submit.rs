//! Submit snapshot candidate, group votes, and finalization transactions.

use std::sync::Arc;

use rpc::Rpc;
use store::Store;
use tape_api::errors::TapeError;
use tape_core::bft::is_supermajority;
use tape_core::bls::BlsSignature;
use tape_core::erasure::GROUP_SIZE;
use tape_core::types::SpoolBitmap;
use tape_protocol::Api;
use tape_store::ops::VoteOps;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

use crate::chain::{submit_finalize_snapshot, submit_propose_snapshot, submit_vote_snapshot};
use crate::context::NodeContext;
use crate::core::chain_tx::{submit_if_at_tip, TxOutcome};
use crate::core::error::NodeError;
use crate::features::snapshot::build::{persist_snapshot_candidate, SnapshotCandidate};
use crate::features::snapshot::vote::vote_candidate;
use crate::features::vote::{bitmap_index_in_group, member_groups};

pub async fn submit_snapshot_proposal<Db, Cluster, Blockchain>(
    ctx: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    candidate: &SnapshotCandidate,
    cancel: &CancellationToken,
) -> Result<(), NodeError>
where
    Db: Store + 'static,
    Cluster: Api + 'static,
    Blockchain: Rpc + 'static,
{
    if cancel.is_cancelled() {
        return Ok(());
    }

    let outcome = submit_if_at_tip(&ctx.ingest, submit_propose_snapshot(ctx, candidate.hash)).await;
    match outcome {
        TxOutcome::Confirmed(txid) => {
            info!(
                epoch = candidate.target_epoch.0,
                hash = %candidate.hash,
                %txid,
                "snapshot: proposal submitted"
            );
        }
        TxOutcome::Program(err) => {
            debug!(
                epoch = candidate.target_epoch.0,
                hash = %candidate.hash,
                ?err,
                "snapshot: proposal program error"
            );
        }
        TxOutcome::Transport(err) => {
            debug!(
                epoch = candidate.target_epoch.0,
                hash = %candidate.hash,
                %err,
                "snapshot: proposal transport error"
            );
        }
        TxOutcome::SkippedStale => {
            debug!(
                epoch = candidate.target_epoch.0,
                hash = %candidate.hash,
                "snapshot: proposal deferred, ingest stale"
            );
        }
    }

    Ok(())
}

pub async fn submit_ready_snapshot_votes<Db, Cluster, Blockchain>(
    ctx: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    candidate: &SnapshotCandidate,
    cancel: &CancellationToken,
) -> Result<(), NodeError>
where
    Db: Store + 'static,
    Cluster: Api + 'static,
    Blockchain: Rpc + 'static,
{
    match cancel
        .run_until_cancelled(submit_ready_snapshot_votes_inner(ctx, candidate))
        .await
    {
        Some(result) => result,
        None => Ok(()),
    }
}

async fn submit_ready_snapshot_votes_inner<Db, Cluster, Blockchain>(
    ctx: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    candidate: &SnapshotCandidate,
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

    let vote = vote_candidate(candidate);
    for group in member_groups(&state.member_spools(me)) {
        let sigs = ctx
            .store
            .iter_vote_sigs(vote, group)
            .map_err(|e| NodeError::Store(format!("iter_vote_sigs: {e}")))?;

        let mut indices = Vec::with_capacity(sigs.len());
        let mut partials = Vec::with_capacity(sigs.len());

        for (signer, signature) in sigs {
            let Some(index) = bitmap_index_in_group(&state, group, signer) else {
                continue;
            };
            indices.push(index as usize);
            partials.push(signature);
        }

        if !is_supermajority(partials.len() as u64, GROUP_SIZE as u64) {
            continue;
        }

        let bitmap = SpoolBitmap::from_indices(&indices);
        let aggregate = BlsSignature::aggregate(&partials)
            .map_err(|e| NodeError::Store(format!("aggregate snapshot sigs: {e:?}")))?;

        let outcome = submit_if_at_tip(
            &ctx.ingest,
            submit_vote_snapshot(ctx, candidate.hash, group, bitmap, aggregate),
        )
        .await;

        match outcome {
            TxOutcome::Confirmed(txid) => {
                info!(
                    epoch = candidate.target_epoch.0,
                    group = group.0,
                    %txid,
                    "snapshot: group vote submitted"
                );
            }
            TxOutcome::Program(TapeError::AlreadySigned) => {
                debug!(
                    epoch = candidate.target_epoch.0,
                    group = group.0,
                    "snapshot: group already voted"
                );
            }
            TxOutcome::Program(err) => {
                debug!(
                    epoch = candidate.target_epoch.0,
                    group = group.0,
                    ?err,
                    "snapshot: vote program error"
                );
            }
            TxOutcome::Transport(err) => {
                debug!(
                    epoch = candidate.target_epoch.0,
                    group = group.0,
                    %err,
                    "snapshot: vote transport error"
                );
            }
            TxOutcome::SkippedStale => {
                debug!(
                    epoch = candidate.target_epoch.0,
                    group = group.0,
                    "snapshot: vote deferred, ingest stale"
                );
            }
        }
    }

    Ok(())
}

pub async fn submit_snapshot_finalization<Db, Cluster, Blockchain>(
    ctx: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    candidate: &SnapshotCandidate,
    cancel: &CancellationToken,
) -> Result<(), NodeError>
where
    Db: Store + 'static,
    Cluster: Api + 'static,
    Blockchain: Rpc + 'static,
{
    if cancel.is_cancelled() {
        return Ok(());
    }

    persist_snapshot_candidate(ctx.as_ref(), candidate)?;

    let outcome = submit_if_at_tip(
        &ctx.ingest,
        submit_finalize_snapshot(ctx, candidate.target_epoch, candidate.tape),
    )
    .await;

    match outcome {
        TxOutcome::Confirmed(txid) => {
            info!(
                epoch = candidate.target_epoch.0,
                hash = %candidate.hash,
                %txid,
                "snapshot: finalized"
            );
        }
        TxOutcome::Program(err) => {
            debug!(
                epoch = candidate.target_epoch.0,
                hash = %candidate.hash,
                ?err,
                "snapshot: finalize program error"
            );
        }
        TxOutcome::Transport(err) => {
            debug!(
                epoch = candidate.target_epoch.0,
                hash = %candidate.hash,
                %err,
                "snapshot: finalize transport error"
            );
        }
        TxOutcome::SkippedStale => {
            debug!(
                epoch = candidate.target_epoch.0,
                hash = %candidate.hash,
                "snapshot: finalize deferred, ingest stale"
            );
        }
    }

    Ok(())
}
