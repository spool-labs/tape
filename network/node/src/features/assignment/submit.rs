//! Submit assignment candidate, group votes, and group finalization transactions.

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

use crate::chain::{submit_finalize_group, submit_propose_assignment, submit_vote_assignment};
use crate::context::NodeContext;
use crate::core::chain_tx::{TxOutcome, submit_if_at_tip};
use crate::core::error::NodeError;
use crate::features::assignment::build::AssignmentCandidate;
use crate::features::assignment::vote::vote_candidate;
use crate::features::vote::{bitmap_index_in_group, member_groups};

pub async fn submit_assignment_proposal<Db, Cluster, Blockchain>(
    ctx: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    candidate: &AssignmentCandidate,
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

    let outcome =
        submit_if_at_tip(&ctx.ingest, submit_propose_assignment(ctx, candidate.hash)).await;
    match outcome {
        TxOutcome::Confirmed(txid) => {
            info!(
                epoch = candidate.target_epoch.0,
                hash = ?candidate.hash,
                ?txid,
                "assignment: proposal submitted"
            );
        }
        TxOutcome::Program(err) => {
            debug!(
                epoch = candidate.target_epoch.0,
                hash = ?candidate.hash,
                ?err,
                "assignment: proposal program error"
            );
        }
        TxOutcome::Transport(err) => {
            debug!(
                epoch = candidate.target_epoch.0,
                hash = ?candidate.hash,
                %err,
                "assignment: proposal transport error"
            );
        }
        TxOutcome::SkippedStale => {
            debug!(
                epoch = candidate.target_epoch.0,
                hash = ?candidate.hash,
                "assignment: proposal deferred, ingest stale"
            );
        }
    }

    Ok(())
}

pub async fn submit_ready_assignment_votes<Db, Cluster, Blockchain>(
    ctx: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    candidate: &AssignmentCandidate,
    cancel: &CancellationToken,
) -> Result<(), NodeError>
where
    Db: Store + 'static,
    Cluster: Api + 'static,
    Blockchain: Rpc + 'static,
{
    match cancel
        .run_until_cancelled(submit_ready_assignment_votes_inner(ctx, candidate))
        .await
    {
        Some(result) => result,
        None => Ok(()),
    }
}

async fn submit_ready_assignment_votes_inner<Db, Cluster, Blockchain>(
    ctx: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    candidate: &AssignmentCandidate,
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
            .map_err(|e| NodeError::Store(format!("aggregate assignment sigs: {e:?}")))?;

        let outcome = submit_if_at_tip(
            &ctx.ingest,
            submit_vote_assignment(ctx, candidate.hash, group, bitmap, aggregate),
        )
        .await;

        match outcome {
            TxOutcome::Confirmed(txid) => {
                info!(
                    epoch = candidate.target_epoch.0,
                    group = group.0,
                    ?txid,
                    "assignment: group vote submitted"
                );
            }
            TxOutcome::Program(TapeError::AlreadySigned) => {
                debug!(
                    epoch = candidate.target_epoch.0,
                    group = group.0,
                    "assignment: group already voted"
                );
            }
            TxOutcome::Program(err) => {
                debug!(
                    epoch = candidate.target_epoch.0,
                    group = group.0,
                    ?err,
                    "assignment: vote program error"
                );
            }
            TxOutcome::Transport(err) => {
                debug!(
                    epoch = candidate.target_epoch.0,
                    group = group.0,
                    %err,
                    "assignment: vote transport error"
                );
            }
            TxOutcome::SkippedStale => {
                debug!(
                    epoch = candidate.target_epoch.0,
                    group = group.0,
                    "assignment: vote deferred, ingest stale"
                );
            }
        }
    }

    Ok(())
}

pub async fn submit_assignment_finalization<Db, Cluster, Blockchain>(
    ctx: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    candidate: &AssignmentCandidate,
    cancel: &CancellationToken,
) -> Result<(), NodeError>
where
    Db: Store + 'static,
    Cluster: Api + 'static,
    Blockchain: Rpc + 'static,
{
    for group in &candidate.groups {
        if cancel.is_cancelled() {
            return Ok(());
        }

        let outcome = submit_if_at_tip(
            &ctx.ingest,
            submit_finalize_group(ctx, candidate.target_epoch, group.payload, group.proof),
        )
        .await;

        match outcome {
            TxOutcome::Confirmed(txid) => {
                info!(
                    epoch = candidate.target_epoch.0,
                    group = group.group.0,
                    ?txid,
                    "assignment: group finalized"
                );
            }
            TxOutcome::Program(err) => {
                debug!(
                    epoch = candidate.target_epoch.0,
                    group = group.group.0,
                    ?err,
                    "assignment: finalize group program error"
                );
            }
            TxOutcome::Transport(err) => {
                debug!(
                    epoch = candidate.target_epoch.0,
                    group = group.group.0,
                    %err,
                    "assignment: finalize group transport error"
                );
            }
            TxOutcome::SkippedStale => {
                debug!(
                    epoch = candidate.target_epoch.0,
                    group = group.group.0,
                    "assignment: finalize group deferred, ingest stale"
                );
                return Ok(());
            }
        }
    }

    Ok(())
}
