//! Submit assignment candidate, group votes, and group finalization transactions.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use rpc::Rpc;
use store::Store;
use tape_api::errors::TapeError;
use tape_core::bft::is_supermajority;
use tape_core::bls::BlsSignature;
use tape_core::erasure::GROUP_SIZE;
use tape_core::system::EpochPhase;
use tape_core::types::{EpochNumber, SpoolBitmap};
use tape_protocol::{Api, ProtocolState};
use tape_store::ops::VoteOps;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

use crate::chain::{submit_finalize_group, submit_propose_assignment, submit_vote_assignment};
use crate::context::NodeContext;
use crate::core::chain_tx::{stagger_by_rank, submit_if_at_tip, TxOutcome, TxRejectionKind};
use crate::core::error::NodeError;
use crate::features::assignment::build::AssignmentCandidate;
use crate::features::assignment::vote::vote_candidate;
use crate::features::lifecycle::manager::committee_rank;
use crate::features::vote::{bitmap_index_in_group, member_groups};

pub async fn submit_assignment_proposal<Db, Cluster, Blockchain>(
    ctx: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    candidate: &AssignmentCandidate,
    cancel: &CancellationToken,
    proposed: &Mutex<HashSet<EpochNumber>>,
) -> Result<(), NodeError>
where
    Db: Store + 'static,
    Cluster: Api + 'static,
    Blockchain: Rpc + 'static,
{
    if cancel.is_cancelled() {
        return Ok(());
    }

    let me = ctx.node_address();
    let state = ctx.state();
    if state.find_member(me).is_none() {
        return Ok(());
    }

    if stagger_by_rank(committee_rank(&state, me), cancel).await {
        return Ok(());
    }

    // Re-read after the stagger: skip proposing if another member's proposal for
    // this voting epoch already landed, or the round already reached a canonical
    // assignment hash, while this node waited its turn.
    if proposed
        .lock()
        .is_ok_and(|seen| seen.contains(&candidate.voting_epoch))
    {
        return Ok(());
    }
    let state = ctx.state();
    if state
        .next_epoch
        .as_ref()
        .is_some_and(|next| next.id == candidate.target_epoch && next.has_assignment_hash())
    {
        return Ok(());
    }

    let outcome = submit_if_at_tip(
        &ctx.ingest,
        "propose_assignment",
        submit_propose_assignment(ctx, candidate.voting_epoch, candidate.hash),
    )
    .await;
    match outcome {
        TxOutcome::Confirmed(txid) => {
            info!(
                epoch = candidate.target_epoch.0,
                hash = %candidate.hash,
                %txid,
                "assignment: proposal submitted"
            );
        }
        TxOutcome::Rejected {
            kind: TxRejectionKind::Program(err),
            ..
        } => {
            debug!(
                epoch = candidate.target_epoch.0,
                hash = %candidate.hash,
                ?err,
                "assignment: proposal program error"
            );
        }
        TxOutcome::Rejected {
            kind: TxRejectionKind::KnownContention,
            err,
        } => {
            debug!(
                epoch = candidate.target_epoch.0,
                hash = %candidate.hash,
                %err,
                "assignment: proposal already submitted"
            );
        }
        TxOutcome::Rejected {
            kind: TxRejectionKind::KnownStaleState,
            err,
        } => {
            debug!(
                epoch = candidate.target_epoch.0,
                hash = %candidate.hash,
                %err,
                "assignment: stale proposal ignored"
            );
        }
        TxOutcome::Rejected {
            kind: TxRejectionKind::UnknownExecution,
            err,
        } => {
            debug!(
                epoch = candidate.target_epoch.0,
                hash = %candidate.hash,
                %err,
                "assignment: proposal transaction rejected"
            );
        }
        TxOutcome::Rejected {
            kind: TxRejectionKind::Transport,
            err,
        } => {
            debug!(
                epoch = candidate.target_epoch.0,
                hash = %candidate.hash,
                %err,
                "assignment: proposal transport error"
            );
        }
        TxOutcome::SkippedStale => {
            debug!(
                epoch = candidate.target_epoch.0,
                hash = %candidate.hash,
                "assignment: proposal deferred, ingest stale"
            );
        }
    }

    Ok(())
}

pub async fn submit_ready_assignment_votes<Db, Cluster, Blockchain>(
    ctx: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    state: &ProtocolState,
    candidate: &AssignmentCandidate,
    cancel: &CancellationToken,
) -> Result<(), NodeError>
where
    Db: Store + 'static,
    Cluster: Api + 'static,
    Blockchain: Rpc + 'static,
{
    let me = ctx.node_address();
    if state.find_member(me).is_none() {
        return Ok(());
    }

    if stagger_by_rank(committee_rank(state, me), cancel).await {
        return Ok(());
    }

    match cancel
        .run_until_cancelled(submit_ready_assignment_votes_inner(ctx, state, candidate))
        .await
    {
        Some(result) => result,
        None => Ok(()),
    }
}

async fn submit_ready_assignment_votes_inner<Db, Cluster, Blockchain>(
    ctx: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    state: &ProtocolState,
    candidate: &AssignmentCandidate,
) -> Result<(), NodeError>
where
    Db: Store + 'static,
    Cluster: Api + 'static,
    Blockchain: Rpc + 'static,
{
    let me = ctx.node_address();
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
            "vote_assignment",
            submit_vote_assignment(
                ctx,
                candidate.voting_epoch,
                candidate.hash,
                group,
                bitmap,
                aggregate,
            ),
        )
        .await;

        match outcome {
            TxOutcome::Confirmed(txid) => {
                info!(
                    epoch = candidate.target_epoch.0,
                    group = group.0,
                    %txid,
                    "assignment: group vote submitted"
                );
            }
            TxOutcome::Rejected {
                kind: TxRejectionKind::Program(TapeError::AlreadySigned),
                ..
            } => {
                debug!(
                    epoch = candidate.target_epoch.0,
                    group = group.0,
                    "assignment: group already voted"
                );
            }
            TxOutcome::Rejected {
                kind: TxRejectionKind::Program(TapeError::BadEpochState),
                ..
            } => {
                debug!(
                    epoch = candidate.target_epoch.0,
                    group = group.0,
                    "assignment: vote phase already changed"
                );
            }
            TxOutcome::Rejected {
                kind: TxRejectionKind::Program(err),
                ..
            } => {
                debug!(
                    epoch = candidate.target_epoch.0,
                    group = group.0,
                    ?err,
                    "assignment: vote program error"
                );
            }
            TxOutcome::Rejected {
                kind: TxRejectionKind::KnownContention,
                err,
            } => {
                debug!(
                    epoch = candidate.target_epoch.0,
                    group = group.0,
                    %err,
                    "assignment: vote already applied"
                );
            }
            TxOutcome::Rejected {
                kind: TxRejectionKind::KnownStaleState,
                err,
            } => {
                debug!(
                    epoch = candidate.target_epoch.0,
                    group = group.0,
                    %err,
                    "assignment: stale vote ignored"
                );
            }
            TxOutcome::Rejected {
                kind: TxRejectionKind::UnknownExecution,
                err,
            } => {
                debug!(
                    epoch = candidate.target_epoch.0,
                    group = group.0,
                    %err,
                    "assignment: vote transaction rejected"
                );
            }
            TxOutcome::Rejected {
                kind: TxRejectionKind::Transport,
                err,
            } => {
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
    let me = ctx.node_address();
    let state = ctx.state();
    if state.find_member(me).is_none() {
        return Ok(());
    }

    if stagger_by_rank(committee_rank(&state, me), cancel).await {
        return Ok(());
    }

    // Re-read after the stagger: bail if the round left Closing, or every group
    // is already finalized. Groups can finalize out of order, so they are not
    // skipped by index; a re-submit of a finalized group is cheaply rejected.
    let state = ctx.state();
    if state.epoch() != candidate.voting_epoch || state.phase() != EpochPhase::Closing {
        return Ok(());
    }
    let finalized_groups = state
        .next_epoch
        .as_ref()
        .map(|next| next.total_groups)
        .unwrap_or(0);
    if finalized_groups >= candidate.groups.len() as u64 {
        return Ok(());
    }

    for group in &candidate.groups {
        if cancel.is_cancelled() {
            return Ok(());
        }

        let outcome = submit_if_at_tip(
            &ctx.ingest,
            "finalize_group",
            submit_finalize_group(ctx, candidate.target_epoch, group.payload, group.proof),
        )
        .await;

        match outcome {
            TxOutcome::Confirmed(txid) => {
                info!(
                    epoch = candidate.target_epoch.0,
                    group = group.group.0,
                    %txid,
                    "assignment: group finalized"
                );
            }
            TxOutcome::Rejected {
                kind: TxRejectionKind::Program(TapeError::UnexpectedState),
                ..
            } => {
                debug!(
                    epoch = candidate.target_epoch.0,
                    group = group.group.0,
                    "assignment: group already finalized or state changed"
                );
            }
            TxOutcome::Rejected {
                kind: TxRejectionKind::Program(err),
                ..
            } => {
                debug!(
                    epoch = candidate.target_epoch.0,
                    group = group.group.0,
                    ?err,
                    "assignment: finalize group program error"
                );
            }
            TxOutcome::Rejected {
                kind: TxRejectionKind::KnownContention,
                err,
            } => {
                debug!(
                    epoch = candidate.target_epoch.0,
                    group = group.group.0,
                    %err,
                    "assignment: group finalization already applied"
                );
            }
            TxOutcome::Rejected {
                kind: TxRejectionKind::KnownStaleState,
                err,
            } => {
                debug!(
                    epoch = candidate.target_epoch.0,
                    group = group.group.0,
                    %err,
                    "assignment: stale group finalization ignored"
                );
            }
            TxOutcome::Rejected {
                kind: TxRejectionKind::UnknownExecution,
                err,
            } => {
                debug!(
                    epoch = candidate.target_epoch.0,
                    group = group.group.0,
                    %err,
                    "assignment: finalize group transaction rejected"
                );
            }
            TxOutcome::Rejected {
                kind: TxRejectionKind::Transport,
                err,
            } => {
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
