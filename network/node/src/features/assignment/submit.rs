//! Submit assignment candidate, group votes, and group finalization transactions.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use tape_rpc::Rpc;
use tape_store_api::Store;
use tape_api::errors::TapeError;
use tape_core::bls::BlsSignature;
use tape_core::system::EpochPhase;
use tape_core::types::{EpochNumber, SpoolBitmap};
use tape_crypto::Hash;
use tape_protocol::{Api, ProtocolState};
use tape_store::ops::VoteOps;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

use crate::chain::{submit_finalize_group, submit_propose_assignment, submit_vote_assignment};
use crate::context::NodeContext;
use crate::core::chain_tx::{submit_if_at_tip, TxOutcome, TxRejectionKind};
use crate::core::error::NodeError;
use crate::features::assignment::build::AssignmentCandidate;
use crate::features::assignment::vote::vote_candidate;
use crate::features::vote::{bitmap_index_in_group, member_groups};

pub async fn submit_assignment_proposal<Db, Cluster, Blockchain>(
    ctx: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    candidate: &AssignmentCandidate,
    cancel: &CancellationToken,
    proposed: &Mutex<HashSet<(EpochNumber, Hash)>>,
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

    // Skip proposing if a proposal for this same hash already landed, or the
    // round already reached a canonical assignment hash. A proposal for any
    // other hash says nothing about this one.
    let own = (candidate.voting_epoch, candidate.hash);
    if proposed.lock().is_ok_and(|seen| seen.contains(&own)) {
        return Ok(());
    }
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
    // Both outcomes mean the vote account for this hash exists.
    if matches!(
        outcome,
        TxOutcome::Confirmed(_)
            | TxOutcome::Rejected {
                kind: TxRejectionKind::KnownContention,
                ..
            }
    ) {
        if let Ok(mut seen) = proposed.lock() {
            seen.insert(own);
        }
    }

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
    for group in member_groups(&state, me) {
        let sigs = ctx
            .store
            .iter_vote_sigs(vote, group)
            .map_err(|e| NodeError::Store(format!("iter_vote_sigs: {e}")))?;

        let mut indices = Vec::with_capacity(sigs.len());
        let mut partials = Vec::with_capacity(sigs.len());

        for (signer, signature) in sigs {
            let Some(index) = bitmap_index_in_group(state, group, signer) else {
                continue;
            };
            indices.push(index as usize);
            partials.push(signature);
        }

        if (partials.len() as u64) < ctx.params().thresholds.assignment {
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

    // Bail if the round left Closing, or every group is already finalized.
    // Groups can finalize out of order, so they are not skipped by index; a
    // re-submit of a finalized group is dropped by its simulation.
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

#[cfg(test)]
mod tests {
    use tape_api::program::tapedrive::assignment_vote_pda;
    use tape_crypto::Hash;

    use super::*;
    use crate::core::ingest::IngestState;
    use crate::harness::{NodeHarness, TestContext};

    fn candidate(epoch: EpochNumber) -> AssignmentCandidate {
        AssignmentCandidate {
            voting_epoch: epoch,
            target_epoch: epoch.next(),
            nonce: Hash::default(),
            hash: Hash::new_unique(),
            groups: Vec::new(),
        }
    }

    async fn closing_harness() -> NodeHarness {
        NodeHarness::builder()
            .nodes(25)
            .phase(EpochPhase::Closing)
            .next_committee_size(20)
            .build()
            .await
            .expect("build harness")
    }

    async fn has_vote_account(ctx: &TestContext, candidate: &AssignmentCandidate) -> bool {
        let (vote, _) = assignment_vote_pda(
            candidate.voting_epoch,
            candidate.target_epoch,
            candidate.hash,
        );
        ctx.rpc.rpc().get_account(&vote).await.is_ok()
    }

    // with nothing on record a node proposes its hash
    #[tokio::test]
    async fn proposes_first() {
        let harness = closing_harness().await;
        let ctx = harness.ctx_for(0);
        ctx.ingest.publish(IngestState::AtTip);

        let candidate = candidate(harness.epoch());
        let proposed = Mutex::new(HashSet::new());

        submit_assignment_proposal(&ctx, &candidate, &CancellationToken::new(), &proposed)
            .await
            .expect("propose");

        assert!(has_vote_account(&ctx, &candidate).await);
    }

    fn is_recorded(
        proposed: &Mutex<HashSet<(EpochNumber, Hash)>>,
        candidate: &AssignmentCandidate,
    ) -> bool {
        proposed
            .lock()
            .expect("record")
            .contains(&(candidate.voting_epoch, candidate.hash))
    }

    // a proposal for another hash leaves a node free to propose its own
    #[tokio::test]
    async fn proposes_beside_another_hash() {
        let harness = closing_harness().await;
        let ctx = harness.ctx_for(0);
        ctx.ingest.publish(IngestState::AtTip);

        let candidate = candidate(harness.epoch());
        let other = (candidate.voting_epoch, Hash::new_unique());
        let proposed = Mutex::new(HashSet::from([other]));

        submit_assignment_proposal(&ctx, &candidate, &CancellationToken::new(), &proposed)
            .await
            .expect("propose");

        assert!(has_vote_account(&ctx, &candidate).await);
        assert!(is_recorded(&proposed, &candidate));
    }

    // a hash already on record is not proposed again
    #[tokio::test]
    async fn skips_proposed_hash() {
        let harness = closing_harness().await;
        let ctx = harness.ctx_for(0);
        ctx.ingest.publish(IngestState::AtTip);

        let candidate = candidate(harness.epoch());
        let own = (candidate.voting_epoch, candidate.hash);
        let proposed = Mutex::new(HashSet::from([own]));

        submit_assignment_proposal(&ctx, &candidate, &CancellationToken::new(), &proposed)
            .await
            .expect("propose");

        assert!(!has_vote_account(&ctx, &candidate).await);
    }

    // a node that proposes a hash another node got in first learns the vote account exists
    #[tokio::test]
    async fn lost_race_is_recorded() {
        let harness = closing_harness().await;
        let (first, second) = (harness.ctx_for(0), harness.ctx_for(1));
        first.ingest.publish(IngestState::AtTip);
        second.ingest.publish(IngestState::AtTip);

        let candidate = candidate(harness.epoch());
        let cancel = CancellationToken::new();
        submit_assignment_proposal(&first, &candidate, &cancel, &Mutex::new(HashSet::new()))
            .await
            .expect("first proposal");

        let proposed = Mutex::new(HashSet::new());
        submit_assignment_proposal(&second, &candidate, &cancel, &proposed)
            .await
            .expect("second proposal");

        assert!(is_recorded(&proposed, &candidate));
    }
}
