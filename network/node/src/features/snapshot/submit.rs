//! Submit snapshot candidate, group votes, and finalization transactions.

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

use crate::chain::{submit_finalize_snapshot, submit_propose_snapshot, submit_vote_snapshot};
use crate::context::NodeContext;
use crate::core::chain_tx::{submit_if_at_tip, TxOutcome, TxRejectionKind};
use crate::core::error::NodeError;
use crate::features::snapshot::build::{persist_snapshot_candidate, SnapshotCandidate};
use crate::features::snapshot::vote::vote_candidate;
use crate::features::vote::{bitmap_index_in_group, member_groups};

pub async fn submit_snapshot_proposal<Db, Cluster, Blockchain>(
    ctx: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    candidate: &SnapshotCandidate,
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
    // round already reached a canonical snapshot hash. A proposal for any
    // other hash says nothing about this one.
    let own = (candidate.voting_epoch, candidate.hash);
    if proposed.lock().is_ok_and(|seen| seen.contains(&own)) {
        return Ok(());
    }
    if state
        .previous
        .as_ref()
        .is_some_and(|prev| prev.epoch.id == candidate.target_epoch && prev.epoch.has_snapshot_hash())
    {
        return Ok(());
    }

    let outcome = submit_if_at_tip(
        &ctx.ingest,
        "propose_snapshot",
        submit_propose_snapshot(ctx, candidate.voting_epoch, candidate.hash),
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
                "snapshot: proposal submitted"
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
                "snapshot: proposal program error"
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
                "snapshot: proposal already submitted"
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
                "snapshot: stale proposal ignored"
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
                "snapshot: proposal transaction rejected"
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
    state: &ProtocolState,
    candidate: &SnapshotCandidate,
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
        .run_until_cancelled(submit_ready_snapshot_votes_inner(ctx, state, candidate))
        .await
    {
        Some(result) => result,
        None => Ok(()),
    }
}

async fn submit_ready_snapshot_votes_inner<Db, Cluster, Blockchain>(
    ctx: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    state: &ProtocolState,
    candidate: &SnapshotCandidate,
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

        if (partials.len() as u64) < ctx.params().thresholds.snapshot {
            continue;
        }

        let bitmap = SpoolBitmap::from_indices(&indices);
        let aggregate = BlsSignature::aggregate(&partials)
            .map_err(|e| NodeError::Store(format!("aggregate snapshot sigs: {e:?}")))?;

        let outcome = submit_if_at_tip(
            &ctx.ingest,
            "vote_snapshot",
            submit_vote_snapshot(
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
                    "snapshot: group vote submitted"
                );
            }
            TxOutcome::Rejected {
                kind: TxRejectionKind::Program(TapeError::AlreadySigned),
                ..
            } => {
                debug!(
                    epoch = candidate.target_epoch.0,
                    group = group.0,
                    "snapshot: group already voted"
                );
            }
            TxOutcome::Rejected {
                kind: TxRejectionKind::Program(TapeError::BadEpochState),
                ..
            } => {
                debug!(
                    epoch = candidate.target_epoch.0,
                    group = group.0,
                    "snapshot: vote phase already changed"
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
                    "snapshot: vote program error"
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
                    "snapshot: vote already applied"
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
                    "snapshot: stale vote ignored"
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
                    "snapshot: vote transaction rejected"
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

    let me = ctx.node_address();
    let state = ctx.state();
    if state.find_member(me).is_none() {
        return Ok(());
    }

    persist_snapshot_candidate(ctx.as_ref(), candidate)?;

    // Finalizing moves the voting epoch out of the Snapshot phase, so skip if
    // another member already finalized.
    if state.epoch() != candidate.voting_epoch || state.phase() != EpochPhase::Snapshot {
        return Ok(());
    }

    let outcome = submit_if_at_tip(
        &ctx.ingest,
        "finalize_snapshot",
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
        TxOutcome::Rejected {
            kind: TxRejectionKind::Program(TapeError::BadEpochState),
            ..
        } => {
            debug!(
                epoch = candidate.target_epoch.0,
                hash = %candidate.hash,
                "snapshot: finalize phase already changed"
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
                "snapshot: finalize program error"
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
                "snapshot: finalization already applied"
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
                "snapshot: stale finalization ignored"
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
                "snapshot: finalize transaction rejected"
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

#[cfg(test)]
mod tests {
    use tape_api::program::tapedrive::snapshot_vote_pda;
    use tape_api::state::Tape;
    use tape_core::types::SlotNumber;

    use super::*;
    use crate::core::ingest::IngestState;
    use crate::harness::{NodeHarness, TestContext};

    fn candidate(epoch: EpochNumber) -> SnapshotCandidate {
        SnapshotCandidate {
            voting_epoch: epoch,
            target_epoch: epoch.prev(),
            end_slot: SlotNumber(1),
            hash: Hash::new_unique(),
            tape: Tape::snapshot(epoch.prev()),
            tracks: Vec::new(),
        }
    }

    async fn snapshot_harness() -> NodeHarness {
        NodeHarness::builder()
            .nodes(25)
            .phase(EpochPhase::Snapshot)
            .no_prev_snapshot_tape()
            .build()
            .await
            .expect("build harness")
    }

    async fn has_vote_account(ctx: &TestContext, candidate: &SnapshotCandidate) -> bool {
        let (vote, _) = snapshot_vote_pda(
            candidate.voting_epoch,
            candidate.target_epoch,
            candidate.hash,
        );
        ctx.rpc.rpc().get_account(&vote).await.is_ok()
    }

    // with nothing on record a node proposes its hash
    #[tokio::test]
    async fn proposes_first() {
        let harness = snapshot_harness().await;
        let ctx = harness.ctx_for(0);
        ctx.ingest.publish(IngestState::AtTip);

        let candidate = candidate(harness.epoch());
        let proposed = Mutex::new(HashSet::new());

        submit_snapshot_proposal(&ctx, &candidate, &CancellationToken::new(), &proposed)
            .await
            .expect("propose");

        assert!(has_vote_account(&ctx, &candidate).await);
    }

    // a proposal for another hash leaves a node free to propose its own
    #[tokio::test]
    async fn proposes_beside_another_hash() {
        let harness = snapshot_harness().await;
        let ctx = harness.ctx_for(0);
        ctx.ingest.publish(IngestState::AtTip);

        let candidate = candidate(harness.epoch());
        let own = (candidate.voting_epoch, candidate.hash);
        let other = (candidate.voting_epoch, Hash::new_unique());
        let proposed = Mutex::new(HashSet::from([other]));

        submit_snapshot_proposal(&ctx, &candidate, &CancellationToken::new(), &proposed)
            .await
            .expect("propose");

        assert!(has_vote_account(&ctx, &candidate).await);
        assert!(proposed.lock().expect("record").contains(&own));
    }

    // a hash already on record is not proposed again
    #[tokio::test]
    async fn skips_proposed_hash() {
        let harness = snapshot_harness().await;
        let ctx = harness.ctx_for(0);
        ctx.ingest.publish(IngestState::AtTip);

        let candidate = candidate(harness.epoch());
        let own = (candidate.voting_epoch, candidate.hash);
        let proposed = Mutex::new(HashSet::from([own]));

        submit_snapshot_proposal(&ctx, &candidate, &CancellationToken::new(), &proposed)
            .await
            .expect("propose");

        assert!(!has_vote_account(&ctx, &candidate).await);
    }

    // a node that proposes a hash another node got in first learns the vote account exists
    #[tokio::test]
    async fn lost_race_is_recorded() {
        let harness = snapshot_harness().await;
        let (first, second) = (harness.ctx_for(0), harness.ctx_for(1));
        first.ingest.publish(IngestState::AtTip);
        second.ingest.publish(IngestState::AtTip);

        let candidate = candidate(harness.epoch());
        let own = (candidate.voting_epoch, candidate.hash);
        let cancel = CancellationToken::new();
        submit_snapshot_proposal(&first, &candidate, &cancel, &Mutex::new(HashSet::new()))
            .await
            .expect("first proposal");

        let proposed = Mutex::new(HashSet::new());
        submit_snapshot_proposal(&second, &candidate, &cancel, &proposed)
            .await
            .expect("second proposal");

        assert!(proposed.lock().expect("record").contains(&own));
    }
}
