//! Submit the eviction proposal and aggregate group votes.

use std::sync::Arc;

use rpc::Rpc;
use store::Store;
use tape_core::bft::is_supermajority;
use tape_core::bls::BlsSignature;
use tape_core::erasure::GROUP_SIZE;
use tape_core::spooler::GroupIndex;
use tape_core::types::SpoolBitmap;
use tape_protocol::{Api, ProtocolState};
use tape_store::ops::VoteOps;
use tracing::{debug, info};

use crate::chain::{submit_propose_eviction, submit_vote_eviction};
use crate::context::NodeContext;
use crate::core::chain_tx::{TxOutcome, submit_if_at_tip};
use crate::core::error::NodeError;
use crate::features::eviction::build::EvictionCandidate;
use crate::features::vote::{bitmap_index_in_group, member_groups};

pub async fn submit_eviction_proposal<Db, Cluster, Blockchain>(
    ctx: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    candidate: &EvictionCandidate,
) -> Result<(), NodeError>
where
    Db: Store + 'static,
    Cluster: Api + 'static,
    Blockchain: Rpc + 'static,
{
    let outcome = submit_if_at_tip(
        &ctx.ingest,
        submit_propose_eviction(ctx, candidate.voting_epoch, candidate.node),
    )
    .await;

    log_outcome(candidate, None, "proposal", &outcome);

    Ok(())
}

pub async fn submit_ready_eviction_votes<Db, Cluster, Blockchain>(
    ctx: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    state: &ProtocolState,
    candidate: &EvictionCandidate,
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

    let vote = candidate.vote();
    for group in member_groups(&state.member_spools(me)) {
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

        if !is_supermajority(partials.len() as u64, GROUP_SIZE as u64) {
            continue;
        }

        let bitmap = SpoolBitmap::from_indices(&indices);
        let aggregate = BlsSignature::aggregate(&partials)
            .map_err(|e| NodeError::Store(format!("aggregate eviction sigs: {e:?}")))?;

        let outcome = submit_if_at_tip(
            &ctx.ingest,
            submit_vote_eviction(
                ctx,
                candidate.voting_epoch,
                candidate.node,
                group,
                bitmap,
                aggregate,
            ),
        )
        .await;

        log_outcome(candidate, Some(group), "group vote", &outcome);
    }

    Ok(())
}

fn log_outcome(
    candidate: &EvictionCandidate,
    group: Option<GroupIndex>,
    action: &str,
    outcome: &TxOutcome,
) {
    let group = group.map(|group| group.0);
    match outcome {
        TxOutcome::Confirmed(txid) => {
            info!(
                epoch = candidate.target_epoch.0,
                node = %candidate.node,
                group,
                %txid,
                "eviction: {action} submitted"
            );
        }
        TxOutcome::Rejected { kind, err } => {
            debug!(
                epoch = candidate.target_epoch.0,
                node = %candidate.node,
                group,
                ?kind,
                %err,
                "eviction: {action} rejected"
            );
        }
        TxOutcome::SkippedStale => {
            debug!(
                epoch = candidate.target_epoch.0,
                node = %candidate.node,
                group,
                "eviction: {action} deferred, ingest stale"
            );
        }
    }
}
