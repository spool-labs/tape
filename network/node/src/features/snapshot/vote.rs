//! Create this node's generic snapshot vote from the canonical candidate.

use std::sync::Arc;

use rpc::Rpc;
use store::Store;
use tape_core::cert::SnapshotSignMessage;
use tape_core::system::{VoteCandidate, VoteKind};
use tape_protocol::{Api, ProtocolState};
use tape_store::ops::VoteOps;
use tokio_util::sync::CancellationToken;

use crate::context::NodeContext;
use crate::core::error::NodeError;
use crate::features::snapshot::build::SnapshotCandidate;
use crate::features::vote::member_groups;

#[derive(Debug, Default)]
pub struct VoteSummary {
    pub votes: usize,
}

pub async fn create_snapshot_votes<Db, Cluster, Blockchain>(
    ctx: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    state: &ProtocolState,
    candidate: &SnapshotCandidate,
    cancel: &CancellationToken,
) -> Result<VoteSummary, NodeError>
where
    Db: Store + 'static,
    Cluster: Api + 'static,
    Blockchain: Rpc + 'static,
{
    if cancel.is_cancelled() {
        return Ok(VoteSummary::default());
    }

    let me = ctx.node_address();
    if state.find_member(me).is_none() {
        return Ok(VoteSummary::default());
    }

    let vote = vote_candidate(candidate);
    let message = SnapshotSignMessage::new(candidate.target_epoch, candidate.hash).to_bytes();
    let signature = ctx
        .bls_sign(&message)
        .map_err(|e| NodeError::Store(format!("snapshot bls_sign: {e:?}")))?;

    let mut votes = 0usize;
    for group in member_groups(&state.member_spools(me)) {
        ctx.store
            .put_vote_sig(vote, group, me, &signature)
            .map_err(|e| NodeError::Store(format!("put_vote_sig: {e}")))?;
        votes += 1;
    }

    Ok(VoteSummary { votes })
}

pub fn vote_candidate(candidate: &SnapshotCandidate) -> VoteCandidate {
    VoteCandidate {
        kind: VoteKind::Snapshot,
        voting_epoch: candidate.voting_epoch,
        target_epoch: candidate.target_epoch,
        hash: candidate.hash,
    }
}
