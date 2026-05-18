//! Push our local assignment vote to peers in each owned group.

use std::sync::Arc;

use rpc::Rpc;
use store::Store;
use tape_core::bft::is_supermajority;
use tape_core::erasure::GROUP_SIZE;
use tape_protocol::Api;
use tape_protocol::api::VoteReq;
use tape_store::ops::VoteOps;
use tokio_util::sync::CancellationToken;
use tracing::trace;

use crate::context::NodeContext;
use crate::core::error::NodeError;
use crate::features::assignment::build::AssignmentCandidate;
use crate::features::assignment::vote::vote_candidate;
use crate::features::vote::{group_peers_without, member_groups};

pub async fn fanout_assignment_votes<Db, Cluster, Blockchain>(
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
        .run_until_cancelled(fanout_assignment_inner(ctx, candidate))
        .await
    {
        Some(result) => result,
        None => Ok(()),
    }
}

async fn fanout_assignment_inner<Db, Cluster, Blockchain>(
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

        if is_supermajority(sigs.len() as u64, GROUP_SIZE as u64) {
            continue;
        }

        let Some((_, signature)) = sigs.into_iter().find(|(signer, _)| *signer == me) else {
            continue;
        };

        let request = VoteReq {
            signer: me,
            candidate: vote,
            group,
            signature,
        };

        for peer in group_peers_without(&state, group, me) {
            if let Err(error) = ctx.api.vote(peer, &request).await {
                trace!(
                    ?error,
                    %peer,
                    epoch = candidate.target_epoch.0,
                    group = group.0,
                    "assignment: vote push failed"
                );
            }
        }
    }

    Ok(())
}
