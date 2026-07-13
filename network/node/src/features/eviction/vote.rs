//! Sign this node's eviction vote for the target node in each owned group.

use std::sync::Arc;

use rpc::Rpc;
use store::Store;
use tape_core::cert::NodeEvictMessage;
use tape_protocol::{Api, ProtocolState};
use tape_store::ops::VoteOps;

use crate::context::NodeContext;
use crate::core::error::NodeError;
use crate::features::eviction::build::EvictionCandidate;
use crate::features::vote::member_groups;

pub async fn create_eviction_votes<Db, Cluster, Blockchain>(
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

    let message =
        NodeEvictMessage::new(candidate.target_epoch, candidate.nonce, candidate.node).to_bytes();
    let signature = ctx
        .bls_sign(&message)
        .map_err(|e| NodeError::Store(format!("eviction bls_sign: {e:?}")))?;

    let vote = candidate.vote();
    for group in member_groups(&state.member_spools(me)) {
        ctx.store
            .put_vote_sig(vote, group, me, &signature)
            .map_err(|e| NodeError::Store(format!("put_vote_sig: {e}")))?;
    }

    Ok(())
}
