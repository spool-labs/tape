use tape_api::event::{NodeEvicted, NodeJoinedCommittee};
use tape_core::system::{apply_member_join, bubble_up_peer, Member, Peer};
use tape_protocol::ProtocolState;

use crate::core::error::NodeError;

pub fn apply_join_committee_event(
    state: &mut ProtocolState,
    event: NodeJoinedCommittee,
) -> Result<(), NodeError> {
    let member = Member {
        node: event.node,
        stake: event.stake,
        assigned: Default::default(),
        blacklisted: Default::default(),
        spools: 0,
    };

    let next_committee = state.next_committee.get_or_insert_with(Vec::new);

    apply_member_join(
        next_committee,
        state.system.committee_size as usize,
        member,
    )
    .map_err(|error| NodeError::Store(format!("apply_member_join: {error:?}")))?;

    upsert_peer_from_join_event(&mut state.peers, event);

    Ok(())
}

/// Drop the evicted node from the next-epoch committee.
///
/// Mirrors the on-chain landing: the member is removed from the next committee
/// while the peer set is left untouched, so next-epoch owners can still sync
/// the node's spools.
pub fn apply_eviction_event(state: &mut ProtocolState, event: NodeEvicted) {
    if let Some(next_committee) = state.next_committee.as_mut() {
        next_committee.retain(|member| member.node != event.node);
    }
}

fn upsert_peer_from_join_event(peers: &mut Vec<Peer>, event: NodeJoinedCommittee) {
    let index = match peers.iter().position(|peer| peer.node == event.node) {
        Some(index) => {
            peers[index].bls_pubkey = event.key;
            peers[index].preferences = event.preferences;
            index
        }
        None => {
            let mut peer = Peer::new(event.node);
            peer.bls_pubkey = event.key;
            peer.preferences = event.preferences;
            peers.push(peer);
            peers.len() - 1
        }
    };

    let count = peers.len();
    bubble_up_peer(peers, count, index);
}
