//! Shared helpers for the snapshot HTTP handler, fanout, and manager.

use tape_core::bls::{BlsPubkey, BlsSignature};
use tape_core::spooler::SpoolGroup;
use tape_core::system::CommitteeMember;
use tape_core::types::NodeId;
use tape_protocol::ProtocolState;

/// Spool groups the local node currently has at least one member slot in.
pub fn local_groups(state: &ProtocolState, node_id: NodeId) -> Vec<SpoolGroup> {
    let Some((member_index, _)) = state.find_member(node_id) else {
        return Vec::new();
    };
    let mut groups: Vec<SpoolGroup> = state
        .member_spools(member_index)
        .into_iter()
        .map(SpoolGroup::of)
        .collect();
    groups.sort_unstable_by_key(|g| g.0);
    groups.dedup_by_key(|g| g.0);
    groups
}

/// Group peer node ids with `exclude` filtered out.
pub fn group_peers_without(
    state: &ProtocolState,
    group: SpoolGroup,
    exclude: NodeId,
) -> Vec<NodeId> {
    state
        .group_peers(group)
        .into_iter()
        .map(|(_, id)| id)
        .filter(|id| *id != exclude)
        .collect()
}

/// Position within a group's spool-ordered peer list at which `node_id` first
/// appears. Returns `None` if the node does not own any spool in that group.
pub fn bitmap_index_in_group(
    state: &ProtocolState,
    group: SpoolGroup,
    node_id: NodeId,
) -> Option<u16> {
    state
        .group_peers(group)
        .iter()
        .position(|(_, id)| *id == node_id)
        .map(|i| i as u16)
}

/// Committee member at bitmap-index `index` of `group` in the current committee.
pub fn group_peer_by_index(
    state: &ProtocolState,
    group: SpoolGroup,
    index: u16,
) -> Option<CommitteeMember> {
    let (_, node_id) = state.group_peers(group).get(index as usize).copied()?;
    state.find_member(node_id).map(|(_, member)| *member)
}

/// Verify a single-signer partial BLS signature against `message`.
pub fn verify_partial(pubkey: &BlsPubkey, message: &[u8], signature: &BlsSignature) -> bool {
    signature.verify_aggregate(message, &[*pubkey]).is_ok()
}

