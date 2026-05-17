//! Shared helpers for the snapshot HTTP handler, fanout, and manager.

use tape_core::spooler::GroupIndex;
use tape_core::types::NodeId;
use tape_protocol::ProtocolState;

/// Group peer node ids with `exclude` filtered out.
pub fn group_peers_without(
    state: &ProtocolState,
    group: GroupIndex,
    exclude: NodeId,
) -> Vec<NodeId> {
    state
        .group_peers(group)
        .into_iter()
        .map(|(_, id)| id)
        .filter(|id| *id != exclude)
        .collect()
}

/// Spool offset within `group` owned by `node_id`, if any. Reads the spool
/// assignment directly so positions can't drift due to unrelated committee
/// entries being filtered out.
pub fn bitmap_index_in_group(
    state: &ProtocolState,
    group: GroupIndex,
    node_id: NodeId,
) -> Option<u16> {
    let (member_index, _) = state.find_member(node_id)?;
    state
        .spools
        .members_in_group(group)
        .iter()
        .position(|&m| m as usize == member_index)
        .map(|i| i as u16)
}
