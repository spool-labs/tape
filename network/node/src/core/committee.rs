use std::collections::HashSet;

use tape_core::erasure::group_for_spool;
use tape_core::spooler::SpoolGroup;
use tape_core::types::NodeId;
use tape_protocol::state::ProtocolState;

pub fn our_member_index(state: &ProtocolState, node_id: NodeId) -> Result<usize, &'static str> {
    state
        .find_member(node_id)
        .map(|(idx, _)| idx)
        .ok_or("our node not present in committee")
}

pub fn our_snapshot_groups(
    state: &ProtocolState,
    node_id: NodeId,
) -> Result<HashSet<SpoolGroup>, &'static str> {
    let (idx, _) = state
        .find_member(node_id)
        .ok_or("our node not present in committee")?;
    Ok(state
        .member_spools(idx)
        .into_iter()
        .map(group_for_spool)
        .collect())
}
