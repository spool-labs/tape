//! Shared helpers for the snapshot HTTP handler, fanout, and manager.

use tape_core::spooler::GroupIndex;
use tape_crypto::Address;
use tape_protocol::ProtocolState;

/// Group peer node addresses with `exclude` filtered out.
pub fn group_peers_without(
    state: &ProtocolState,
    group: GroupIndex,
    exclude: Address,
) -> Vec<Address> {
    state
        .group_peers(group)
        .into_iter()
        .map(|(_, id)| id)
        .filter(|id| *id != exclude)
        .collect()
}

/// Spool offset within `group` owned by `node`, if any. Reads the spool
/// assignment directly so positions can't drift due to unrelated committee
/// entries being filtered out.
pub fn bitmap_index_in_group(
    state: &ProtocolState,
    group: GroupIndex,
    node: Address,
) -> Option<u16> {
    let (spool, _) = state.spool_for_node_in_group(group, node)?;
    group.position_of(spool).map(|position| position as u16)
}
