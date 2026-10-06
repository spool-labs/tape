use tape_api::event::VoteRecorded;
use tape_core::spooler::GroupIndex;
use tape_crypto::Address;
use tape_protocol::ProtocolState;

/// The live groups a node votes in this epoch.
pub fn member_groups(state: &ProtocolState, node: Address) -> Vec<GroupIndex> {
    let mut groups = state
        .member_live_spools(node)
        .into_iter()
        .map(GroupIndex::containing)
        .collect::<Vec<_>>();
    groups.sort_by_key(|group| group.0);
    groups.dedup_by_key(|group| group.0);
    groups
}

pub fn group_peers_without(
    state: &ProtocolState,
    group: GroupIndex,
    exclude: Address,
) -> Vec<Address> {
    let Some(own) = state.virtual_group(group) else {
        return Vec::new();
    };

    state
        .group_peers(own)
        .into_iter()
        .map(|(_, id)| id)
        .filter(|id| *id != exclude)
        .collect()
}

pub fn bitmap_index_in_group(
    state: &ProtocolState,
    group: GroupIndex,
    node: Address,
) -> Option<u16> {
    let (spool, _) = state.spool_for_node_in_group(state.virtual_group(group)?, node)?;
    group.position_of(spool).map(|position| position as u16)
}

pub fn all_vote_groups_signed(event: &VoteRecorded) -> bool {
    event.signed_groups == event.total_groups
}
