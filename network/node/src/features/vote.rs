use tape_api::event::VoteRecorded;
use tape_core::spooler::GroupIndex;
use tape_core::types::SpoolIndex;
use tape_crypto::Address;
use tape_protocol::ProtocolState;

pub fn member_groups(spools: &[SpoolIndex]) -> Vec<GroupIndex> {
    let mut groups = spools
        .iter()
        .copied()
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
    state
        .group_peers(group)
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
    let (spool, _) = state.spool_for_node_in_group(group, node)?;
    group.position_of(spool).map(|position| position as u16)
}

pub fn all_vote_groups_signed(event: &VoteRecorded) -> bool {
    let signed_groups = u64::from_le_bytes(event.signed_groups);
    let total_groups = u64::from_le_bytes(event.total_groups);

    signed_groups == total_groups
}
