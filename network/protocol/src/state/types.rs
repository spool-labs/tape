//! ProtocolState — snapshot of on-chain protocol state.

use tape_core::erasure::{MEMBER_COUNT, SPOOL_COUNT};
use tape_core::spooler::{SpoolAssignment, SpoolGroup, SpoolIndex};
use tape_core::system::{Committee, CommitteeMember, EpochPhase};
use tape_core::types::{EpochNumber, NodeId};
use tape_crypto::Hash;

/// Snapshot of on-chain protocol state.
///
/// Contains committee membership, spool assignments, and epoch info.
/// Does not include network addresses — those live in `PeerManager`.
#[derive(Debug, Clone)]
pub struct ProtocolState {
    pub epoch: EpochNumber,
    pub phase: EpochPhase,
    pub nonce: Hash,
    pub committee: Vec<CommitteeMember>,
    pub committee_prev: Vec<CommitteeMember>,
    pub committee_next: Vec<CommitteeMember>,
    pub spools: SpoolAssignment<SPOOL_COUNT>,
    pub spools_prev: SpoolAssignment<SPOOL_COUNT>,
}

impl Default for ProtocolState {
    fn default() -> Self {
        Self {
            epoch: EpochNumber(0),
            phase: EpochPhase::Active,
            nonce: Hash::default(),
            committee: Vec::new(),
            committee_prev: Vec::new(),
            committee_next: Vec::new(),
            spools: bytemuck::Zeroable::zeroed(),
            spools_prev: bytemuck::Zeroable::zeroed(),
        }
    }
}

impl ProtocolState {
    /// Which node owns this spool in the current committee?
    pub fn spool_owner(&self, spool: SpoolIndex) -> Option<NodeId> {
        let mapping = self.spools.0.get(spool as usize)?;
        let member_index = *mapping as usize;
        self.committee.get(member_index).map(|m| m.id)
    }

    /// Which node owned this spool in the previous committee?
    pub fn spool_owner_prev(&self, spool: SpoolIndex) -> Option<NodeId> {
        let mapping = self.spools_prev.0.get(spool as usize)?;
        let member_index = *mapping as usize;
        self.committee_prev.get(member_index).map(|m| m.id)
    }

    /// All spools assigned to a member (by index in current committee).
    pub fn member_spools(&self, member_index: usize) -> Vec<SpoolIndex> {
        self.spools.spools_for_member(member_index)
    }

    /// Find a member in the current committee by NodeId.
    /// Returns (member_index, &CommitteeMember).
    pub fn find_member(&self, node_id: NodeId) -> Option<(usize, &CommitteeMember)> {
        self.committee
            .iter()
            .enumerate()
            .find(|(_, m)| m.id == node_id)
    }

    /// Find a member in the next committee by NodeId.
    /// Returns (member_index, &CommitteeMember).
    pub fn find_member_next(&self, node_id: NodeId) -> Option<(usize, &CommitteeMember)> {
        self.committee_next
            .iter()
            .enumerate()
            .find(|(_, m)| m.id == node_id)
    }

    /// Build a fixed-size Committee array from the current committee Vec.
    pub fn committee_as_array(&self) -> Committee<MEMBER_COUNT> {
        let mut committee = Committee::new();
        for member in &self.committee {
            let _ = committee.try_join(member);
        }
        committee
    }

    /// Map each spool in a group to its owning NodeId (current committee).
    ///
    /// Returns a vec of (global_spool_index, node_id) for all spools in the group.
    /// Spools owned by members not in the committee are skipped.
    pub fn group_peers(&self, group: SpoolGroup) -> Vec<(SpoolIndex, NodeId)> {
        group_peers_inner(&self.spools, &self.committee, group)
    }

    /// Map each spool in a group to its owning NodeId (previous committee).
    pub fn group_peers_prev(&self, group: SpoolGroup) -> Vec<(SpoolIndex, NodeId)> {
        group_peers_inner(&self.spools_prev, &self.committee_prev, group)
    }

    /// Count unique members responsible for spools in a group (current committee).
    pub fn group_member_count(&self, group: SpoolGroup) -> usize {
        group_member_count_inner(&self.spools, group)
    }

    /// Count unique members responsible for spools in a group (previous committee).
    pub fn group_member_count_prev(&self, group: SpoolGroup) -> usize {
        group_member_count_inner(&self.spools_prev, group)
    }
}

fn group_peers_inner(
    spools: &SpoolAssignment<SPOOL_COUNT>,
    committee: &[CommitteeMember],
    group: SpoolGroup,
) -> Vec<(SpoolIndex, NodeId)> {
    let members = spools.members_in_group(group);
    let base = group.base();
    members
        .iter()
        .enumerate()
        .filter_map(|(i, &member_idx)| {
            let spool = base + i as SpoolIndex;
            let node_id = committee.get(member_idx as usize)?.id;
            Some((spool, node_id))
        })
        .collect()
}

fn group_member_count_inner(
    spools: &SpoolAssignment<SPOOL_COUNT>,
    group: SpoolGroup,
) -> usize {
    let members = spools.members_in_group(group);
    let mut seen = [false; 256]; // SpoolMapping is u8
    let mut count = 0;
    for &m in members {
        if !seen[m as usize] {
            seen[m as usize] = true;
            count += 1;
        }
    }
    count
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use tape_core::types::coin::{Coin, TAPE};

    fn empty_state() -> ProtocolState {
        ProtocolState::default()
    }

    #[test]
    fn find_member_empty() {
        let state = empty_state();
        assert!(state.find_member(NodeId(1)).is_none());
        assert!(state.find_member_next(NodeId(1)).is_none());
    }

    #[test]
    fn spool_owner_empty() {
        let state = empty_state();
        assert!(state.spool_owner(0).is_none());
    }

    fn state_with_3_members() -> ProtocolState {
        let mut state = ProtocolState::default();
        for i in 0..3u64 {
            state.committee.push(CommitteeMember::new(
                NodeId(i + 1),
                Coin::<TAPE>::new(1000 - i),
            ));
        }
        let mut spools = [0u8; SPOOL_COUNT];
        for (i, s) in spools.iter_mut().enumerate() {
            *s = (i % 3) as u8;
        }
        state.spools = SpoolAssignment::new(spools);
        state
    }

    #[test]
    fn group_peers_all_spools() {
        let state = state_with_3_members();
        let peers = state.group_peers(SpoolGroup(0));
        assert_eq!(peers.len(), 20);
        assert_eq!(peers[0], (0, NodeId(1)));
        assert_eq!(peers[1], (1, NodeId(2)));
    }

    #[test]
    fn group_peers_as_hashmap() {
        let state = state_with_3_members();
        let map: HashMap<SpoolIndex, NodeId> = state.group_peers(SpoolGroup(0)).into_iter().collect();
        assert_eq!(map.len(), 20);
        assert_eq!(map[&0], NodeId(1));
    }

    #[test]
    fn group_peers_prev_uses_previous() {
        let mut state = state_with_3_members();
        state.committee_prev = vec![
            CommitteeMember::new(NodeId(10), Coin::<TAPE>::new(500)),
            CommitteeMember::new(NodeId(20), Coin::<TAPE>::new(400)),
        ];
        let mut prev_spools = [0u8; SPOOL_COUNT];
        for (i, s) in prev_spools.iter_mut().enumerate() {
            *s = (i % 2) as u8;
        }
        state.spools_prev = SpoolAssignment::new(prev_spools);

        let peers = state.group_peers_prev(SpoolGroup(0));
        assert_eq!(peers.len(), 20);
        assert_eq!(peers[0].1, NodeId(10));
        assert_eq!(peers[1].1, NodeId(20));
    }

    #[test]
    fn group_member_count_3() {
        let state = state_with_3_members();
        assert_eq!(state.group_member_count(SpoolGroup(0)), 3);
    }

    #[test]
    fn group_member_count_single() {
        let mut state = ProtocolState::default();
        state.committee.push(CommitteeMember::new(NodeId(1), Coin::<TAPE>::new(1000)));
        state.spools = SpoolAssignment::new([0u8; SPOOL_COUNT]);
        assert_eq!(state.group_member_count(SpoolGroup(0)), 1);
    }

    #[test]
    fn find_member_next_uses_next_committee() {
        let mut state = ProtocolState::default();
        state.committee_next.push(CommitteeMember::new(NodeId(7), Coin::<TAPE>::new(1000)));

        let member = state.find_member_next(NodeId(7)).unwrap();
        assert_eq!(member.0, 0);
        assert_eq!(member.1.id, NodeId(7));
    }
}
