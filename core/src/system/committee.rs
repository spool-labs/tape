use core::fmt;
use crate::types::*;
use crate::bls::*;
use std::collections::BTreeMap;
use bytemuck::{Pod, Zeroable};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LeaderSetError {
    AlreadyPresent { idx: usize },
    Full,
    NotFull,
    NotFound,
    NotBetter { min_idx: usize, min_stake: Coin<TAPE> },
    ZeroStake,
}

/// Relative NodeId within a committee
pub type RelativeNodeId = u8;

/// A CommitteeMember represents a staking pool that can be part of a committee. Each member has a
/// unique NodeId and a BLS public key used for verifying aggregate signatures from the many
/// committee members.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct CommitteeMember {
    pub id: NodeId,
    pub key: BlsPubkey,
}

/// A LeaderSet defines a set of committee members that will be considered for appointment
/// during an upcoming epoch. Each member has an associated stake, which influences their
/// likelihood of being assigned seats in the committee.
#[repr(C)]
#[derive(Clone, Copy, PartialEq)]
pub struct LeaderSet<const NODES: usize> {
    pub member_count: u64,
    pub members: [CommitteeMember; NODES],
    pub stakes: [Coin<TAPE>; NODES], // (member_index -> stake)
}

unsafe impl<const NODES: usize> Zeroable for LeaderSet<NODES> {}
unsafe impl<const NODES: usize> Pod for LeaderSet<NODES> {}

impl<const NODES: usize> LeaderSet<NODES> {
    /// Number of active members in the leader set.
    #[inline]
    pub fn size(&self) -> usize {
        (self.member_count as usize).min(NODES)
    }

    /// Capacity of the set.
    #[inline]
    pub fn capacity(&self) -> usize {
        NODES
    }

    /// Whether the set is full.
    #[inline]
    pub fn is_full(&self) -> bool {
        self.size() == NODES
    }

    /// Checks if the given NodeId is present in the leader set.
    #[inline]
    pub fn contains(&self, node_id: &NodeId) -> bool {
        self.index_of(node_id).is_some()
    }

    /// Returns the index of the given NodeId, if any.
    #[inline]
    pub fn index_of(&self, node_id: &NodeId) -> Option<usize> {
        let count = self.size();
        self.members[..count].iter().position(|m| &m.id == node_id)
    }

    /// Helper: get stake at index as u64.
    #[inline]
    pub fn stake_at(&self, idx: usize) -> Coin<TAPE> {
        self.stakes[idx]
    }

    /// Helper: set stake at index.
    #[inline]
    fn set_stake_at(&mut self, idx: usize, stake: Coin<TAPE>) {
        self.stakes[idx] = stake;
    }

    /// Helper: find index of minimum stake (among active members).
    pub fn min_stake_index(&self) -> Option<usize> {
        debug_assert!(self.is_sorted(), "not sorted");

        let count = self.size();
        if count == 0 {
            return None;
        }

        Some(count - 1)
    }

    /// Minimum stake in the set (0 if empty).
    pub fn threshold_stake(&self) -> Coin<TAPE> {
        self.min_stake_index()
            .map(|i| self.stake_at(i))
            .unwrap_or(TAPE::zero())
    }

    /// Total stake in the set.
    pub fn total_stake(&self) -> Coin<TAPE> {
        let count = self.size();
        let mut sum = TAPE::zero();
        for i in 0..count {
            sum = sum.saturating_add(self.stake_at(i));
        }
        sum
    }

    /// Tries to join  new member with the given stake. If the set is not full, it inserts the
    /// new member. If the set is full, it replaces the current minimum-stake member.
    pub fn try_join(
        &mut self,
        member: CommitteeMember,
        staked_amount: Coin<TAPE>,
    ) -> Result<usize, LeaderSetError> {
        if self.is_full() {
            self.replace_if_better(member, staked_amount)
        } else {
            self.insert(member, staked_amount)
        }
    }

    /// Inserts a new node if there is capacity. Never evicts.
    /// Returns the index where the member was inserted.
    pub fn insert(
        &mut self,
        member: CommitteeMember, 
        staked_amount: Coin::<TAPE>
    ) -> Result<usize, LeaderSetError> {

        if staked_amount == TAPE::zero() {
            return Err(LeaderSetError::ZeroStake);
        }

        if let Some(idx) = self.index_of(&member.id) {
            return Err(LeaderSetError::AlreadyPresent { idx });
        }

        let count = self.size();
        if count >= NODES {
            return Err(LeaderSetError::Full);
        }

        self.members[count] = member;
        self.set_stake_at(count, staked_amount);
        self.member_count = (count + 1) as u64;

        self.sort_active_desc();

        Ok(self.index_of(&member.id).expect("just inserted"))
    }

    /// Replaces the current minimum-stake member if the set is full and the new stake is strictly larger.
    /// Returns the index replaced on success.
    pub fn replace_if_better(
        &mut self,
        member: CommitteeMember,
        staked_amount: Coin::<TAPE>,
    ) -> Result<usize, LeaderSetError> {
        if staked_amount == TAPE::zero() {
            return Err(LeaderSetError::ZeroStake);
        }

        if let Some(idx) = self.index_of(&member.id) {
            return Err(LeaderSetError::AlreadyPresent { idx });
        }

        if !self.is_full() {
            return Err(LeaderSetError::NotFull);
        }

        let Some(min_idx) = self.min_stake_index() else {
            return Err(LeaderSetError::NotFull);
        };

        let min_val = self.stake_at(min_idx);
        if staked_amount <= min_val {
            return Err(LeaderSetError::NotBetter { min_idx, min_stake: min_val });
        }

        self.members[min_idx] = member;
        self.set_stake_at(min_idx, staked_amount);

        self.sort_active_desc();

        Ok(self.index_of(&member.id).expect("just inserted"))
    }

    /// Updates the staked amount of the node with the given NodeId.
    /// Never removes. Returns the previous stake on success.
    pub fn update_stake(
        &mut self,
        node_id: &NodeId,
        new_stake: Coin::<TAPE>,
    ) -> Result<Coin<TAPE>, LeaderSetError> {

        let Some(idx) = self.index_of(node_id) else {
            return Err(LeaderSetError::NotFound);
        };

        let old = self.stake_at(idx);
        self.set_stake_at(idx, new_stake);

        self.sort_active_desc();

        Ok(old)
    }

    /// Removes a node with the given NodeId from the set using unordered swap-remove semantics.
    /// Returns the removed member and its stake.
    pub fn remove(&mut self, node_id: &NodeId) -> Result<(CommitteeMember, Coin<TAPE>), LeaderSetError> {
        let Some(idx) = self.index_of(node_id) else {
            return Err(LeaderSetError::NotFound);
        };

        let count = self.size();
        debug_assert!(idx < count);

        let removed_member = self.members[idx];
        let removed_stake = self.stake_at(idx);

        let last = count - 1;
        if idx != last {
            self.members[idx] = self.members[last];
            self.stakes[idx] = self.stakes[last];
        }

        self.members[last] = CommitteeMember::zeroed();
        self.stakes[last] = TAPE::zero();
        self.member_count = count as u64 - 1;

        self.sort_active_desc();

        Ok((removed_member, removed_stake))
    }

    /// Returns an iterator over the committee members.
    pub fn iter_members(&self) -> impl Iterator<Item = &CommitteeMember> {
        let count = self.size();
        self.members[..count].iter()
    }

    /// Returns the IDs of the nodes in the set.
    pub fn leader_ids(&self) -> Vec<NodeId> {
        let count = self.size();
        self.members[..count].iter().map(|m| m.id).collect()
    }

    /// Returns parallel vectors of IDs and stake.
    pub fn leader_ids_and_stake(&self) -> (Vec<NodeId>, Vec<Coin::<TAPE>>) {
        let count = self.size();
        let mut ids = Vec::with_capacity(count);
        let mut stakes = Vec::with_capacity(count);
        for i in 0..count {
            ids.push(self.members[i].id);
            stakes.push(self.stake_at(i));
        }
        (ids, stakes)
    }

    /// Sorts the active members in-place by descending stake, then ascending NodeId for
    /// determinism.
    #[inline]
    fn sort_active_desc(&mut self) {
        let count = self.size();
        if count <= 1 {
            return;
        }

        // Collect active entries, sort by stake desc, then NodeId asc for determinism
        let mut entries: Vec<(CommitteeMember, Coin<TAPE>)> =
            (0..count).map(|i| (self.members[i], self.stakes[i])).collect();

        entries.sort_by(|(ma, sa), (mb, sb)| {
            // Highest stake first
            sb.cmp(sa).then(ma.id.cmp(&mb.id))
        });

        // Write back
        for i in 0..count {
            self.members[i] = entries[i].0;
            self.stakes[i] = entries[i].1;
        }
    }

    fn is_sorted(&self) -> bool {
        let count = self.size();
        for i in 1..count {
            let a = self.stakes[i - 1];
            let b = self.stakes[i];

            if a < b {
                return false;
            }
        }
        true
    }
}

impl<const NODES: usize> fmt::Debug for LeaderSet<NODES> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let count = self.size();
        f.debug_struct("LeaderSet")
            .field("member_count", &count)
            .field("members", &&self.members[..count])
            .field("stakes", &&self.stakes[..count])
            .finish()
    }
}

impl<const NODES: usize> fmt::Display for LeaderSet<NODES> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let count = self.size();
        write!(f, "LeaderSet(size={}, members=[", count)?;
        for i in 0..count {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(
                f,
                "{{ id: {:?}, stake: {:?} }}",
                self.members[i].id, self.stakes[i]
            )?;
        }
        write!(f, "])")
    }
}

/// An AppointedSet defines a set of committee members and their assigned seats. The number of
/// seats assigned depends on the originating LeaderSet stakes. More stake usually means more
/// seats assigned to that member. The number of seats is finite and is distributed using the
/// Jefferson method (a.k.a. d'Hondt method). A single committee member is likely to be assigned
/// multiple seats and the seat count influences the weight of that node's signature in the
/// committee.
///
/// Each seat is uniquely identified by its index in the seats array and is not interchangeable.
/// Seat movement between epochs is minimized to reduce disruption (a pool dropping out and coming
/// back the next epoch will likely get the same set of seat indicies if all stake remains equal).
#[repr(C)]
#[derive(Clone, Copy, PartialEq)]
pub struct AppointedSet<const NODES: usize, const SEATS: usize> {
    pub member_count: u64,
    pub members: [CommitteeMember; NODES],
    pub seats: [RelativeNodeId; SEATS], // (seat_index -> member_index)
}

unsafe impl<const NODES: usize, const SEATS: usize> Zeroable for AppointedSet<NODES, SEATS> {}
unsafe impl<const NODES: usize, const SEATS: usize> Pod for AppointedSet<NODES, SEATS> {}

impl<const NODES: usize, const SEATS: usize> AppointedSet<NODES, SEATS> {

    /// Creates a new, empty AppointedSet.
    pub fn new() -> Self {
        debug_assert!(NODES <= u8::MAX as usize);

        Self {
            member_count: 0,
            members: [CommitteeMember::zeroed(); NODES],
            seats: [0; SEATS],
        }
    }

    /// Returns the size of the appointed committee (number of active members).
    #[inline]
    pub fn size(&self) -> usize {
        (self.member_count as usize).min(NODES)
    }

    /// Checks if a node with the given NodeId is part of the committee.
    #[inline]
    pub fn contains(&self, node_id: &NodeId) -> bool {
        self.index_of(node_id).is_some()
    }

    /// Returns the index of the node with the given NodeId in the members array, if it exists.
    #[inline]
    pub fn index_of(&self, node_id: &NodeId) -> Option<usize> {
        let count = self.size();
        self.members[..count]
            .iter()
            .position(|m| &m.id == node_id)
    }

    /// Returns the weight (number of seats) assigned to the node with the given NodeId.
    pub fn node_weight(&self, node_id: &NodeId) -> u16 {
        let count = self.size();
        if count == 0 {
            return 0;
        }

        if let Some(idx) = self.index_of(node_id) {
            assert!(idx <= u8::MAX as usize);

            self.seats
                .iter()
                .filter(|&&seat| seat == idx as u8)
                .count() as u16
        } else {
            0
        }
    }

    /// Returns a map of NodeId to their assigned weight (number of seats).
    pub fn weights(&self) -> BTreeMap<NodeId, u16> {
        let count = self.size();

        // Initialize entries for active members
        let mut map = BTreeMap::new();
        for m in &self.members[..count] {
            map.entry(m.id).or_insert(0);
        }

        // Tally seats that reference valid member indices
        for &seat in &self.seats {
            let idx = seat as usize;
            if idx < count {
                let id = self.members[idx].id;
                let e = map.entry(id).or_insert(0);
                *e += 1;
            }
        }

        map
    }

    /// Returns a list of seat indices assigned to the node with the given NodeId.
    pub fn seats_for(&self, node_id: &NodeId) -> Vec<u16> {
        let count = self.size();
        if count == 0 {
            return Vec::new();
        }

        if let Some(idx) = self.index_of(node_id) {
            assert!(idx <= u8::MAX as usize);

            let target = idx as u8;
            let mut out = Vec::new();
            for (seat_idx, &seat_owner) in self.seats.iter().enumerate() {
                if seat_owner == target {
                    out.push(seat_idx as u16);
                }
            }
            out
        } else {
            Vec::new()
        }
    }

    /// Returns an iterator over the committee members.
    pub fn iter_members(&self) -> impl Iterator<Item = &CommitteeMember> {
        let count = self.size();
        self.members[..count].iter()
    }
}

impl<const NODES: usize, const SEATS: usize> fmt::Debug for AppointedSet<NODES, SEATS> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let count = self.size();

        // Only seats pointing to a valid member index are included.
        let seats: Vec<(usize, _)> = self
            .seats
            .iter()
            .enumerate()
            .filter_map(|(seat_idx, &owner_idx)| {
                let idx = owner_idx as usize;
                if idx < count {
                    Some((seat_idx, self.members[idx].id))
                } else {
                    None
                }
            })
            .collect();

        f.debug_struct("AppointedSet")
            .field("member_count", &count)
            .field("members", &&self.members[..count])
            .field("seats", &seats)
            .finish()
    }
}

impl<const NODES: usize, const SEATS: usize> fmt::Display for AppointedSet<NODES, SEATS> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let count = self.size();

        write!(f, "AppointedSet(size={}, members=[", count)?;
        for i in 0..count {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{:?}", self.members[i].id)?;
        }

        // Count seats per valid member index
        let mut counts = vec![0u16; count];
        for &seat in &self.seats {
            let idx = seat as usize;
            if idx < count {
                counts[idx] = counts[idx].saturating_add(1);
            }
        }

        write!(f, "], weights=[")?;
        let mut first = true;
        for i in 0..count {
            if !first {
                write!(f, ", ")?;
            }
            first = false;
            write!(f, "{:?}: {}", self.members[i].id, counts[i])?;
        }
        write!(f, "])")
    }
}


/// Combine members from the current committee and the next leader set into a single Vec.
pub fn get_unique_members<'a, const NODES: usize, const SEATS: usize>(
    current: &'a AppointedSet<NODES, SEATS>,
    next: &'a LeaderSet<NODES>,
) -> Vec<&'a CommitteeMember> {
    let mut unique = Vec::new();

    // First add all members from the current committee to the unique array
    for member in current.iter_members() {
        unique.push(member);
    }

    // Then add members from the leader set
    for index in 0..next.size() {
        let member = &next.members[index];

        // Check if this member was already in the array
        let previous = unique
            .iter()
            .position(|&m| m.id == member.id);

        // If yes, use the latest CommitteeMember
        // (in case the BlsPubkey changed)
        if let Some(index) = previous {
            unique[index] = member; 
        } else {
            unique.push(member);
        }
    }

    unique
}


#[cfg(test)]
mod tests {
    use super::*;
    use bytemuck::Zeroable;

    fn tape(v: u64) -> Coin<TAPE> {
        TAPE::new(v)
    }

    fn node(n: u8) -> NodeId {
        NodeId::new(n as u64)
    }

    // Helper to build a CommitteeMember with the given NodeId
    fn member(id: NodeId) -> CommitteeMember {
        CommitteeMember {
            id,
            key: BlsPubkey::zeroed(),
        }
    }

    // Helper to create an empty LeaderSet<N>
    fn empty_leader_set<const N: usize>() -> LeaderSet<N> {
        LeaderSet {
            member_count: 0,
            members: [CommitteeMember::zeroed(); N],
            stakes: [TAPE::zero(); N],
        }
    }

    // Helper: stake for a node (used only in tests)
    fn stake_for_node<const N: usize>(set: &LeaderSet<N>, node_id: &NodeId) -> Option<Coin<TAPE>> {
        set.index_of(node_id).map(|i| set.stake_at(i))
    }

    #[test]
    fn leader_evict_correct_node_simple() {
        const N: usize = 5;
        let mut set: LeaderSet<N> = empty_leader_set();

        let m1 = member(node(1));
        let m2 = member(node(2));
        let m3 = member(node(3));
        let m4 = member(node(4));
        let m5 = member(node(5));
        let m6 = member(node(6));

        assert_eq!(set.insert(m1, tape(10)), Ok(0));
        assert_eq!(set.insert(m2, tape(9)), Ok(1));
        assert_eq!(set.insert(m3, tape(8)), Ok(2));
        assert_eq!(set.insert(m4, tape(7)), Ok(3));
        assert_eq!(set.insert(m5, tape(6)), Ok(4));

        let mut total = 10 + 9 + 8 + 7 + 6;
        assert_eq!(set.total_stake(), tape(total));

        // Full. Replace min (stake 6) if better with 11.
        let replaced_idx = set.replace_if_better(m6, tape(11)).expect("should replace min");
        assert_eq!(set.stake_at(replaced_idx), tape(11));

        total = total - 6 + 11;
        assert_eq!(set.total_stake(), tape(total));

        let active_ids = set.leader_ids();

        // node 5 should not be part of the set
        assert!(!active_ids.contains(&m5.id));

        // all other nodes should be
        assert!(active_ids.contains(&m1.id));
        assert!(active_ids.contains(&m2.id));
        assert!(active_ids.contains(&m3.id));
        assert!(active_ids.contains(&m4.id));
        assert!(active_ids.contains(&m6.id));
    }

    #[test]
    fn leader_evict_correct_node_with_updates() {
        const N: usize = 5;
        let mut set: LeaderSet<N> = empty_leader_set();

        let nodes = [
            member(node(1)),
            member(node(2)),
            member(node(3)),
            member(node(4)),
            member(node(5)),
            member(node(6)),
        ];

        // Insert out of order
        assert_eq!(set.insert(nodes[3], tape(7)), Ok(0));
        assert_eq!(set.insert(nodes[0], tape(10)), Ok(0));
        assert_eq!(set.insert(nodes[2], tape(8)), Ok(1));
        assert_eq!(set.insert(nodes[1], tape(9)), Ok(1));
        assert_eq!(set.insert(nodes[4], tape(6)), Ok(4));

        let mut total = 10 + 9 + 8 + 7 + 6;
        assert_eq!(set.total_stake(), tape(total));

        // Update node[0] to 12
        let old = set.update_stake(&nodes[0].id, tape(12)).unwrap();
        assert_eq!(old, tape(10));
        total = total - 10 + 12;
        assert_eq!(set.total_stake(), tape(total));
        assert_eq!(stake_for_node(&set, &nodes[0].id), Some(tape(12)));

        // Update node[2] to 13
        let old = set.update_stake(&nodes[2].id, tape(13)).unwrap();
        assert_eq!(old, tape(8));
        total = total - 8 + 13;
        assert_eq!(set.total_stake(), tape(total));
        assert_eq!(stake_for_node(&set, &nodes[2].id), Some(tape(13)));

        // Update node[3] to 9
        let old = set.update_stake(&nodes[3].id, tape(9)).unwrap();
        assert_eq!(old, tape(7));
        total = total - 7 + 9;
        assert_eq!(set.total_stake(), tape(total));
        assert_eq!(stake_for_node(&set, &nodes[3].id), Some(tape(9)));

        // Update node[1] to 10
        let old = set.update_stake(&nodes[1].id, tape(10)).unwrap();
        assert_eq!(old, tape(9));
        total = total - 9 + 10;
        assert_eq!(set.total_stake(), tape(total));
        assert_eq!(stake_for_node(&set, &nodes[1].id), Some(tape(10)));

        // Update node[4] to 7
        let old = set.update_stake(&nodes[4].id, tape(7)).unwrap();
        assert_eq!(old, tape(6));
        total = total - 6 + 7;
        assert_eq!(set.total_stake(), tape(total));
        assert_eq!(stake_for_node(&set, &nodes[4].id), Some(tape(7)));

        // Insert node[5] with 11; set is full so use replacement; should evict current min (7)
        let replaced_idx = set.replace_if_better(nodes[5], tape(11)).expect("should replace min with 11");
        assert_eq!(set.stake_at(replaced_idx), tape(11));
        total = total - 7 + 11;
        assert_eq!(set.total_stake(), tape(total));
        assert_eq!(stake_for_node(&set, &nodes[5].id), Some(tape(11)));

        let active_ids = set.leader_ids();
        assert!(!active_ids.contains(&nodes[4].id)); // evicted
        assert!(active_ids.contains(&nodes[0].id));
        assert!(active_ids.contains(&nodes[1].id));
        assert!(active_ids.contains(&nodes[2].id));
        assert!(active_ids.contains(&nodes[3].id));
        assert!(active_ids.contains(&nodes[5].id));
    }

    #[test]
    fn leader_insert_equal_min_does_not_replace() {
        const N: usize = 4;
        let mut set: LeaderSet<N> = empty_leader_set();

        let a = member(node(1));
        let b = member(node(2));
        let ccc = member(node(3));
        let d = member(node(4));
        let e = member(node(5));

        assert_eq!(set.insert(a, tape(10)), Ok(0));
        assert_eq!(set.insert(b, tape(9)), Ok(1));
        assert_eq!(set.insert(ccc, tape(8)), Ok(2));
        assert_eq!(set.insert(d, tape(6)), Ok(3));

        // Full, min = 6. Try to replace with equal stake 6; should NOT replace.
        let err = set.replace_if_better(e, tape(6)).unwrap_err();
        assert!(matches!(err, LeaderSetError::NotBetter { .. }));

        // Insert should also fail because full.
        assert_eq!(set.insert(e, tape(6)), Err(LeaderSetError::Full));

        let ids = set.leader_ids();
        assert!(ids.contains(&a.id));
        assert!(ids.contains(&b.id));
        assert!(ids.contains(&ccc.id));
        assert!(ids.contains(&d.id));
        assert!(!ids.contains(&e.id));
    }

    #[test]
    fn leader_update_below_threshold_removes_when_full() {
        const N: usize = 5;
        let mut set: LeaderSet<N> = empty_leader_set();

        let a = member(node(1)); // 10
        let b = member(node(2)); // 9
        let c = member(node(3)); // 8
        let d = member(node(4)); // 7
        let e = member(node(5)); // 6

        assert_eq!(set.insert(a, tape(10)), Ok(0));
        assert_eq!(set.insert(b, tape(9)), Ok(1));
        assert_eq!(set.insert(c, tape(8)), Ok(2));
        assert_eq!(set.insert(d, tape(7)), Ok(3));
        assert_eq!(set.insert(e, tape(6)), Ok(4));

        let total_before = 10 + 9 + 8 + 7 + 6;
        assert_eq!(set.total_stake(), tape(total_before));

        // Emulate old policy explicitly: when full, updating below the current threshold removes.
        let threshold_before = set.threshold_stake(); // 6
        let new_stake_for_c = tape(5);
        if new_stake_for_c < threshold_before {
            let (removed_member, removed_stake) = set.remove(&c.id).expect("c should be removed");
            assert_eq!(removed_member.id, c.id);
            assert_eq!(removed_stake, tape(8));
        } else {
            let _old = set.update_stake(&c.id, new_stake_for_c).unwrap();
        }

        let ids = set.leader_ids();
        assert!(!ids.contains(&c.id));
        let total_after = total_before - 8; // c removed
        assert_eq!(set.total_stake(), tape(total_after));
    }

    #[test]
    fn leader_ids_and_stake_parallel() {
        const N: usize = 3;
        let mut set: LeaderSet<N> = empty_leader_set();

        let a = member(node(1));
        let b = member(node(2));
        let c = member(node(3));

        assert_eq!(set.insert(a, tape(5)), Ok(0));
        assert_eq!(set.insert(b, tape(10)), Ok(0));
        assert_eq!(set.insert(c, tape(20)), Ok(0));

        let (ids, stakes) = set.leader_ids_and_stake();

        assert_eq!(ids.len(), 3);
        assert_eq!(stakes.len(), 3);

        // The internal order matches insertion order when not full and no swap-removals occurred
        assert_eq!(ids[0], c.id);
        assert_eq!(ids[1], b.id);
        assert_eq!(ids[2], a.id);

        assert_eq!(stakes[0], tape(20));
        assert_eq!(stakes[1], tape(10));
        assert_eq!(stakes[2], tape(5));
    }

    #[test]
    fn try_nominate_inserts_when_not_full() {
        const N: usize = 3;
        let mut set: LeaderSet<N> = empty_leader_set();

        let a = member(node(1));
        let b = member(node(2));

        assert_eq!(set.try_join(a, tape(10)), Ok(0));
        assert_eq!(set.try_join(b, tape(5)), Ok(1));
        assert!(set.contains(&a.id) && set.contains(&b.id));
    }

    #[test]
    fn try_join_replaces_when_full_and_better() {
        const N: usize = 3;
        let mut set: LeaderSet<N> = empty_leader_set();

        let a = member(node(1));
        let b = member(node(2));
        let c = member(node(3));
        let d = member(node(4)); // challenger

        assert_eq!(set.try_join(a, tape(10)), Ok(0));
        assert_eq!(set.try_join(b, tape(9)), Ok(1));
        assert_eq!(set.try_join(c, tape(6)), Ok(2));

        // Full now; challenger beats min (6)
        let idx = set.try_join(d, tape(11)).expect("should replace min");
        assert_eq!(set.stake_at(idx), tape(11));
        assert!(!set.contains(&c.id));
        assert!(set.contains(&d.id));
    }

    #[test]
    fn try_join_rejects_when_full_and_not_better() {
        const N: usize = 2;
        let mut set: LeaderSet<N> = empty_leader_set();

        let a = member(node(1));
        let b = member(node(2));
        let c = member(node(3));

        assert_eq!(set.try_join(a, tape(10)), Ok(0));
        assert_eq!(set.try_join(b, tape(7)), Ok(1));

        // Full; c == min (7) is not strictly better
        let err = set.try_join(c, tape(7)).unwrap_err();
        assert!(matches!(err, LeaderSetError::NotBetter { .. }));
        assert!(!set.contains(&c.id));
    }

    #[test]
    fn try_join_rejects_when_already_present() {
        const N: usize = 3;
        let mut set: LeaderSet<N> = empty_leader_set();

        let a = member(node(1));

        assert_eq!(set.try_join(a, tape(10)), Ok(0));
        let err = set.try_join(a, tape(12)).unwrap_err();
        assert!(matches!(err, LeaderSetError::AlreadyPresent { .. }));
        // Stake unchanged by try_join
        let idx = set.index_of(&a.id).unwrap();
        assert_eq!(set.stake_at(idx), tape(10));
    }

    #[test]
    fn try_join_rejects_zero_stake() {
        const N: usize = 2;
        let mut set: LeaderSet<N> = empty_leader_set();

        let a = member(node(1));
        let b = member(node(2));

        let err = set.try_join(a, TAPE::zero()).unwrap_err();
        assert!(matches!(err, LeaderSetError::ZeroStake));

        assert_eq!(set.try_join(a, tape(5)), Ok(0));
        assert_eq!(set.try_join(b, tape(6)), Ok(0));

        // Full; zero still invalid
        let c = member(node(3));
        let err = set.try_join(c, TAPE::zero()).unwrap_err();
        assert!(matches!(err, LeaderSetError::ZeroStake));
    }

    #[test]
    fn appointed_weights_and_seats() {
        const N: usize = 4;
        const M: usize = 6;
        let mut app: AppointedSet<N, M> = AppointedSet::new();

        let a = member(node(1));
        let b = member(node(2));
        let c = member(node(3));

        app.members[0] = a;
        app.members[1] = b;
        app.members[2] = c;
        app.member_count = 3;

        // Assign seats: 0->a, 1->a, 2->b, 3->c, 4->b, 5->a
        app.seats = [0, 0, 1, 2, 1, 0];

        assert!(app.contains(&a.id));
        assert!(app.contains(&b.id));
        assert!(app.contains(&c.id));
        assert!(!app.contains(&node(9))); // not present

        assert_eq!(app.node_weight(&a.id), 3);
        assert_eq!(app.node_weight(&b.id), 2);
        assert_eq!(app.node_weight(&c.id), 1);

        let weights = app.weights();
        assert_eq!(weights.get(&a.id), Some(&3));
        assert_eq!(weights.get(&b.id), Some(&2));
        assert_eq!(weights.get(&c.id), Some(&1));
        assert_eq!(weights.len(), 3);

        let a_seats = app.seats_for(&a.id);
        let b_seats = app.seats_for(&b.id);
        let c_seats = app.seats_for(&c.id);

        assert_eq!(a_seats, vec![0, 1, 5]);
        assert_eq!(b_seats, vec![2, 4]);
        assert_eq!(c_seats, vec![3]);
    }

    #[test]
    fn appointed_ignores_invalid_seats() {
        const N: usize = 3;
        const M: usize = 5;
        let mut app: AppointedSet<N, M> = AppointedSet::new();

        let a = member(node(1));
        let b = member(node(2));
        let c = member(node(3));

        app.members[0] = a;
        app.members[1] = b;
        app.members[2] = c;
        app.member_count = 3;

        // Two seats reference invalid member index 3 (>= member_count)
        app.seats = [0, 1, 3, 0, 3];

        // Only seats 0,1,3 should count
        assert_eq!(app.node_weight(&a.id), 2);
        assert_eq!(app.node_weight(&b.id), 1);
        assert_eq!(app.node_weight(&c.id), 0);

        let weights = app.weights();
        assert_eq!(weights.get(&a.id), Some(&2));
        assert_eq!(weights.get(&b.id), Some(&1));
        assert_eq!(weights.get(&c.id), Some(&0));
    }

    #[test]
    fn appointed_new_is_empty() {
        const N: usize = 5;
        const M: usize = 8;
        let app: AppointedSet<N, M> = AppointedSet::new();

        assert_eq!(app.size(), 0);
        assert!(!app.contains(&node(1)));

        assert!(app.seats_for(&node(1)).is_empty());
        assert_eq!(app.node_weight(&node(1)), 0);

        let weights = app.weights();
        assert!(weights.is_empty());
    }
}
