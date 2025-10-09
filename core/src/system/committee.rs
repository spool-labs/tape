use crate::types::*;
use crate::bls::*;
use std::collections::BTreeMap;
use bytemuck::{Pod, Zeroable};

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

/// A CandidateSet defines a set of committee members that will be considered for appointment
/// during an upcoming epoch. Each member has an associated stake, which influences their
/// likelihood of being assigned seats in the committee.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct CandidateSet<const N: usize> {
    pub member_count: u64,
    pub members: [CommitteeMember; N],
    pub stakes: [Coin<TAPE>; N], // (member_index -> stake)
}

unsafe impl<const N: usize> Zeroable for CandidateSet<N> {}
unsafe impl<const N: usize> Pod for CandidateSet<N> {}

impl<const N: usize> CandidateSet<N> {
    /// Number of active members in the candidate set.
    #[inline]
    pub fn size(&self) -> usize {
        (self.member_count as usize).min(N)
    }

    /// Checks if the given NodeId is present in the candidate set.
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
    pub fn set_stake_at(&mut self, idx: usize, stake: Coin<TAPE>) {
        self.stakes[idx] = stake;
    }

    /// Helper: find index of minimum stake (among active members).
    pub fn min_stake_index(&self) -> Option<usize> {
        let count = self.size();
        if count == 0 {
            return None;
        }
        let mut min_idx = 0usize;
        let mut min_val = self.stake_at(0);
        for i in 1..count {
            let v = self.stake_at(i);
            if v < min_val {
                min_val = v;
                min_idx = i;
            }
        }
        Some(min_idx)
    }

    /// Helper: swap-remove at index, zeroing the freed slot.
    pub fn remove_index(&mut self, idx: usize) {
        let count = self.size();
        if count == 0 || idx >= count {
            return;
        }

        let last = count - 1;
        if idx != last {
            self.members[idx] = self.members[last];
            self.stakes[idx] = self.stakes[last];
        }

        self.members[last] = CommitteeMember::zeroed();
        self.stakes[last] = TAPE::zero();

        self.member_count = count as u64 - 1;
    }

    /// Inserts the node if it is not already present; otherwise updates its stake.
    /// If the new stake is zero or below the threshold, the node is removed.
    /// Returns true if the node is in the set after the operation.
    pub fn insert_or_update(&mut self, member: CommitteeMember, staked_amount: Coin::<TAPE>) -> bool {
        if let Some(idx) = self.index_of(&member.id) {
            let full = self.size() == N;
            let threshold = if full {
                self.threshold_stake()
            } else { 
                TAPE::zero() 
            };

            // If full and new stake is below threshold, remove the member
            if full && staked_amount < threshold {
                self.remove_index(idx);
                return false;
            }

            self.set_stake_at(idx, staked_amount);
            true
        } else {
            // Insert new
            self.insert(member, staked_amount)
        }
    }

    /// Updates the staked amount of the node with the given NodeId.
    /// Returns true if the node exists in the set (stake updated), false otherwise.
    pub fn update(&mut self, node_id: &NodeId, staked_amount: Coin::<TAPE>) -> bool {
        if let Some(idx) = self.index_of(node_id) {
            self.set_stake_at(idx, staked_amount);
            true
        } else {
            false
        }
    }

    /// Inserts a new node if it has enough stake to be included.
    /// - If there is capacity, the node is appended.
    /// - If full, the node replaces the current minimum-stake node iff its stake is strictly larger.
    /// Returns true if inserted, false otherwise (including if it already exists).
    pub fn insert(&mut self, member: CommitteeMember, staked_amount: Coin::<TAPE>) -> bool {
        if self.contains(&member.id) {
            return false;
        }

        // If the set is not full, append the member
        let count = self.size();
        if count < N {
            self.members[count] = member;
            self.set_stake_at(count, staked_amount);
            self.member_count = (count + 1) as u64;
            return true;
        }

        // Otherwise, replace the minimum stake member if the new stake is larger
        if let Some(min_idx) = self.min_stake_index() {
            let min_val = self.stake_at(min_idx);
            if staked_amount > min_val {
                self.members[min_idx] = member;
                self.set_stake_at(min_idx, staked_amount);
                return true;
            }
        }

        false
    }

    /// Removes a node with the given NodeId from the set (no-op if absent).
    pub fn remove(&mut self, node_id: &NodeId) {
        if let Some(idx) = self.index_of(node_id) {
            self.remove_index(idx);
        }
    }

    /// Returns the IDs of the nodes in the set.
    pub fn candidate_ids(&self) -> Vec<NodeId> {
        let count = self.size();
        self.members[..count].iter().map(|m| m.id).collect()
    }

    /// Returns parallel vectors of IDs and stake.
    pub fn candidate_ids_and_stake(&self) -> (Vec<NodeId>, Vec<Coin::<TAPE>>) {
        let count = self.size();
        let mut ids = Vec::with_capacity(count);
        let mut stakes = Vec::with_capacity(count);
        for i in 0..count {
            ids.push(self.members[i].id);
            stakes.push(self.stake_at(i));
        }
        (ids, stakes)
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
}

/// An AppointedSet defines a set of committee members and their assigned seats. The number of
/// seats assigned depends on the originating CandidateSet stakes. More stake usually means more
/// seats assigned to that member. The number of seats is finite and is distributed using the
/// Jefferson method (a.k.a. d'Hondt method). A single committee member is likely to be assigned
/// multiple seats and the seat count influences the weight of that node's signature in the
/// committee.
///
/// Each seat is uniquely identified by its index in the seats array and is not interchangeable.
/// Seat movement between epochs is minimized to reduce disruption (a pool dropping out and coming
/// back the next epoch will likely get the same set of seat indicies if all stake remains equal).
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct AppointedSet<const N: usize, const M: usize> {
    pub member_count: u64,
    pub members: [CommitteeMember; N],
    pub seats: [RelativeNodeId; M], // (seat_index -> member_index)
}

unsafe impl<const N: usize, const M: usize> Zeroable for AppointedSet<N, M> {}
unsafe impl<const N: usize, const M: usize> Pod for AppointedSet<N, M> {}

impl<const N: usize, const M: usize> AppointedSet<N, M> {

    /// Creates a new, empty AppointedSet.
    pub fn new() -> Self {
        assert!(N <= u8::MAX as usize);
        Self {
            member_count: 0,
            members: [CommitteeMember::zeroed(); N],
            seats: [0; M],
        }
    }

    /// Returns the size of the appointed committee (number of active members).
    #[inline]
    pub fn size(&self) -> usize {
        (self.member_count as usize).min(N)
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
                //*e = e.saturating_add(1);
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
    fn member_with_id(id: NodeId) -> CommitteeMember {
        CommitteeMember {
            id,
            key: BlsPubkey::zeroed(),
        }
    }

    // Helper to create an empty CandidateSet<N>
    fn empty_candidate_set<const N: usize>() -> CandidateSet<N> {
        CandidateSet {
            member_count: 0,
            members: [CommitteeMember::zeroed(); N],
            stakes: [TAPE::zero(); N],
        }
    }

    // Helper: stake for a node (used only in tests)
    fn stake_for_node<const N: usize>(set: &CandidateSet<N>, node_id: &NodeId) -> Option<Coin<TAPE>> {
        set.index_of(node_id).map(|i| set.stake_at(i))
    }

    #[test]
    fn candidate_evict_correct_node_simple() {
        const N: usize = 5;
        let mut set: CandidateSet<N> = empty_candidate_set();

        let m1 = member_with_id(node(1));
        let m2 = member_with_id(node(2));
        let m3 = member_with_id(node(3));
        let m4 = member_with_id(node(4));
        let m5 = member_with_id(node(5));
        let m6 = member_with_id(node(6));

        assert!(set.insert_or_update(m1, tape(10)));
        assert!(set.insert_or_update(m2, tape(9)));
        assert!(set.insert_or_update(m3, tape(8)));
        assert!(set.insert_or_update(m4, tape(7)));
        assert!(set.insert_or_update(m5, tape(6)));

        let mut total = 10 + 9 + 8 + 7 + 6;
        assert_eq!(set.total_stake(), tape(total));

        // Insert another node which should evict node 5 (stake 6)
        assert!(set.insert_or_update(m6, tape(11)));

        total = total - 6 + 11;
        assert_eq!(set.total_stake(), tape(total));

        let active_ids = set.candidate_ids();

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
    fn candidate_evict_correct_node_with_updates() {
        const N: usize = 5;
        let mut set: CandidateSet<N> = empty_candidate_set();

        let nodes = [
            member_with_id(node(1)),
            member_with_id(node(2)),
            member_with_id(node(3)),
            member_with_id(node(4)),
            member_with_id(node(5)),
            member_with_id(node(6)),
        ];

        // Insert out of order
        assert!(set.insert_or_update(nodes[3], tape(7)));
        assert!(set.insert_or_update(nodes[0], tape(10)));
        assert!(set.insert_or_update(nodes[2], tape(8)));
        assert!(set.insert_or_update(nodes[1], tape(9)));
        assert!(set.insert_or_update(nodes[4], tape(6)));

        let mut total = 10 + 9 + 8 + 7 + 6;
        assert_eq!(set.total_stake(), tape(total));

        // Update node[0] to 12
        assert!(set.insert_or_update(nodes[0], tape(12)));
        total = total - 10 + 12;
        assert_eq!(set.total_stake(), tape(total));
        assert_eq!(stake_for_node(&set, &nodes[0].id), Some(tape(12)));

        // Update node[2] to 13
        assert!(set.insert_or_update(nodes[2], tape(13)));
        total = total - 8 + 13;
        assert_eq!(set.total_stake(), tape(total));
        assert_eq!(stake_for_node(&set, &nodes[2].id), Some(tape(13)));

        // Update node[3] to 9
        assert!(set.insert_or_update(nodes[3], tape(9)));
        total = total - 7 + 9;
        assert_eq!(set.total_stake(), tape(total));
        assert_eq!(stake_for_node(&set, &nodes[3].id), Some(tape(9)));

        // Update node[1] to 10
        assert!(set.insert_or_update(nodes[1], tape(10)));
        total = total - 9 + 10;
        assert_eq!(set.total_stake(), tape(total));
        assert_eq!(stake_for_node(&set, &nodes[1].id), Some(tape(10)));

        // Update node[4] to 7
        assert!(set.insert_or_update(nodes[4], tape(7)));
        total = total - 6 + 7;
        assert_eq!(set.total_stake(), tape(total));
        assert_eq!(stake_for_node(&set, &nodes[4].id), Some(tape(7)));

        // Insert node[5] with 11; should evict node[4] (min = 7)
        assert!(set.insert_or_update(nodes[5], tape(11)));
        total = total - 7 + 11;
        assert_eq!(set.total_stake(), tape(total));
        assert_eq!(stake_for_node(&set, &nodes[5].id), Some(tape(11)));

        let active_ids = set.candidate_ids();
        assert!(!active_ids.contains(&nodes[4].id)); // evicted
        assert!(active_ids.contains(&nodes[0].id));
        assert!(active_ids.contains(&nodes[1].id));
        assert!(active_ids.contains(&nodes[2].id));
        assert!(active_ids.contains(&nodes[3].id));
        assert!(active_ids.contains(&nodes[5].id));
    }

    #[test]
    fn candidate_insert_equal_min_does_not_replace() {
        const N: usize = 4;
        let mut set: CandidateSet<N> = empty_candidate_set();

        let a = member_with_id(node(1));
        let b = member_with_id(node(2));
        let ccc = member_with_id(node(3));
        let d = member_with_id(node(4));
        let e = member_with_id(node(5));

        assert!(set.insert_or_update(a, tape(10)));
        assert!(set.insert_or_update(b, tape(9)));
        assert!(set.insert_or_update(ccc, tape(8)));
        assert!(set.insert_or_update(d, tape(6)));

        // Full, min = 6. Try to insert e with equal stake 6; should NOT replace.
        assert!(!set.insert_or_update(e, tape(6)));

        let ids = set.candidate_ids();
        assert!(ids.contains(&a.id));
        assert!(ids.contains(&b.id));
        assert!(ids.contains(&ccc.id));
        assert!(ids.contains(&d.id));
        assert!(!ids.contains(&e.id));
    }

    #[test]
    fn candidate_update_below_threshold_removes_when_full() {
        const N: usize = 5;
        let mut set: CandidateSet<N> = empty_candidate_set();

        let a = member_with_id(node(1)); // 10
        let b = member_with_id(node(2)); // 9
        let c_m = member_with_id(node(3)); // 8
        let d = member_with_id(node(4)); // 7
        let e = member_with_id(node(5)); // 6

        assert!(set.insert_or_update(a, tape(10)));
        assert!(set.insert_or_update(b, tape(9)));
        assert!(set.insert_or_update(c_m, tape(8)));
        assert!(set.insert_or_update(d, tape(7)));
        assert!(set.insert_or_update(e, tape(6)));

        let total_before = 10 + 9 + 8 + 7 + 6;
        assert_eq!(set.total_stake(), tape(total_before));

        // Full, threshold = current min = 6.
        // Update c (8 -> 5), which is below threshold => should remove c.
        assert!(!set.insert_or_update(c_m, tape(5)));

        let ids = set.candidate_ids();
        assert!(!ids.contains(&c_m.id));
        let total_after = total_before - 8; // c removed
        assert_eq!(set.total_stake(), tape(total_after));
    }

    #[test]
    fn candidate_ids_and_stake_parallel() {
        const N: usize = 3;
        let mut set: CandidateSet<N> = empty_candidate_set();

        let a = member_with_id(node(1));
        let b = member_with_id(node(2));
        let c_m = member_with_id(node(3));

        assert!(set.insert_or_update(a, tape(5)));
        assert!(set.insert_or_update(b, tape(10)));
        assert!(set.insert_or_update(c_m, tape(20)));

        let (ids, stakes) = set.candidate_ids_and_stake();

        assert_eq!(ids.len(), 3);
        assert_eq!(stakes.len(), 3);

        // The internal order matches insertion order when not full and no swap-removals occurred
        assert_eq!(ids[0], a.id);
        assert_eq!(ids[1], b.id);
        assert_eq!(ids[2], c_m.id);

        assert_eq!(stakes[0], tape(5));
        assert_eq!(stakes[1], tape(10));
        assert_eq!(stakes[2], tape(20));
    }

    #[test]
    fn appointed_weights_and_seats() {
        const N: usize = 4;
        const M: usize = 6;
        let mut app: AppointedSet<N, M> = AppointedSet::new();

        let a = member_with_id(node(1));
        let b = member_with_id(node(2));
        let c_m = member_with_id(node(3));

        app.members[0] = a;
        app.members[1] = b;
        app.members[2] = c_m;
        app.member_count = 3;

        // Assign seats: 0->a, 1->a, 2->b, 3->c, 4->b, 5->a
        app.seats = [0, 0, 1, 2, 1, 0];

        assert!(app.contains(&a.id));
        assert!(app.contains(&b.id));
        assert!(app.contains(&c_m.id));
        assert!(!app.contains(&node(9))); // not present

        assert_eq!(app.node_weight(&a.id), 3);
        assert_eq!(app.node_weight(&b.id), 2);
        assert_eq!(app.node_weight(&c_m.id), 1);

        let weights = app.weights();
        assert_eq!(weights.get(&a.id), Some(&3));
        assert_eq!(weights.get(&b.id), Some(&2));
        assert_eq!(weights.get(&c_m.id), Some(&1));
        assert_eq!(weights.len(), 3);

        let a_seats = app.seats_for(&a.id);
        let b_seats = app.seats_for(&b.id);
        let c_seats = app.seats_for(&c_m.id);

        assert_eq!(a_seats, vec![0, 1, 5]);
        assert_eq!(b_seats, vec![2, 4]);
        assert_eq!(c_seats, vec![3]);
    }

    #[test]
    fn appointed_ignores_invalid_seats() {
        const N: usize = 3;
        const M: usize = 5;
        let mut app: AppointedSet<N, M> = AppointedSet::new();

        let a = member_with_id(node(1));
        let b = member_with_id(node(2));
        let c_m = member_with_id(node(3));

        app.members[0] = a;
        app.members[1] = b;
        app.members[2] = c_m;
        app.member_count = 3;

        // Two seats reference invalid member index 3 (>= member_count)
        app.seats = [0, 1, 3, 0, 3];

        // Only seats 0,1,3 should count
        assert_eq!(app.node_weight(&a.id), 2);
        assert_eq!(app.node_weight(&b.id), 1);
        assert_eq!(app.node_weight(&c_m.id), 0);

        let weights = app.weights();
        assert_eq!(weights.get(&a.id), Some(&2));
        assert_eq!(weights.get(&b.id), Some(&1));
        assert_eq!(weights.get(&c_m.id), Some(&0));
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
