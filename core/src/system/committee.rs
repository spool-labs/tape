use core::fmt;
use crate::types::*;
use crate::bls::*;
use bytemuck::{Pod, Zeroable};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommitteeError {
    AlreadyPresent { idx: usize },
    Full,
    NotFull,
    NotFound,
    NotBetter { min_idx: usize, min_stake: Coin<TAPE> },
    ZeroStake,
}

#[repr(C)]
#[derive(Clone, Copy, PartialEq, Zeroable, Pod, Debug)]
pub struct CommitteeMember {
    pub id: NodeId,
    pub stake: Coin<TAPE>,
    pub key: BlsPubkey,
    pub blacklist: StorageUnits,
}

#[repr(C)]
#[derive(Clone, Copy, PartialEq)]
pub struct Committee<const NODES: usize> {
    pub member_count: u64,
    pub members: [CommitteeMember; NODES],
}

unsafe impl<const NODES: usize> Zeroable for Committee<NODES> {}
unsafe impl<const NODES: usize> Pod for Committee<NODES> {}

impl<const NODES: usize> Committee<NODES> {
    /// Creates a new, empty Committee.
    pub fn new() -> Self {
        Committee {
            member_count: 0,
            members: [CommitteeMember::zeroed(); NODES],
        }
    }

    /// Creates a new Committee from the given members.
    pub fn from_members(members: &[CommitteeMember]) -> Self {
        let mut committee = Self::new();

        for (i, member) in members.iter().take(NODES).enumerate() {
            committee.members[i] = *member;
            committee.member_count += 1;
        }

        committee.sort_active_desc();
        committee
    }

    /// Number of active members in the set.
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

    /// Checks if the given NodeId is present in the set.
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

    /// Get a member with the given NodeId, if any.
    #[inline]
    pub fn get_member(&self, node_id: &NodeId) -> Option<(CommitteeMember, usize)> {
        let Some(idx) = self.index_of(node_id) else {
            return None;
        };

        Some((self.members[idx], idx))
    }

    /// Helper: get stake for NodeId, if any.
    #[inline]
    pub fn get_stake(&self, node_id: &NodeId) -> Option<Coin<TAPE>> {
        self.index_of(node_id).map(|i| self.members[i].stake)
    }

    /// Helper: find index of minimum stake (among active members).
    #[inline]
    pub fn min_stake_index(&self) -> Option<usize> {
        #[cfg(debug_assertions)]
        debug_assert!(self.is_sorted(), "not sorted");

        let count = self.size();
        if count == 0 {
            return None;
        }
        Some(count - 1)
    }

    /// Minimum stake in the set (0 if empty).
    #[inline]
    pub fn threshold_stake(&self) -> Coin<TAPE> {
        self.min_stake_index()
            .map(|i| self.members[i].stake)
            .unwrap_or(TAPE::zero())
    }

    /// Total stake in the set.
    pub fn total_stake(&self) -> Coin<TAPE> {
        let count = self.size();
        let mut sum = TAPE::zero();
        for i in 0..count {
            sum = sum.saturating_add(self.members[i].stake);
        }
        sum
    }

    /// Tries to join a new member with the given stake. If the set is not full, it inserts the
    /// new member. If the set is full, it replaces the current minimum-stake member.
    #[inline]
    pub fn try_join(
        &mut self,
        member: &CommitteeMember
    ) -> Result<usize, CommitteeError> {
        if self.is_full() {
            self.replace_if_better(member)
        } else {
            self.insert(member)
        }
    }

    /// Inserts a new node if there is capacity. Never evicts.
    /// Returns the index where the member was inserted.
    pub fn insert(
        &mut self,
        member: &CommitteeMember
    ) -> Result<usize, CommitteeError> {
        let id = member.id;
        let staked_amount = member.stake;

        if staked_amount == TAPE::zero() {
            return Err(CommitteeError::ZeroStake);
        }

        if let Some(idx) = self.index_of(&id) {
            return Err(CommitteeError::AlreadyPresent { idx });
        }

        let count = self.size();
        if count >= NODES {
            return Err(CommitteeError::Full);
        }

        self.members[count] = *member;
        self.member_count = (count + 1) as u64;

        self.sort_active_desc();

        let new_index = self
            .index_of(&id)
            .expect("just inserted");

        Ok(new_index)
    }

    /// Replaces the current minimum-stake member if the set is full and the new stake is strictly larger.
    /// Returns the index replaced on success.
    pub fn replace_if_better(
        &mut self,
        member: &CommitteeMember
    ) -> Result<usize, CommitteeError> {
        let id = member.id;
        let staked_amount = member.stake;

        if staked_amount == TAPE::zero() {
            return Err(CommitteeError::ZeroStake);
        }

        if let Some(idx) = self.index_of(&id) {
            return Err(CommitteeError::AlreadyPresent { idx });
        }

        if !self.is_full() {
            return Err(CommitteeError::NotFull);
        }

        let Some(min_idx) = self.min_stake_index() else {
            return Err(CommitteeError::NotFull);
        };

        let min_val = self.members[min_idx].stake;
        if staked_amount <= min_val {
            return Err(CommitteeError::NotBetter { min_idx, min_stake: min_val });
        }

        self.members[min_idx] = *member;
        self.sort_active_desc();

        let new_index = self
            .index_of(&id)
            .expect("just inserted");

        Ok(new_index)
    }

    /// Updates the staked amount of the node with the given NodeId.
    /// Never removes. Returns the previous stake on success.
    pub fn update_stake(
        &mut self,
        node_id: &NodeId,
        new_stake: Coin::<TAPE>,
    ) -> Result<Coin<TAPE>, CommitteeError> {
        let Some(idx) = self.index_of(node_id) else {
            return Err(CommitteeError::NotFound);
        };

        let old = self.members[idx].stake;
        self.members[idx].stake = new_stake;

        self.sort_active_desc();

        Ok(old)
    }

    /// Removes a node with the given NodeId from the set using unordered swap-remove semantics.
    /// Returns the removed member and its stake.
    pub fn remove(&mut self, node_id: &NodeId) -> Result<(NodeId, Coin<TAPE>), CommitteeError> {
        let Some(idx) = self.index_of(node_id) else {
            return Err(CommitteeError::NotFound);
        };

        let count = self.size();
        debug_assert!(idx < count);

        let removed = self.members[idx];

        let last = count - 1;
        if idx != last {
            self.members[idx] = self.members[last];
        }

        self.members[last] = CommitteeMember::zeroed();
        self.member_count = count as u64 - 1;

        self.sort_active_desc();

        Ok((removed.id, removed.stake))
    }

    /// Returns an iterator over CommitteeMembers.
    #[inline]
    pub fn iter(&self) -> impl Iterator<Item = CommitteeMember> + '_ {
        let count = self.size();
        self.members[..count].iter().copied()
    }

    /// Array of active member NodeIds (sorted by descending stake).
    #[inline]
    pub fn active_members(&self) -> Vec<NodeId> {
        let count = self.size();
        let mut ids: Vec<NodeId> = Vec::with_capacity(count);
        for i in 0..count {
            ids.push(self.members[i].id);
        }
        ids
    }

    /// Array of active member stakes (sorted by descending stake).
    #[inline]
    pub fn active_stakes(&self) -> Vec<Coin<TAPE>> {
        let count = self.size();
        let mut stakes: Vec<Coin<TAPE>> = Vec::with_capacity(count);
        for i in 0..count {
            stakes.push(self.members[i].stake);
        }
        stakes
    }

    /// Sorts the active members in-place by descending stake, then ascending NodeId for determinism.
    #[inline]
    fn sort_active_desc(&mut self) {
        let count = self.size();
        if count <= 1 {
            return;
        }

        let mut entries: Vec<CommitteeMember> = (0..count).map(|i| self.members[i]).collect();

        entries.sort_by(|a, b| {
            // Highest stake first, then NodeId ascending
            b.stake.cmp(&a.stake).then(a.id.cmp(&b.id))
        });

        for i in 0..count {
            self.members[i] = entries[i];
        }
    }

    #[cfg(debug_assertions)]
    fn is_sorted(&self) -> bool {
        let count = self.size();
        for i in 1..count {
            let a = self.members[i - 1].stake;
            let b = self.members[i].stake;
            if a < b {
                return false;
            }
        }
        true
    }
}

impl<const NODES: usize> fmt::Debug for Committee<NODES> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let count = self.size();
        f.debug_struct("Committee")
            .field("member_count", &count)
            .field("members", &self.iter().collect::<Vec<_>>())
            .finish()
    }
}

impl<const NODES: usize> fmt::Display for Committee<NODES> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let count = self.size();
        write!(f, "Committee(size={}, members=[", count)?;
        for (i, m) in self.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{{ id: {:?}, stake: {:?} }}", m.id, m.stake)?;
        }
        write!(f, "])")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tape(v: u64) -> Coin<TAPE> { TAPE::new(v) }
    fn node(n: u8) -> NodeId { NodeId::new(n as u64) }
    fn empty_set<const N: usize>() -> Committee<N> { Committee::new() }
    fn member(id: NodeId, stake: u64) -> CommitteeMember {
        CommitteeMember {
            id,
            stake: TAPE(stake),
            key: BlsPubkey::zeroed(),
            blacklist: StorageUnits::zeroed(),
        }
    }

    #[test]
    fn evict_node() {
        const N: usize = 5;
        let mut set: Committee<N> = empty_set();

        let m1 = node(1);
        let m2 = node(2);
        let m3 = node(3);
        let m4 = node(4);
        let m5 = node(5);
        let m6 = node(6);

        assert_eq!(set.insert(&member(m1, 10)), Ok(0));
        assert_eq!(set.insert(&member(m2, 9)), Ok(1));
        assert_eq!(set.insert(&member(m3, 8)), Ok(2));
        assert_eq!(set.insert(&member(m4, 7)), Ok(3));
        assert_eq!(set.insert(&member(m5, 6)), Ok(4));

        let mut total = 10 + 9 + 8 + 7 + 6;
        assert_eq!(set.total_stake(), tape(total));

        let replaced_idx = set.replace_if_better(&member(m6, 11)).expect("should replace min");
        let replaced = set.iter().nth(replaced_idx).unwrap();
        assert_eq!(replaced.id, m6);
        assert_eq!(replaced.stake, tape(11));

        total = total - 6 + 11;
        assert_eq!(set.total_stake(), tape(total));

        let active_ids: Vec<_> = set.active_members();

        assert!(!active_ids.contains(&m5));
        assert!(active_ids.contains(&m1));
        assert!(active_ids.contains(&m2));
        assert!(active_ids.contains(&m3));
        assert!(active_ids.contains(&m4));
        assert!(active_ids.contains(&m6));
    }

    #[test]
    fn evict_update() {
        const N: usize = 5;
        let mut set: Committee<N> = empty_set();

        let nodes = [
            node(1),
            node(2),
            node(3),
            node(4),
            node(5),
            node(6),
        ];

        assert_eq!(set.insert(&member(nodes[3], 7)), Ok(0));
        assert_eq!(set.insert(&member(nodes[0], 10)), Ok(0));
        assert_eq!(set.insert(&member(nodes[2], 8)), Ok(1));
        assert_eq!(set.insert(&member(nodes[1], 9)), Ok(1));
        assert_eq!(set.insert(&member(nodes[4], 6)), Ok(4));

        let mut total = 10 + 9 + 8 + 7 + 6;
        assert_eq!(set.total_stake(), tape(total));

        let old = set.update_stake(&nodes[0], tape(12)).unwrap();
        assert_eq!(old, tape(10));
        total = total - 10 + 12;
        assert_eq!(set.total_stake(), tape(total));
        assert_eq!(set.get_stake(&nodes[0]), Some(tape(12)));

        let old = set.update_stake(&nodes[2], tape(13)).unwrap();
        assert_eq!(old, tape(8));
        total = total - 8 + 13;
        assert_eq!(set.total_stake(), tape(total));
        assert_eq!(set.get_stake(&nodes[2]), Some(tape(13)));

        let old = set.update_stake(&nodes[3], tape(9)).unwrap();
        assert_eq!(old, tape(7));
        total = total - 7 + 9;
        assert_eq!(set.total_stake(), tape(total));
        assert_eq!(set.get_stake(&nodes[3]), Some(tape(9)));

        let old = set.update_stake(&nodes[1], tape(10)).unwrap();
        assert_eq!(old, tape(9));
        total = total - 9 + 10;
        assert_eq!(set.total_stake(), tape(total));
        assert_eq!(set.get_stake(&nodes[1]), Some(tape(10)));

        let old = set.update_stake(&nodes[4], tape(7)).unwrap();
        assert_eq!(old, tape(6));
        total = total - 6 + 7;
        assert_eq!(set.total_stake(), tape(total));
        assert_eq!(set.get_stake(&nodes[4]), Some(tape(7)));

        let replaced_idx = set.replace_if_better(&member(nodes[5], 11)).expect("should replace min with 11");
        let replaced = set.iter().nth(replaced_idx).unwrap();
        assert_eq!(replaced.id, nodes[5]);
        assert_eq!(replaced.stake, tape(11));

        total = total - 7 + 11;
        assert_eq!(set.total_stake(), tape(total));
        assert_eq!(set.get_stake(&nodes[5]), Some(tape(11)));

        let active_ids: Vec<_> = set.active_members();
        assert!(!active_ids.contains(&nodes[4]));
        assert!(active_ids.contains(&nodes[0]));
        assert!(active_ids.contains(&nodes[1]));
        assert!(active_ids.contains(&nodes[2]));
        assert!(active_ids.contains(&nodes[3]));
        assert!(active_ids.contains(&nodes[5]));
    }

    #[test]
    fn insert_equal() {
        const N: usize = 4;
        let mut set: Committee<N> = empty_set();

        let a = node(1);
        let b = node(2);
        let c = node(3);
        let d = node(4);
        let e = node(5);

        assert_eq!(set.insert(&member(a, 10)), Ok(0));
        assert_eq!(set.insert(&member(b, 9)), Ok(1));
        assert_eq!(set.insert(&member(c, 8)), Ok(2));
        assert_eq!(set.insert(&member(d, 6)), Ok(3));

        let err = set.replace_if_better(&member(e, 6)).unwrap_err();
        assert!(matches!(err, CommitteeError::NotBetter { .. }));

        assert_eq!(set.insert(&member(e, 6)), Err(CommitteeError::Full));

        let ids: Vec<_> = set.active_members();
        assert!(ids.contains(&a));
        assert!(ids.contains(&b));
        assert!(ids.contains(&c));
        assert!(ids.contains(&d));
        assert!(!ids.contains(&e));
    }

    #[test]
    fn update_remove() {
        const N: usize = 5;
        let mut set: Committee<N> = empty_set();

        let a = node(1);
        let b = node(2);
        let c = node(3);
        let d = node(4);
        let e = node(5);

        assert_eq!(set.insert(&member(a, 10)), Ok(0));
        assert_eq!(set.insert(&member(b, 9)), Ok(1));
        assert_eq!(set.insert(&member(c, 8)), Ok(2));
        assert_eq!(set.insert(&member(d, 7)), Ok(3));
        assert_eq!(set.insert(&member(e, 6)), Ok(4));

        let total_before = 10 + 9 + 8 + 7 + 6;
        assert_eq!(set.total_stake(), tape(total_before));

        let threshold_before = set.threshold_stake();
        let new_stake_for_c = tape(5);
        if new_stake_for_c < threshold_before {
            let (removed_member, removed_stake) = set.remove(&c).expect("c should be removed");
            assert_eq!(removed_member, c);
            assert_eq!(removed_stake, tape(8));
        } else {
            let _old = set.update_stake(&c, new_stake_for_c).unwrap();
        }

        let ids: Vec<_> = set.active_members();
        assert!(!ids.contains(&c));
        let total_after = total_before - 8;
        assert_eq!(set.total_stake(), tape(total_after));
    }

    #[test]
    fn nominate_insert() {
        const N: usize = 3;
        let mut set: Committee<N> = empty_set();

        let a = node(1);
        let b = node(2);

        assert_eq!(set.try_join(&member(a, 10)), Ok(0));
        assert_eq!(set.try_join(&member(b, 5)), Ok(1));
        let ids: Vec<_> = set.active_members();
        assert!(ids.contains(&a) && ids.contains(&b));
    }

    #[test]
    fn join_replace() {
        const N: usize = 3;
        let mut set: Committee<N> = empty_set();

        let a = node(1);
        let b = node(2);
        let c = node(3);
        let d = node(4);

        assert_eq!(set.try_join(&member(a, 10)), Ok(0));
        assert_eq!(set.try_join(&member(b, 9)), Ok(1));
        assert_eq!(set.try_join(&member(c, 6)), Ok(2));

        let idx = set.try_join(&member(d, 11)).expect("should replace min");
        let replaced = set.iter().nth(idx).unwrap();
        assert_eq!(replaced.id, d);
        assert_eq!(replaced.stake, tape(11));

        let ids: Vec<_> = set.active_members();
        assert!(!ids.contains(&c));
        assert!(ids.contains(&d));
    }

    #[test]
    fn join_reject() {
        const N: usize = 2;
        let mut set: Committee<N> = empty_set();

        let a = node(1);
        let b = node(2);
        let c = node(3);

        assert_eq!(set.try_join(&member(a, 10)), Ok(0));
        assert_eq!(set.try_join(&member(b, 7)), Ok(1));

        let err = set.try_join(&member(c, 7)).unwrap_err();
        assert!(matches!(err, CommitteeError::NotBetter { .. }));
        let ids: Vec<_> = set.active_members();
        assert!(!ids.contains(&c));
    }

    #[test]
    fn join_present() {
        const N: usize = 3;
        let mut set: Committee<N> = empty_set();

        let a = node(1);

        assert_eq!(set.try_join(&member(a, 10)), Ok(0));
        let err = set.try_join(&member(a, 12)).unwrap_err();
        assert!(matches!(err, CommitteeError::AlreadyPresent { .. }));
        let idx = set.index_of(&a).unwrap();
        let m = set.iter().nth(idx).unwrap();
        assert_eq!(m.stake, tape(10));
    }

    #[test]
    fn join_zero() {
        const N: usize = 2;
        let mut set: Committee<N> = empty_set();

        let a = node(1);
        let b = node(2);

        let err = set.try_join(&member(a, 0)).unwrap_err();
        assert!(matches!(err, CommitteeError::ZeroStake));

        assert_eq!(set.try_join(&member(a, 5)), Ok(0));
        assert_eq!(set.try_join(&member(b, 6)), Ok(0));

        let c = node(3);
        let err = set.try_join(&member(c, 0)).unwrap_err();
        assert!(matches!(err, CommitteeError::ZeroStake));
    }
}
