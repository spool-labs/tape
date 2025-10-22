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
#[derive(Clone, Copy, PartialEq, Zeroable, Pod)]
pub struct CommitteeMember {
    pub id: NodeId,
    pub stake: Coin<TAPE>,
    pub key: BlsPubkey,
}

#[repr(C)]
#[derive(Clone, Copy, PartialEq)]
pub struct Committee<const NODES: usize> {
    pub member_count: u64,
    pub members: [NodeId; NODES],
    pub stakes: [Coin<TAPE>; NODES], // (index -> stake)
    pub keys: [BlsPubkey; NODES],    // (index -> bls pubkey)
}

unsafe impl<const NODES: usize> Zeroable for Committee<NODES> {}
unsafe impl<const NODES: usize> Pod for Committee<NODES> {}

impl<const NODES: usize> Committee<NODES> {

    /// Creates a new, empty Committee.
    pub fn new() -> Self {
        Committee {
            member_count: 0,
            members: [NodeId::zeroed(); NODES],
            stakes: [TAPE::zero(); NODES],
            keys: [BlsPubkey::zeroed(); NODES],
        }
    }

    /// Creates a new Committee from the given members.
    pub fn from_members(members: &[CommitteeMember]) -> Self {
        let mut committee = Self::new();

        for member in members.iter().take(NODES) {
            committee.members[committee.member_count as usize] = member.id;
            committee.stakes[committee.member_count as usize] = member.stake;
            committee.keys[committee.member_count as usize] = member.key;
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
        self.members[..count].iter().position(|m| m == node_id)
    }

    /// Helper: get stake at index as u64.
    #[inline]
    pub fn stake_at(&self, idx: usize) -> Coin<TAPE> {
        self.stakes[idx]
    }

    /// Helper: get stake for NodeId, if any.
    #[inline]
    pub fn get_stake_for(&self, node_id: &NodeId) -> Option<Coin<TAPE>> {
        self.index_of(node_id).map(|i| self.stake_at(i))
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

    /// Tries to join a new member with the given stake. If the set is not full, it inserts the
    /// new member. If the set is full, it replaces the current minimum-stake member.
    #[inline]
    pub fn try_join(
        &mut self,
        member: &NodeId,
        staked_amount: Coin<TAPE>,
    ) -> Result<usize, CommitteeError> {
        if self.is_full() {
            self.replace_if_better(member, staked_amount)
        } else {
            self.insert(member, staked_amount)
        }
    }

    /// Returns the BLS public key for the given member, if any.
    #[inline]
    pub fn bls_key_of(&self, member: &NodeId) -> Option<BlsPubkey> {
        let Some(idx) = self.index_of(member) else {
            return None;
        };

        Some(self.keys[idx])
    }

    /// Sets the BLS public key for the given member.
    #[inline]
    pub fn set_bls_key(&mut self, member: &NodeId, key: BlsPubkey) -> Result<(), CommitteeError> {
        let Some(idx) = self.index_of(member) else {
            return Err(CommitteeError::NotFound);
        };

        self.keys[idx] = key;
        Ok(())
    }

    /// Inserts a new node if there is capacity. Never evicts.
    /// Returns the index where the member was inserted.
    pub fn insert(
        &mut self,
        member: &NodeId, 
        staked_amount: Coin::<TAPE>
    ) -> Result<usize, CommitteeError> {

        if staked_amount == TAPE::zero() {
            return Err(CommitteeError::ZeroStake);
        }

        if let Some(idx) = self.index_of(&member) {
            return Err(CommitteeError::AlreadyPresent { idx });
        }

        let count = self.size();
        if count >= NODES {
            return Err(CommitteeError::Full);
        }

        self.members[count] = *member;
        self.keys[count] = BlsPubkey::zeroed();
        self.stakes[count] = staked_amount;
        self.member_count = (count + 1) as u64;

        self.sort_active_desc();

        Ok(self.index_of(&member).expect("just inserted"))
    }

    /// Replaces the current minimum-stake member if the set is full and the new stake is strictly larger.
    /// Returns the index replaced on success.
    pub fn replace_if_better(
        &mut self,
        member: &NodeId,
        staked_amount: Coin::<TAPE>,
    ) -> Result<usize, CommitteeError> {
        if staked_amount == TAPE::zero() {
            return Err(CommitteeError::ZeroStake);
        }

        if let Some(idx) = self.index_of(&member) {
            return Err(CommitteeError::AlreadyPresent { idx });
        }

        if !self.is_full() {
            return Err(CommitteeError::NotFull);
        }

        let Some(min_idx) = self.min_stake_index() else {
            return Err(CommitteeError::NotFull);
        };

        let min_val = self.stake_at(min_idx);
        if staked_amount <= min_val {
            return Err(CommitteeError::NotBetter { min_idx, min_stake: min_val });
        }

        self.members[min_idx] = *member;
        self.keys[min_idx] = BlsPubkey::zeroed();
        self.stakes[min_idx] = staked_amount;

        self.sort_active_desc();

        Ok(self.index_of(&member).expect("just inserted"))
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

        let old = self.stake_at(idx);
        self.stakes[idx] = new_stake;

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

        let removed_member = self.members[idx];
        let removed_stake = self.stake_at(idx);

        let last = count - 1;
        if idx != last {
            self.members[idx] = self.members[last];
            self.keys[idx] = self.keys[last];
            self.stakes[idx] = self.stakes[last];
        }

        self.members[last] = NodeId::zeroed();
        self.keys[last] = BlsPubkey::zeroed();
        self.stakes[last] = TAPE::zero();
        self.member_count = count as u64 - 1;

        self.sort_active_desc();

        Ok((removed_member, removed_stake))
    }

    /// Returns an iterator over the node IDs.
    #[inline]
    pub fn iter_members(&self) -> impl Iterator<Item = &NodeId> {
        let count = self.size();
        self.members[..count].iter()
    }

    /// Returns an iterator over the stakes.
    #[inline]
    pub fn iter_stakes(&self) -> impl Iterator<Item = &Coin<TAPE>> {
        let count = self.size();
        self.stakes[..count].iter()
    }

    /// Returns an iterator over the BLS public keys.
    #[inline]
    pub fn iter_bls_keys(&self) -> impl Iterator<Item = &BlsPubkey> {
        let count = self.size();
        self.keys[..count].iter()
    }

    /// Returns a slice of active members (sorted by descending stake).
    #[inline]
    pub fn active_members(&self) -> &[NodeId] {
        let count = self.size();
        &self.members[..count]
    }

    /// Returns a slice of active stakes (sorted by descending stake).
    #[inline]
    pub fn active_stakes(&self) -> &[Coin<TAPE>] {
        let count = self.size();
        &self.stakes[..count]
    }

    /// Returns a slice of active BLS public keys (sorted by descending stake).
    #[inline]
    pub fn active_bls_keys(&self) -> &[BlsPubkey] {
        let count = self.size();
        &self.keys[..count]
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
        let mut entries: Vec<(NodeId, BlsPubkey, Coin<TAPE>)> =
            (0..count).map(|i| (self.members[i], self.keys[i], self.stakes[i])).collect();

        entries.sort_by(|(ma, _ka, sa), (mb, _kb, sb)| {
            // Highest stake first
            sb.cmp(sa).then(ma.cmp(mb))
        });

        // Write back
        for i in 0..count {
            self.members[i] = entries[i].0;
            self.keys[i] = entries[i].1;
            self.stakes[i] = entries[i].2;
        }
    }

    #[cfg(debug_assertions)]
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

impl<const NODES: usize> fmt::Debug for Committee<NODES> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let count = self.size();
        f.debug_struct("Committee")
            .field("member_count", &count)
            .field("members", &&self.members[..count])
            .field("keys", &&self.keys[..count])
            .field("stakes", &&self.stakes[..count])
            .finish()
    }
}

impl<const NODES: usize> fmt::Display for Committee<NODES> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let count = self.size();
        write!(f, "Committee(size={}, members=[", count)?;
        for i in 0..count {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(
                f,
                "{{ id: {:?}, stake: {:?} }}",
                self.members[i], self.stakes[i]
            )?;
        }
        write!(f, "])")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tape(v: u64) -> Coin<TAPE> {
        TAPE::new(v)
    }

    fn node(n: u8) -> NodeId {
        NodeId::new(n as u64)
    }

    fn empty_set<const N: usize>() -> Committee<N> {
        Committee::new()
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

        assert_eq!(set.insert(&m1, tape(10)), Ok(0));
        assert_eq!(set.insert(&m2, tape(9)), Ok(1));
        assert_eq!(set.insert(&m3, tape(8)), Ok(2));
        assert_eq!(set.insert(&m4, tape(7)), Ok(3));
        assert_eq!(set.insert(&m5, tape(6)), Ok(4));

        let mut total = 10 + 9 + 8 + 7 + 6;
        assert_eq!(set.total_stake(), tape(total));

        let replaced_idx = set.replace_if_better(&m6, tape(11)).expect("should replace min");
        assert_eq!(set.stake_at(replaced_idx), tape(11));

        total = total - 6 + 11;
        assert_eq!(set.total_stake(), tape(total));

        let active_ids = set.active_members().to_vec();

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

        assert_eq!(set.insert(&nodes[3], tape(7)), Ok(0));
        assert_eq!(set.insert(&nodes[0], tape(10)), Ok(0));
        assert_eq!(set.insert(&nodes[2], tape(8)), Ok(1));
        assert_eq!(set.insert(&nodes[1], tape(9)), Ok(1));
        assert_eq!(set.insert(&nodes[4], tape(6)), Ok(4));

        let mut total = 10 + 9 + 8 + 7 + 6;
        assert_eq!(set.total_stake(), tape(total));

        let old = set.update_stake(&nodes[0], tape(12)).unwrap();
        assert_eq!(old, tape(10));
        total = total - 10 + 12;
        assert_eq!(set.total_stake(), tape(total));
        assert_eq!(set.get_stake_for(&nodes[0]), Some(tape(12)));

        let old = set.update_stake(&nodes[2], tape(13)).unwrap();
        assert_eq!(old, tape(8));
        total = total - 8 + 13;
        assert_eq!(set.total_stake(), tape(total));
        assert_eq!(set.get_stake_for(&nodes[2]), Some(tape(13)));

        let old = set.update_stake(&nodes[3], tape(9)).unwrap();
        assert_eq!(old, tape(7));
        total = total - 7 + 9;
        assert_eq!(set.total_stake(), tape(total));
        assert_eq!(set.get_stake_for(&nodes[3]), Some(tape(9)));

        let old = set.update_stake(&nodes[1], tape(10)).unwrap();
        assert_eq!(old, tape(9));
        total = total - 9 + 10;
        assert_eq!(set.total_stake(), tape(total));
        assert_eq!(set.get_stake_for(&nodes[1]), Some(tape(10)));

        let old = set.update_stake(&nodes[4], tape(7)).unwrap();
        assert_eq!(old, tape(6));
        total = total - 6 + 7;
        assert_eq!(set.total_stake(), tape(total));
        assert_eq!(set.get_stake_for(&nodes[4]), Some(tape(7)));

        let replaced_idx = set.replace_if_better(&nodes[5], tape(11)).expect("should replace min with 11");
        assert_eq!(set.stake_at(replaced_idx), tape(11));
        total = total - 7 + 11;
        assert_eq!(set.total_stake(), tape(total));
        assert_eq!(set.get_stake_for(&nodes[5]), Some(tape(11)));

        let active_ids = set.active_members().to_vec();
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

        assert_eq!(set.insert(&a, tape(10)), Ok(0));
        assert_eq!(set.insert(&b, tape(9)), Ok(1));
        assert_eq!(set.insert(&c, tape(8)), Ok(2));
        assert_eq!(set.insert(&d, tape(6)), Ok(3));

        let err = set.replace_if_better(&e, tape(6)).unwrap_err();
        assert!(matches!(err, CommitteeError::NotBetter { .. }));

        assert_eq!(set.insert(&e, tape(6)), Err(CommitteeError::Full));

        let ids = set.active_members().to_vec();
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

        assert_eq!(set.insert(&a, tape(10)), Ok(0));
        assert_eq!(set.insert(&b, tape(9)), Ok(1));
        assert_eq!(set.insert(&c, tape(8)), Ok(2));
        assert_eq!(set.insert(&d, tape(7)), Ok(3));
        assert_eq!(set.insert(&e, tape(6)), Ok(4));

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

        let ids = set.active_members().to_vec();
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

        assert_eq!(set.try_join(&a, tape(10)), Ok(0));
        assert_eq!(set.try_join(&b, tape(5)), Ok(1));
        assert!(set.contains(&a) && set.contains(&b));
    }

    #[test]
    fn join_replace() {
        const N: usize = 3;
        let mut set: Committee<N> = empty_set();

        let a = node(1);
        let b = node(2);
        let c = node(3);
        let d = node(4);

        assert_eq!(set.try_join(&a, tape(10)), Ok(0));
        assert_eq!(set.try_join(&b, tape(9)), Ok(1));
        assert_eq!(set.try_join(&c, tape(6)), Ok(2));

        let idx = set.try_join(&d, tape(11)).expect("should replace min");
        assert_eq!(set.stake_at(idx), tape(11));
        assert!(!set.contains(&c));
        assert!(set.contains(&d));
    }

    #[test]
    fn join_reject() {
        const N: usize = 2;
        let mut set: Committee<N> = empty_set();

        let a = node(1);
        let b = node(2);
        let c = node(3);

        assert_eq!(set.try_join(&a, tape(10)), Ok(0));
        assert_eq!(set.try_join(&b, tape(7)), Ok(1));

        let err = set.try_join(&c, tape(7)).unwrap_err();
        assert!(matches!(err, CommitteeError::NotBetter { .. }));
        assert!(!set.contains(&c));
    }

    #[test]
    fn join_present() {
        const N: usize = 3;
        let mut set: Committee<N> = empty_set();

        let a = node(1);

        assert_eq!(set.try_join(&a, tape(10)), Ok(0));
        let err = set.try_join(&a, tape(12)).unwrap_err();
        assert!(matches!(err, CommitteeError::AlreadyPresent { .. }));
        let idx = set.index_of(&a).unwrap();
        assert_eq!(set.stake_at(idx), tape(10));
    }

    #[test]
    fn join_zero() {
        const N: usize = 2;
        let mut set: Committee<N> = empty_set();

        let a = node(1);
        let b = node(2);

        let err = set.try_join(&a, TAPE::zero()).unwrap_err();
        assert!(matches!(err, CommitteeError::ZeroStake));

        assert_eq!(set.try_join(&a, tape(5)), Ok(0));
        assert_eq!(set.try_join(&b, tape(6)), Ok(0));

        let c = node(3);
        let err = set.try_join(&c, TAPE::zero()).unwrap_err();
        assert!(matches!(err, CommitteeError::ZeroStake));
    }
}
