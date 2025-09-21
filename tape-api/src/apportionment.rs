use std::cmp::Ordering;
use std::collections::{BTreeMap, BinaryHeap};
use std::fmt;

use bytemuck::{Pod, Zeroable};

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug, Hash)]
pub struct NodeId(pub u64);

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let NodeId(id) = self;
        write!(f, "{id}")
    }
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct StakeLeaderSet<const N: usize> {
    pub len: u16,               // number of valid entries (<= N and <= u16::MAX)
    pub node_ids: [NodeId; N],  // sorted by NodeId (ascending)
    pub stakes: [u64; N],       // stakes[i] belongs to node_ids[i]
}

unsafe impl<const N: usize> Zeroable for StakeLeaderSet<N> {}
unsafe impl<const N: usize> Pod for StakeLeaderSet<N> {}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct Committee<const N: usize, const S: usize> {
    pub len: u16,               // number of nodes in committee
    pub node_ids: [NodeId; N],  // sorted by NodeId
    pub shard_counts: [u16; N], // shard count per node
    pub shard_offsets: [u16; N],// offset into `shards` for node i (0 for count = 0)
    pub shards: [u16; S],       // packed shard ids [0..total_shards-1]
}

unsafe impl<const N: usize, const S: usize> Zeroable for Committee<N, S> {}
unsafe impl<const N: usize, const S: usize> Pod for Committee<N, S> {}

impl<const N: usize, const S: usize> Committee<N, S> {

    pub const fn size_bytes() -> usize {
        std::mem::size_of::<Self>()
    }

    pub fn empty() -> Self {
        let mut out: Self = Zeroable::zeroed();
        out.len = 0;
        out
    }

    pub fn new(node_shards: BTreeMap<NodeId, u16>) -> Self {
        let node_count = node_shards.len();
        assert!(node_count <= N, "Too many nodes for N");

        let total_shards: usize = node_shards
            .values()
            .map(|&s| s as usize)
            .sum();

        assert!(total_shards <= S, "Too many shards for S");

        let (ids, counts, offsets, shards) = 
            pack_shards(node_shards.into_iter());

        let mut out = Self::empty();

        out.write_fixed(&ids, &counts, &offsets, &shards);
        out
    }

    pub fn from(
        prev: Option<&Committee<N, S>>,
        leaders: &StakeLeaderSet<N>,
        shard_count: u16,
    ) -> Committee<N, S> {
        let dist = stake_weighted_shard_counts(leaders, shard_count);
        if let Some(prev) = prev {
            if prev.size() == 0 {
                Committee::new(dist)
            } else {
                prev.apply_assignments(dist)
            }
        } else {
            Committee::new(dist)
        }
    }

    /// Copy vectors into fixed arrays and set `len`.
    fn write_fixed(&mut self, ids: &[NodeId], counts: &[u16], offsets: &[u16], shards: &[u16]) {
        assert!(ids.len() <= N, "Too many nodes for N");
        let total_shards: usize = counts.iter().map(|&c| c as usize).sum();
        assert!(total_shards <= S, "Too many shards for S");

        for (i, &nid) in ids.iter().enumerate() {
            self.node_ids[i] = nid;
            self.shard_counts[i] = counts[i];
            self.shard_offsets[i] = offsets[i];
        }
        for (i, &sh) in shards.iter().enumerate() {
            self.shards[i] = sh;
        }
        self.len = ids.len() as u16;
    }

    /// Expose current layout as NodeId -> Vec<shard_id>.
    fn current_layout(&self) -> BTreeMap<NodeId, Vec<u16>> {
        let mut m = BTreeMap::new();
        for i in 0..self.len as usize {
            let nid = self.node_ids[i];
            let off = self.shard_offsets[i] as usize;
            let cnt = self.shard_counts[i] as usize;
            m.insert(nid, self.shards[off..off + cnt].to_vec());
        }
        m
    }

    /// Transition to new per-node shard counts, reusing prior shard ids where possible.
    pub fn apply_assignments(&self, new_assignments: BTreeMap<NodeId, u16>) -> Self {
        let prev_layout = self.current_layout();

        let prev_total: u64 = prev_layout.values().map(|v| v.len() as u64).sum();
        let new_total: u64 = new_assignments.values().map(|&s| s as u64).sum();

        assert_eq!(prev_total, new_total);

        let (mut new_cmt, mut to_move, remaining_needs) =
            compute_allocations(new_assignments, &prev_layout);

        fill_deficits(&mut new_cmt, &mut to_move, &remaining_needs);

        let ids_vec: Vec<NodeId> = new_cmt
            .iter()
            .filter(|(_, v)| !v.is_empty())
            .map(|(&nid, _)| nid)
            .collect();

        let mut counts = Vec::with_capacity(ids_vec.len());
        let mut offsets = Vec::with_capacity(ids_vec.len());
        let total_shards: usize = new_cmt.values().map(|v| v.len()).sum();

        let mut shards = Vec::with_capacity(total_shards);
        let mut offset: u16 = 0;
        for nid in &ids_vec {
            let shrds = new_cmt.get(nid).unwrap();
            if !shrds.is_empty() {
                offsets.push(offset);
                counts.push(shrds.len() as u16);
                shards.extend_from_slice(shrds);
                offset = offset.wrapping_add(shrds.len() as u16);
            }
        }

        let mut out = Self::empty();
        out.write_fixed(&ids_vec, &counts, &offsets, &shards);
        out
    }

    fn index_of(&self, node_id: &NodeId) -> Option<usize> {
        self.node_ids[0..self.len as usize]
            .binary_search(node_id)
            .ok()
    }

    fn slice_for(&self, idx: usize) -> &[u16] {
        let off = self.shard_offsets[idx] as usize;
        let cnt = self.shard_counts[idx] as usize;
        &self.shards[off..off + cnt]
    }

    pub fn contains(&self, node_id: &NodeId) -> bool {
        self.index_of(node_id).is_some()
    }

    pub fn shards(&self, node_id: &NodeId) -> Option<&[u16]> {
        self.index_of(node_id).map(|idx| self.slice_for(idx))
    }

    pub fn size(&self) -> usize {
        self.len as usize
    }

    pub fn total_shards(&self) -> usize {
        (0..self.len as usize)
            .map(|i| self.shard_counts[i] as usize)
            .sum()
    }
}

/// Decide which shards are kept vs freed, and what additional shards are needed.
/// Returns (new_cmt_with_kept, freed_shards, remaining_needs).
fn compute_allocations(
    mut new_assignments: BTreeMap<NodeId, u16>,
    prev: &BTreeMap<NodeId, Vec<u16>>,
) -> (BTreeMap<NodeId, Vec<u16>>, Vec<u16>, BTreeMap<NodeId, u16>) {
    let mut new_cmt: BTreeMap<NodeId, Vec<u16>> = BTreeMap::new();
    let mut to_move: Vec<u16> = Vec::new();

    for (&node_id, prev_shards) in prev.iter() {
        let assigned_len_opt = new_assignments.remove(&node_id);
        match assigned_len_opt {
            None => {
                to_move.extend_from_slice(prev_shards);
            }
            Some(assigned_len) if assigned_len == 0 => {
                to_move.extend_from_slice(prev_shards);
            }
            Some(assigned_len) => {
                let curr_len = prev_shards.len() as u16;
                if curr_len == assigned_len {
                    new_cmt.insert(node_id, prev_shards.clone());
                } else if curr_len > assigned_len {
                    let keep = assigned_len as usize;
                    let mut node_shards = prev_shards.clone();
                    let to_free = node_shards.split_off(keep);
                    to_move.extend(to_free);
                    new_cmt.insert(node_id, node_shards);
                } else {
                    new_cmt.insert(node_id, prev_shards.clone());
                    let need_more = assigned_len - curr_len;
                    new_assignments.insert(node_id, need_more);
                }
            }
        }
    }

    (new_cmt, to_move, new_assignments)
}

/// Fill remaining needs by consuming from `to_move` (LIFO to preserve existing order semantics).
fn fill_deficits(
    new_cmt: &mut BTreeMap<NodeId, Vec<u16>>,
    to_move: &mut Vec<u16>,
    needs: &BTreeMap<NodeId, u16>,
) {
    for (&node_id, &needed) in needs.iter() {
        let need = needed as usize;
        if need == 0 {
            continue;
        }
        let mut curr = new_cmt.remove(&node_id).unwrap_or_default();
        for _ in 0..need {
            let shard = to_move.pop().expect("Not enough freed shards to reassign");
            curr.push(shard);
        }
        new_cmt.insert(node_id, curr);
    }
}

/// Build dense vectors (ids, counts, offsets, shards) from a map of counts.
fn pack_shards(assigned: impl IntoIterator<Item = (NodeId, u16)>) -> (Vec<NodeId>, Vec<u16>, Vec<u16>, Vec<u16>) {
    let mut ids = Vec::new();
    let mut counts = Vec::new();
    let mut offsets = Vec::new();
    let mut shards = Vec::new();

    let mut next_shard: u16 = 0;
    let mut offset: u16 = 0;

    for (node_id, count) in assigned {
        ids.push(node_id);
        counts.push(count);
        offsets.push(offset);

        for _ in 0..count {
            shards.push(next_shard);
            next_shard = next_shard.wrapping_add(1);
            offset = offset.wrapping_add(1);
        }
    }
    (ids, counts, offsets, shards)
}

#[derive(Clone, Debug)]
struct Quotient {
    numer: u128,
    denom: u128,
}

impl Quotient {
    fn from_quot(numer: u128, denom: u128) -> Self {
        assert!(denom > 0, "Denominator must be > 0");
        Self { numer, denom }
    }
}

#[derive(Clone, Debug)]
struct Entry {
    quotient: Quotient,
    tie_breaker: u64,
    index: usize,
}

impl PartialEq for Entry {
    fn eq(&self, other: &Self) -> bool {
        self.quotient.numer * other.quotient.denom
            == other.quotient.numer * self.quotient.denom
            && self.tie_breaker == other.tie_breaker
            && self.index == other.index
    }
}

impl Eq for Entry {}

impl PartialOrd for Entry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}


fn compare_quotients(a: &Quotient, b: &Quotient) -> Ordering {
    let left = a.numer.saturating_mul(b.denom);
    let right = b.numer.saturating_mul(a.denom);
    left.cmp(&right)
}

fn tie_break(t1: u64, i1: usize, t2: u64, i2: usize) -> Ordering {
    match t1.cmp(&t2) {
        Ordering::Greater => Ordering::Greater,
        Ordering::Less => Ordering::Less,
        Ordering::Equal => i2.cmp(&i1) 
    }
}

impl Ord for Entry {
    fn cmp(&self, other: &Self) -> Ordering {
        match compare_quotients(&self.quotient, &other.quotient) {
            Ordering::Equal => tie_break(
                self.tie_breaker, self.index, other.tie_breaker, other.index),
            ord => ord,
        }
    }
}


fn max_shards_per_node(node_count: u64, shard_count: u64) -> u64 {
    const MIN_NODES: u64 = 20;
    const MAX_PER_NODE_SHARE: u64 = 10; // 10%

    if shard_count == 0 || node_count == 0 {
        return 0;
    }

    if node_count >= MIN_NODES {
        shard_count / MAX_PER_NODE_SHARE
    } else {
        // Scale linearly between 1 and MIN_NODES
        let num = shard_count.saturating_mul(MIN_NODES);
        let den = node_count.saturating_mul(MAX_PER_NODE_SHARE);
        num.saturating_add(den - 1) / den
    }
}

fn allocate_shards_dhondt(node_priorities: &[u64], shard_count: u16, stake: &[u64]) -> Vec<u16> {
    let node_count = stake.len();
    if node_count == 0 {
        return Vec::new();
    }

    let total_stake: u128 = stake
        .iter()
        .map(|&x| x as u128)
        .sum();

    assert!(total_stake > 0, "Total stake must be > 0");

    let n_shards_u64 = shard_count as u64;
    let max_shards = max_shards_per_node(node_count as u64, n_shards_u64);
    let dist_number = (total_stake as u128 / (n_shards_u64 as u128 + 1)) + 1;

    let mut shards: Vec<u64> = stake
        .iter()
        .map(|&s| {
            let base = (s as u128) / dist_number;
            let v = base as u64;
            v.min(max_shards)
        })
        .collect();

    let mut heap = BinaryHeap::new();
    for (i, &s) in stake.iter().enumerate() {
        if shards[i] != max_shards {
            let denom = shards[i] + 1;
            let quotient = Quotient::from_quot(s as u128, denom as u128);
            heap.push(Entry {
                quotient,
                tie_breaker: node_priorities[i],
                index: i,
            });
        }
    }

    let mut distributed: u64 = shards.iter().sum();
    while distributed < n_shards_u64 {
        let Entry {
            quotient: _,
            tie_breaker,
            index,
        } = heap.pop().expect("Heap empty while distributing shards");

        shards[index] += 1;
        distributed += 1;
        if shards[index] != max_shards {
            let denom = shards[index] + 1;
            let q = Quotient::from_quot(stake[index] as u128, denom as u128);
            heap.push(Entry {
                quotient: q,
                tie_breaker,
                index,
            });
        }
    }

    shards
        .into_iter()
        .map(|x| x as u16)
        .collect()
}

impl<const N: usize> StakeLeaderSet<N> {
    pub fn size_bytes() -> usize {
        std::mem::size_of::<Self>()
    }

    pub fn new(mut items: Vec<(NodeId, u64)>) -> Self {
        items.sort_by_key(|(id, _)| *id);
        assert!(items.len() <= N, "Too many items for N");

        let mut out: Self = Zeroable::zeroed();
        out.len = items.len() as u16;
        for (i, (id, st)) in items.into_iter().enumerate() {
            out.node_ids[i] = id;
            out.stakes[i] = st;
        }
        out
    }
}

pub fn stake_weighted_shard_counts<const N: usize>(
    leaders: &StakeLeaderSet<N>,
    shard_count: u16,
) -> BTreeMap<NodeId, u16> {
    let node_count = leaders.len as usize;
    if node_count == 0 || shard_count == 0 {
        return BTreeMap::new();
    }

    let node_ids: Vec<NodeId> = leaders.node_ids[0..node_count].to_vec();
    let stakes: Vec<u64> = leaders.stakes[0..node_count].to_vec();

    let node_priorities: Vec<u64> = (0..node_count)
        .map(|i| (node_count - i) as u64)
        .collect();

    let shards_vec = allocate_shards_dhondt(&node_priorities, shard_count, &stakes);
    let mut distribution = BTreeMap::new();
    for (i, &sh) in shards_vec.iter().enumerate() {
        if sh > 0 {
            distribution.insert(node_ids[i], sh);
        }
    }

    distribution
}


#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    const MAX_NODES: usize = 256;
    const NUM_SHARDS: usize = 1000;

    type TestStakeLeaderSet = StakeLeaderSet<{MAX_NODES}>;
    type TestCommittee = Committee<{MAX_NODES}, {NUM_SHARDS}>;

    #[test]
    fn test_max_shards_per_node_threshold() {
       assert_eq!(max_shards_per_node(20, 1000), 100);
       assert_eq!(max_shards_per_node(25, 1000), 100);
       assert_eq!(max_shards_per_node(5, 1000), 400);
    }

    #[test]
    fn test_dhondt_basic_even() {
       let stake = vec![25_000_u64, 25_000, 25_000, 25_000];
       let priorities: Vec<u64> = (0..4).map(|i| 4 - i).collect();
       assert_eq!(allocate_shards_dhondt(&priorities, 4, &stake), vec![1, 1, 1, 1]);

       let res = allocate_shards_dhondt(&priorities, 1000, &stake);
       assert_eq!(res.iter().map(|&x| x as u64).sum::<u64>(), 1000);
       assert_eq!(res, vec![250, 250, 250, 250]);
    }

    #[test]
    fn test_dhondt_basic_uneven() {
       let stake = vec![50_000_u64, 30_000, 15_000, 5_000];
       let priorities: Vec<u64> = (0..4).map(|i| 4 - i).collect();
       assert_eq!(allocate_shards_dhondt(&priorities, 4, &stake), vec![2, 2, 0, 0]);

       let res = allocate_shards_dhondt(&priorities, 1000, &stake);
       assert_eq!(res.iter().map(|&x| x as u64).sum::<u64>(), 1000);
       assert_eq!(res, vec![500, 300, 150, 50]);
    }

    #[test]
    fn test_stake_weighted_equal_three_nodes() {
       // Expect a tie-broken split 4,3,3 for 10 shards
       let leaders = TestStakeLeaderSet::new(vec![(NodeId(1), 1000), (NodeId(2), 1000), (NodeId(3), 1000)]);
       let res = stake_weighted_shard_counts(&leaders, 10);
       assert_eq!(res.values().copied().sum::<u16>(), 10);
       let v: Vec<u16> = [NodeId(1), NodeId(2), NodeId(3)]
           .iter()
           .map(|nid| *res.get(nid).unwrap_or(&0))
           .collect();
       assert_eq!(v, vec![4, 3, 3]);
    }

    #[test]
    fn test_compute_single_node() {
       let leaders = TestStakeLeaderSet::new(vec![(NodeId(10), 1_000_000)]);

       let c = TestCommittee::from(None, &leaders, 10);
       assert_eq!(c.size(), 1);
       assert_eq!(c.total_shards(), 10);
       assert_eq!(c.shards(&NodeId(10)).unwrap().len(), 10);
    }

    #[test]
    fn test_compute_even_distribution() {
       let leaders = TestStakeLeaderSet::new(vec![(NodeId(1), 1000), (NodeId(2), 1000), (NodeId(3), 1000)]);

       let c = TestCommittee::from(None, &leaders, 6);
       assert_eq!(c.size(), 3);
       assert_eq!(c.shards(&NodeId(1)).unwrap().len(), 2);
       assert_eq!(c.shards(&NodeId(2)).unwrap().len(), 2);
       assert_eq!(c.shards(&NodeId(3)).unwrap().len(), 2);
    }

    #[test]
    fn test_compute_uneven_distribution() {
       let leaders =
           TestStakeLeaderSet::new(vec![(NodeId(1), 4000), (NodeId(2), 2000), (NodeId(3), 1000)]);

       let c = TestCommittee::from(None, &leaders, 10);
       assert_eq!(c.size(), 3);
       assert_eq!(c.shards(&NodeId(1)).unwrap().len(), 6);
       assert_eq!(c.shards(&NodeId(2)).unwrap().len(), 3);
       assert_eq!(c.shards(&NodeId(3)).unwrap().len(), 1);
    }

    #[test]
    fn test_committee_initialize_and_transition_preserve() {
       // Initialize with reversed order: 3..0, each 2 shards for total 8
       let init_map: BTreeMap<NodeId, u16> = [
           (NodeId(3), 2),
           (NodeId(2), 2),
           (NodeId(1), 2),
           (NodeId(0), 2),
       ].into();

       let c1 = TestCommittee::new(init_map);

       assert_eq!(c1.size(), 4);
       assert_eq!(c1.shards(&NodeId(0)).unwrap(), &[0u16, 1]); // << to be removed
       assert_eq!(c1.shards(&NodeId(1)).unwrap(), &[2u16, 3]); // << to be removed
       assert_eq!(c1.shards(&NodeId(2)).unwrap(), &[4u16, 5]);
       assert_eq!(c1.shards(&NodeId(3)).unwrap(), &[6u16, 7]);

       // Remove two last nodes: new size 2, assign 4 shards each
       let t_map: BTreeMap<NodeId, u16> = [
           (NodeId(3), 4),
           (NodeId(2), 4),
       ].into();
       let c2 = c1.apply_assignments(t_map);
       assert_eq!(c2.size(), 2);

       let s3 = c2.shards(&NodeId(3)).unwrap();
       assert!(s3.contains(&6) && s3.contains(&7)); // kept initial shards
       assert!(s3.contains(&0) && s3.contains(&1)); // acquired freed shards

       let s2 = c2.shards(&NodeId(2)).unwrap();
       assert!(s2.contains(&4) && s2.contains(&5));
       assert!(s2.contains(&2) && s2.contains(&3));
    }

    #[test]
    fn test_committee_default_scenario_transition_chain() {
       let n1 = NodeId(1);
       let n2 = NodeId(2);
       let n3 = NodeId(3);
       let n4 = NodeId(4);
       let n5 = NodeId(5);

       let c1 = TestCommittee::new([
           (n1, 2),
           (n2, 2),
           (n3, 2),
           (n4, 2),
           (n5, 2),
       ].into());

       assert_eq!(c1.size(), 5);
       assert_eq!(c1.shards(&n1).unwrap(), &[0u16, 1]);
       assert_eq!(c1.shards(&n2).unwrap(), &[2u16, 3]);
       assert_eq!(c1.shards(&n3).unwrap(), &[4u16, 5]);
       assert_eq!(c1.shards(&n4).unwrap(), &[6u16, 7]);
       assert_eq!(c1.shards(&n5).unwrap(), &[8u16, 9]);

       // Reassign to 3 nodes, each 2 shards, expect to keep prior shards
       let c2 = c1.apply_assignments([(n2, 3), (n3, 3), (n1, 4)].into());
       assert_eq!(c2.size(), 3);
       let s1 = c2.shards(&n1).unwrap();
       let s2 = c2.shards(&n2).unwrap();
       let s3 = c2.shards(&n3).unwrap();
       assert!(s1.contains(&0) && s1.contains(&1));
       assert!(s2.contains(&2) && s2.contains(&3));
       assert!(s3.contains(&4) && s3.contains(&5));

       // Each should have acquired one freed shard
       let c3 = c2.apply_assignments([(n2, 3), (n3, 3), (n4, 2), (n5, 2)].into());
       assert_eq!(c3.size(), 4);
       assert_eq!(c3.shards(&n2).unwrap(), s2);
       assert_eq!(c3.shards(&n3).unwrap(), s3);
       assert_eq!(c3.shards(&n4).unwrap().len(), 2);
       assert_eq!(c3.shards(&n5).unwrap().len(), 2);

       // Finally, assign all 10 shards to n1
       let c4 = c3.apply_assignments([(n1, 10)].into());
       assert_eq!(c4.size(), 1);
       let s = c4.shards(&n1).unwrap();
       for i in 0..10 {
           assert!(s.contains(&(i as u16)));
       }
    }

    fn print_table_header() {
        println!(
            "{:<8} | {:>12} | {:>6} | {}",
            "NodeId", "Stake", "Shards", "ShardIds"
        );
        println!("{}", "-".repeat(8 + 3 + 12 + 3 + 6 + 3 + 40));
    }

    fn print_committee_table<const N: usize, const S: usize>(
        leaders: &StakeLeaderSet<N>,
        committee: &Committee<N, S>,
    ) {
        print_table_header();
        for i in 0..leaders.len as usize {
            let node = leaders.node_ids[i];
            let stake = leaders.stakes[i];
            let shards = committee.shards(&node).unwrap_or(&[]);
            println!(
                "{:<8} | {:>12} | {:>6} | {:?}",
                node.0, stake, shards.len(), shards
            );
        }
        println!();
        println!(
            "Total nodes: {} | Total shards: {}",
            committee.size(),
            committee.total_shards()
        );
    }

    #[test]
    fn test_many_nodes() {

        let gen_items = || {
            (0..MAX_NODES as u64)
                .map(|i| (NodeId(MAX_NODES as u64 - i), 1000 + (i as u64) * 100))
                .collect::<Vec<(NodeId, u64)>>()
        };

        let items = gen_items();

        let leaders = TestStakeLeaderSet::new(items);
        let committee = TestCommittee::from(None, &leaders, 1000);

        print_committee_table(&leaders, &committee);

        println!("StakeLeaderSet size: {} bytes", TestStakeLeaderSet::size_bytes());
        println!("Committee size: {} bytes", TestCommittee::size_bytes());
    }
}
