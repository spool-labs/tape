use std::cmp::Ordering;
use std::collections::{BTreeMap, BinaryHeap};
use std::fmt;

use bytemuck::{Pod, Zeroable};

// =========================
// Basic Types
// =========================

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug, Hash)]
pub struct NodeId(pub u64);

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let NodeId(id) = self;
        write!(f, "{id}")
    }
}

// =========================
// New Structures
// =========================

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct ActiveSet<const N: usize> {
    pub len: u16,              // number of valid entries (<= N and <= u16::MAX)
    pub node_ids: [NodeId; N], // sorted by NodeId (ascending)
    pub stakes: [u64; N],      // stakes[i] belongs to node_ids[i]
}

unsafe impl<const N: usize> Zeroable for ActiveSet<N> {}
unsafe impl<const N: usize> Pod for ActiveSet<N> {}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct Committee<const N: usize, const S: usize> {
    pub len: u16,                       // number of nodes in committee
    pub node_ids: [NodeId; N],          // sorted by NodeId
    pub shard_counts: [u16; N],         // shard count per node
    pub shard_offsets: [u16; N],        // offset into `shards` for node i (0 for count = 0)
    pub shards: [u16; S],               // packed shard ids [0..total_shards-1]
}

unsafe impl<const N: usize, const S: usize> Zeroable for Committee<N,S> {}
unsafe impl<const N: usize, const S: usize> Pod for Committee<N,S> {}

// =========================
// Committee Methods
// =========================

impl<const N: usize, const S: usize> Committee<N, S> {
    pub const fn size_bytes() -> usize {
        std::mem::size_of::<Self>()
    }

    // Create an empty committee
    #[inline(never)]
    pub fn empty() -> Self {
        // Use Zeroable to avoid large temporaries
        let mut out: Self = Zeroable::zeroed();
        out.len = 0;
        out
    }

    // Initialize committee with sequential shard assignment from 0..N-1,
    // with assigned_number being a map of node -> number_of_shards (u16).
    #[inline(never)]
    pub fn initialize(assigned_number: BTreeMap<NodeId, u16>) -> Self {
        let total_shards: usize = assigned_number.values().map(|&s| s as usize).sum();
        assert!(assigned_number.len() <= N, "Too many nodes for N");
        //assert!(total_shards <= S, "Too many shards for S");

        // Build directly into the output object to avoid big locals on the stack.
        let mut out = Self::empty();

        let mut shard_idx: u16 = 0;
        let mut i = 0usize;
        let mut offset: u16 = 0;
        for (&node_id, &count) in assigned_number.iter() {
            out.node_ids[i] = node_id;
            out.shard_counts[i] = count;
            out.shard_offsets[i] = offset;
            // Fill this node's shard slice
            for _ in 0..count {
                out.shards[offset as usize] = shard_idx;
                shard_idx = shard_idx.wrapping_add(1);
                offset = offset.wrapping_add(1);
            }
            i += 1;
        }
        out.len = assigned_number.len() as u16;
        out
    }

    // Transition committee with minimal movement of shards:
    // Keep existing shards where possible, free those for removed or reduced assignments,
    // and assign freed shards to nodes that need more or newly added nodes.
    // NOTE: still uses BTreeMap/Vec internally (heap) for minimal code disruption,
    // but avoids large stack arrays by packing directly into the returned struct.
    pub fn transition(&self, mut new_assignments: BTreeMap<NodeId, u16>) -> Self {
        let mut new_cmt: BTreeMap<NodeId, Vec<u16>> = BTreeMap::new();
        let mut to_move: Vec<u16> = Vec::new();

        // Total shards in new committee:
        let new_total_shards: u64 = new_assignments.values().map(|&s| s as u64).sum();

        // Current shards count:
        let mut current_total_shards: u64 = 0;

        // First pass: examine nodes present in old committee
        for j in 0..self.len as usize {
            let node_id = self.node_ids[j];
            let off = self.shard_offsets[j] as usize;
            let cnt = self.shard_counts[j] as usize;
            let prev_shards_slice = &self.shards[off..off + cnt];
            current_total_shards += cnt as u64;

            // Determine assigned length in new assignments (if present)
            let assigned_len_opt = new_assignments.remove(&node_id);
            match assigned_len_opt {
                None => {
                    // Node removed or assigned 0: free all its shards
                    to_move.extend_from_slice(prev_shards_slice);
                }
                Some(assigned_len) if assigned_len == 0 => {
                    // Node has zero shards now: free all its shards
                    to_move.extend_from_slice(prev_shards_slice);
                }
                Some(assigned_len) => {
                    let curr_len = cnt as u16;
                    if curr_len == assigned_len {
                        // No change: copy shards over
                        new_cmt.insert(node_id, prev_shards_slice.to_vec());
                    } else if curr_len > assigned_len {
                        // Reduce: remove extra shards from end
                        let keep = assigned_len as usize;
                        let mut node_shards = prev_shards_slice.to_vec();
                        // Remove from the end to free
                        let to_free = node_shards.split_off(keep);
                        to_move.extend(to_free);
                        new_cmt.insert(node_id, node_shards);
                    } else {
                        // curr_len < assigned_len: need more shards later
                        // Keep current shards and record that this node needs more
                        new_cmt.insert(node_id, prev_shards_slice.to_vec());
                        // Re-insert with remaining needed count (assigned_len - curr_len)
                        let need_more = assigned_len - curr_len;
                        new_assignments.insert(node_id, need_more);
                    }
                }
            }
        }

        // Check shard count consistency
        assert_eq!(
            new_total_shards, current_total_shards,
            "EInvalidShardAssignment: total shards differ in transition"
        );

        // Now new_assignments contains only nodes that still need shards > 0,
        // including truly new nodes and nodes that had a deficit.

        // Assign shards from to_move to these nodes
        for (&node_id, &needed) in new_assignments.iter() {
            let need = needed as usize;
            if need == 0 {
                continue;
            }
            // Gather current shards if any
            let mut curr = new_cmt.remove(&node_id).unwrap_or_default();
            // Pull from to_move
            for _ in 0..need {
                let shard = to_move.pop().expect("Not enough freed shards to reassign");
                curr.push(shard);
            }
            new_cmt.insert(node_id, curr);
        }

        // Pack directly into output to avoid large locals.
        let mut out = Self::empty();

        let total_shards: usize = new_cmt.values().map(|v| v.len()).sum();
        assert!(new_cmt.len() <= N, "Too many nodes for N");
        assert!(total_shards <= S, "Too many shards for S");

        let mut len: u16 = 0;
        let mut offset: u16 = 0;
        for (&nid, shrds) in new_cmt.iter() {
            if !shrds.is_empty() {
                let i = len as usize;
                out.node_ids[i] = nid;
                out.shard_counts[i] = shrds.len() as u16;
                out.shard_offsets[i] = offset;

                // copy shard ids
                let base = offset as usize;
                for (k, &sh) in shrds.iter().enumerate() {
                    out.shards[base + k] = sh;
                }

                offset = offset.wrapping_add(shrds.len() as u16);
                len = len.wrapping_add(1);
            }
        }
        out.len = len;
        out
    }

    pub fn contains(&self, node_id: &NodeId) -> bool {
        self.node_ids[0..self.len as usize].binary_search(node_id).is_ok()
    }

    pub fn shards(&self, node_id: &NodeId) -> Option<&[u16]> {
        match self.node_ids[0..self.len as usize].binary_search(node_id) {
            Ok(idx) => {
                let off = self.shard_offsets[idx] as usize;
                let cnt = self.shard_counts[idx] as usize;
                Some(&self.shards[off..off + cnt])
            }
            Err(_) => None,
        }
    }

    pub fn size(&self) -> usize {
        self.len as usize
    }

    pub fn total_shards(&self) -> usize {
        (0..self.len as usize)
            .map(|i| self.shard_counts[i] as usize)
            .sum()
    }

    // Utility to compute difference between two committees: nodes in left not in right and vice versa.
    pub fn diff(&self, other: &Self) -> (Vec<NodeId>, Vec<NodeId>) {
        let mut only_in_self = Vec::new();
        let mut only_in_other = Vec::new();
        let mut i = 0;
        let mut j = 0;
        while i < self.len as usize && j < other.len as usize {
            let a = self.node_ids[i];
            let b = other.node_ids[j];
            if a == b {
                i += 1;
                j += 1;
            } else if a < b {
                only_in_self.push(a);
                i += 1;
            } else {
                only_in_other.push(b);
                j += 1;
            }
        }
        while i < self.len as usize {
            only_in_self.push(self.node_ids[i]);
            i += 1;
        }
        while j < other.len as usize {
            only_in_other.push(other.node_ids[j]);
            j += 1;
        }
        (only_in_self, only_in_other)
    }
}

// =========================
// Apportionment Queue with Quotients
// =========================

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

// Entry in the priority queue: max-heap by quotient, tie-breaker descending, then index ascending
#[derive(Clone, Debug)]
struct Entry {
    quotient: Quotient,
    tie_breaker: u64,
    index: usize,
}

impl PartialEq for Entry {
    fn eq(&self, other: &Self) -> bool {
        self.quotient.numer * other.quotient.denom == other.quotient.numer * self.quotient.denom
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

impl Ord for Entry {
    fn cmp(&self, other: &Self) -> Ordering {
        // Compare quotients: a/b vs c/d => compare a*d ? c*b
        let left = self.quotient.numer.saturating_mul(other.quotient.denom);
        let right = other.quotient.numer.saturating_mul(self.quotient.denom);

        match left.cmp(&right) {
            Ordering::Greater => Ordering::Greater,
            Ordering::Less => Ordering::Less,
            Ordering::Equal => {
                // Tie-breaker: higher tie_breaker first
                match self.tie_breaker.cmp(&other.tie_breaker) {
                    Ordering::Greater => Ordering::Greater,
                    Ordering::Less => Ordering::Less,
                    Ordering::Equal => {
                        // As a final tie-breaker, prefer smaller index (stable)
                        other.index.cmp(&self.index) // reversed to keep BinaryHeap as max-heap
                    }
                }
            }
        }
    }
}

// =========================
// d'Hondt Apportionment
// =========================

// Constants mirroring Move implementation for shards limit logic.
const MIN_NODES_FOR_SHARDS_LIMIT: u64 = 20;
const SHARDS_LIMIT_DENOMINATOR: u64 = 10; // 10%

fn divide_and_round_up(a: u64, b: u64) -> u64 {
    if b == 0 {
        panic!("Division by zero")
    }
    if a == 0 {
        0
    } else {
        (a + b - 1) / b
    }
}

fn max_shards_per_node(n_nodes: u64, n_shards: u64) -> u64 {
    if n_nodes >= MIN_NODES_FOR_SHARDS_LIMIT {
        n_shards / SHARDS_LIMIT_DENOMINATOR
    } else {
        // ceil((n_shards * MIN_NODES_FOR_SHARDS_LIMIT) / (n_nodes * SHARDS_LIMIT_DENOMINATOR))
        let num = n_shards
            .saturating_mul(MIN_NODES_FOR_SHARDS_LIMIT);
        let den = n_nodes.saturating_mul(SHARDS_LIMIT_DENOMINATOR);
        divide_and_round_up(num, den)
    }
}

// Implementation of the D'Hondt method (aka Jefferson method).
// node_priorities: tie-breakers for nodes: higher means higher precedence
// n_shards: total shards to distribute
// stake: stake per node
#[inline(never)]
fn dhondt(node_priorities: &[u64], n_shards: u16, stake: &[u64]) -> Vec<u16> {
    let n_nodes = stake.len();
    if n_nodes == 0 {
        return Vec::new();
    }
    let total_stake: u128 = stake.iter().map(|&x| x as u128).sum();
    assert!(total_stake > 0, "ENoStake: total stake is zero");

    let n_shards_u64 = n_shards as u64;
    let max_shards = max_shards_per_node(n_nodes as u64, n_shards_u64);

    // Hagenbach-Bischoff initial assignment
    // distribution number = total_stake/(n_shards + 1) + 1
    let dist_number = (total_stake as u128 / (2 * n_shards_u64 as u128 + 1)) + 1;
    let mut shards: Vec<u64> = stake
        .iter()
        .map(|&s| {
            let base = (s as u128) / dist_number;
            let v = base as u64;
            v.min(max_shards)
        })
        .collect();

    // Priority queue of quotients
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
    // Distribute remaining shards
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

    shards.into_iter().map(|x| x as u16).collect()
}

// =========================
// Apportionment pipeline
// =========================

impl<const N: usize> ActiveSet<N> {
    pub fn size_bytes() -> usize {
        std::mem::size_of::<Self>()
    }

    pub fn new(mut items: Vec<(NodeId, u64)>) -> Self {
        items.sort_by_key(|(id, _)| *id);
        assert!(items.len() <= N, "Too many items for N");

        // Build in-place to avoid large local arrays on the stack.
        let mut out: Self =  Zeroable::zeroed() ;
        out.len = items.len() as u16;
        for (i, (id, st)) in items.into_iter().enumerate() {
            out.node_ids[i] = id;
            out.stakes[i] = st;
        }
        out
    }
}

// Perform apportionment given an active set and number of shards.
// Returns sorted mapping of NodeId -> shards allocated (>0 only).
#[inline(never)]
pub fn apportionment<const N: usize>(active_set: &ActiveSet<N>, n_shards: u16) -> BTreeMap<NodeId, u16> {
    let n_nodes = active_set.len as usize;
    if n_nodes == 0 || n_shards == 0 {
        return BTreeMap::new();
    }

    // These are heap-backed Vecs; fine for a 32 KB heap budget and keep stack tiny.
    let node_ids: Vec<NodeId> = active_set.node_ids[0..n_nodes].to_vec();
    let stakes: Vec<u64> = active_set.stakes[0..n_nodes].to_vec();

    // priorities: n_nodes - i (1-based preference), so earlier node (lower index) gets higher tie-breaker.
    let node_priorities: Vec<u64> = (0..n_nodes)
        .map(|i| (n_nodes - i) as u64)
        .collect();

    let shards_vec = dhondt(&node_priorities, n_shards, &stakes);
    let mut distribution = BTreeMap::new();
    for (i, &sh) in shards_vec.iter().enumerate() {
        if sh > 0 {
            distribution.insert(node_ids[i], sh);
        }
    }
    distribution
}

// Compute the next committee given the previous committee (if any), number of shards, and active set.
#[inline(never)]
pub fn compute_next_committee<const N: usize, const S: usize>(
    prev_committee: Option<&Committee<N, S>>,
    n_shards: u16,
    active_set: &ActiveSet<N>,
) -> Committee<N, S> {
    let dist = apportionment(active_set, n_shards);
    if let Some(prev) = prev_committee {
        if prev.size() == 0 {
            Committee::initialize(dist)
        } else {
            prev.transition(dist)
        }
    } else {
        Committee::initialize(dist)
    }
}

// =========================
// Tests
// =========================

#[cfg(test)]
mod tests {
    use super::*;

    //const TEST_N: usize = 10;
    //const TEST_S: usize = 1000;
    //
    //fn map_from_vec(v: Vec<(NodeId, u16)>) -> BTreeMap<NodeId, u16> {
    //    let mut m = BTreeMap::new();
    //    for (k, val) in v {
    //        m.insert(k, val);
    //    }
    //    m
    //}
    //
    //#[test]
    //fn test_max_shards_per_node_threshold() {
    //    // At least 20 nodes: max shards per node is total/10
    //    assert_eq!(max_shards_per_node(20, 1000), 100);
    //    assert_eq!(max_shards_per_node(25, 1000), 100);
    //    // Below threshold: scaled up limit
    //    // ceil((1000*20)/(5*10)) = ceil(20000/50)= ceil(400)=400
    //    assert_eq!(max_shards_per_node(5, 1000), 400);
    //}
    //
    //#[test]
    //fn test_dhondt_basic_even() {
    //    let stake = vec![25_000_u64, 25_000, 25_000, 25_000];
    //    let priorities: Vec<u64> = (0..4).map(|i| 4 - i).collect();
    //    assert_eq!(dhondt(&priorities, 4, &stake), vec![1, 1, 1, 1]);
    //
    //    let res = dhondt(&priorities, 1000, &stake);
    //    assert_eq!(res.iter().map(|&x| x as u64).sum::<u64>(), 1000);
    //    assert_eq!(res, vec![250, 250, 250, 250]);
    //}
    //
    //#[test]
    //fn test_dhondt_basic_uneven() {
    //    let stake = vec![50_000_u64, 30_000, 15_000, 5_000];
    //    let priorities: Vec<u64> = (0..4).map(|i| 4 - i).collect();
    //    assert_eq!(dhondt(&priorities, 4, &stake), vec![2, 2, 0, 0]);
    //
    //    let res = dhondt(&priorities, 1000, &stake);
    //    assert_eq!(res.iter().map(|&x| x as u64).sum::<u64>(), 1000);
    //    assert_eq!(res, vec![500, 300, 150, 50]);
    //}
    //
    //#[test]
    //fn test_apportionment_equal_three_nodes() {
    //    // Expect a tie-broken split 4,3,3 for 10 shards
    //    let active = ActiveSet::<TEST_N>::new(vec![(NodeId(1), 1000), (NodeId(2), 1000), (NodeId(3), 1000)]);
    //    let res = apportionment(&active, 10);
    //    assert_eq!(res.values().copied().sum::<u16>(), 10);
    //    let v: Vec<u16> = [NodeId(1), NodeId(2), NodeId(3)]
    //        .iter()
    //        .map(|nid| *res.get(nid).unwrap_or(&0))
    //        .collect();
    //    assert_eq!(v, vec![4, 3, 3]);
    //}
    //
    //#[test]
    //fn test_compute_single_node() {
    //    let active = ActiveSet::<TEST_N>::new(vec![(NodeId(10), 1_000_000)]);
    //    let c = compute_next_committee::<TEST_N, TEST_S>(None, 10, &active);
    //    assert_eq!(c.size(), 1);
    //    assert_eq!(c.total_shards(), 10);
    //    assert_eq!(c.shards(&NodeId(10)).unwrap().len(), 10);
    //}
    //
    //#[test]
    //fn test_compute_even_distribution() {
    //    let active = ActiveSet::<TEST_N>::new(vec![(NodeId(1), 1000), (NodeId(2), 1000), (NodeId(3), 1000)]);
    //    let c = compute_next_committee::<TEST_N, TEST_S>(None, 6, &active);
    //    assert_eq!(c.size(), 3);
    //    assert_eq!(c.shards(&NodeId(1)).unwrap().len(), 2);
    //    assert_eq!(c.shards(&NodeId(2)).unwrap().len(), 2);
    //    assert_eq!(c.shards(&NodeId(3)).unwrap().len(), 2);
    //}
    //
    //#[test]
    //fn test_compute_uneven_distribution() {
    //    let active =
    //        ActiveSet::<TEST_N>::new(vec![(NodeId(1), 4000), (NodeId(2), 2000), (NodeId(3), 1000)]);
    //    let c = compute_next_committee::<TEST_N, TEST_S>(None, 10, &active);
    //    assert_eq!(c.size(), 3);
    //    assert_eq!(c.shards(&NodeId(1)).unwrap().len(), 6);
    //    assert_eq!(c.shards(&NodeId(2)).unwrap().len(), 3);
    //    assert_eq!(c.shards(&NodeId(3)).unwrap().len(), 1);
    //}
    //
    //#[test]
    //fn test_committee_initialize_and_transition_preserve() {
    //    // Initialize with reversed order: 3..0, each 2 shards for total 8
    //    let init_map = map_from_vec(vec![
    //        (NodeId(3), 2),
    //        (NodeId(2), 2),
    //        (NodeId(1), 2),
    //        (NodeId(0), 2),
    //    ]);
    //    let c1 = Committee::<TEST_N, TEST_S>::initialize(init_map);
    //
    //    assert_eq!(c1.size(), 4);
    //    assert_eq!(c1.shards(&NodeId(0)).unwrap(), &[0u16, 1]); // << to be removed
    //    assert_eq!(c1.shards(&NodeId(1)).unwrap(), &[2u16, 3]); // << to be removed
    //    assert_eq!(c1.shards(&NodeId(2)).unwrap(), &[4u16, 5]);
    //    assert_eq!(c1.shards(&NodeId(3)).unwrap(), &[6u16, 7]);
    //
    //    // Remove two last nodes: new size 2, assign 4 shards each
    //    let t_map = map_from_vec(vec![
    //        (NodeId(3), 4), 
    //        (NodeId(2), 4)
    //    ]);
    //    let c2 = c1.transition(t_map);
    //    assert_eq!(c2.size(), 2);
    //
    //    let s3 = c2.shards(&NodeId(3)).unwrap();
    //    assert!(s3.contains(&6) && s3.contains(&7)); // kept initial shards
    //    assert!(s3.contains(&0) && s3.contains(&1)); // acquired freed shards
    //
    //    let s2 = c2.shards(&NodeId(2)).unwrap();
    //    assert!(s2.contains(&4) && s2.contains(&5));
    //    assert!(s2.contains(&2) && s2.contains(&3));
    //}
    //
    //#[test]
    //fn test_committee_default_scenario_transition_chain() {
    //    let n1 = NodeId(1);
    //    let n2 = NodeId(2);
    //    let n3 = NodeId(3);
    //    let n4 = NodeId(4);
    //    let n5 = NodeId(5);
    //
    //    let c1 = Committee::<TEST_N, TEST_S>::initialize(map_from_vec(vec![
    //        (n1, 2),
    //        (n2, 2),
    //        (n3, 2),
    //        (n4, 2),
    //        (n5, 2),
    //    ]));
    //    assert_eq!(c1.size(), 5);
    //    assert_eq!(c1.shards(&n1).unwrap(), &[0u16, 1]);
    //    assert_eq!(c1.shards(&n2).unwrap(), &[2u16, 3]);
    //    assert_eq!(c1.shards(&n3).unwrap(), &[4u16, 5]);
    //    assert_eq!(c1.shards(&n4).unwrap(), &[6u16, 7]);
    //    assert_eq!(c1.shards(&n5).unwrap(), &[8u16, 9]);
    //
    //    // Transition to 4/3/3 for n1,n2,n3
    //    let c2 = c1.transition(map_from_vec(vec![(n2, 3), (n3, 3), (n1, 4)]));
    //    assert_eq!(c2.size(), 3);
    //    let s1 = c2.shards(&n1).unwrap();
    //    let s2 = c2.shards(&n2).unwrap();
    //    let s3 = c2.shards(&n3).unwrap();
    //    assert!(s1.contains(&0) && s1.contains(&1));
    //    assert!(s2.contains(&2) && s2.contains(&3));
    //    assert!(s3.contains(&4) && s3.contains(&5));
    //
    //    // Transition to n2,n3,n4,n5: 3,3,2,2
    //    let c3 = c2.transition(map_from_vec(vec![(n2, 3), (n3, 3), (n4, 2), (n5, 2)]));
    //    assert_eq!(c3.size(), 4);
    //    assert_eq!(c3.shards(&n2).unwrap(), s2);
    //    assert_eq!(c3.shards(&n3).unwrap(), s3);
    //    assert_eq!(c3.shards(&n4).unwrap().len(), 2);
    //    assert_eq!(c3.shards(&n5).unwrap().len(), 2);
    //
    //    // Finally transition to only n1 owning all 10 shards
    //    let c4 = c3.transition(map_from_vec(vec![(n1, 10)]));
    //    assert_eq!(c4.size(), 1);
    //    let s = c4.shards(&n1).unwrap();
    //    for i in 0..10 {
    //        assert!(s.contains(&(i as u16)));
    //    }
    //}
    //
    fn print_table_header() {
        println!(
            "{:<8} | {:>12} | {:>6} | {}",
            "NodeId", "Stake", "Shards", "ShardIds"
        );
        println!("{}", "-".repeat(8 + 3 + 12 + 3 + 6 + 3 + 40));
    }

    fn print_committee_table<const N: usize, const S: usize>(active: &ActiveSet<N>, committee: &Committee<N, S>) {
        print_table_header();
        for i in 0..active.len as usize {
            let node = active.node_ids[i];
            let stake = active.stakes[i];
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
    //
    //#[test]
    //fn test_inspect_equal_three_nodes_table() {
    //    let active = ActiveSet::<TEST_N>::new(vec![
    //        (NodeId(1), 1000),
    //        (NodeId(2), 1000),
    //        (NodeId(3), 1000),
    //    ]);
    //    let committee = compute_next_committee::<TEST_N, TEST_S>(None, 10, &active);
    //    println!("\n=== Equal stake, 3 nodes, 10 shards ===");
    //    print_committee_table(&active, &committee);
    //}
    //
    //#[test]
    //fn test_inspect_uneven_six_nodes_table() {
    //    let active = ActiveSet::<TEST_N>::new(vec![
    //        (NodeId(10), 50_000),
    //        (NodeId(11), 40_000),
    //        (NodeId(12), 25_000),
    //        (NodeId(13), 25_000),
    //        (NodeId(14), 10_000),
    //        (NodeId(15), 5_000),
    //    ]);
    //    let committee = compute_next_committee::<TEST_N, TEST_S>(None, 100, &active);
    //    println!("\n=== Uneven stake, 6 nodes, 100 shards ===");
    //    print_committee_table(&active, &committee);
    //}
    //
    //#[test]
    //fn test_consistency() {
    //    const N: usize = 20;
    //    const S: usize = 256;
    //
    //    // Generate N nodes with descending stake, optionally excluding one node
    //    let gen_items = |exclude: Option<u64>| {
    //        (0..N as u64)
    //            .filter(|&i| exclude.map_or(true, |e| i != e))
    //            .map(|i| (NodeId(i), 1000 + (i as u64) * 100))
    //            .collect::<Vec<(NodeId, u64)>>()
    //    };
    //
    //    let items = gen_items(None);
    //    let active = ActiveSet::<N>::new(items);
    //    let committee = compute_next_committee::<N, S>(None, S as u16, &active);
    //    print_committee_table(&active, &committee);
    //
    //    // Remove one node
    //    let items = gen_items(Some(12));
    //    let active2 = ActiveSet::<N>::new(items);
    //    let committee2 = compute_next_committee::<N, S>(Some(&committee), S as u16, &active2);
    //    print_committee_table(&active2, &committee2);
    //
    //    // Add it back 
    //    let items = gen_items(None);
    //    let active3 = ActiveSet::<N>::new(items);
    //    let committee3 = compute_next_committee::<N, S>(Some(&committee2), S as u16, &active3);
    //    print_committee_table(&active3, &committee3);
    //
    //    let (only_in_1, only_in_2) = committee.diff(&committee2);
    //    assert_eq!(only_in_1, vec![NodeId(12)]);
    //    assert_eq!(only_in_2, vec![]);
    //    let (only_in_2b, only_in_3) = committee2.diff(&committee3);
    //    assert_eq!(only_in_2b, vec![]);
    //    assert_eq!(only_in_3, vec![NodeId(12)]);
    //    let (only_in_1b, only_in_3b) = committee.diff(&committee3);
    //    assert_eq!(only_in_1b, vec![]);
    //    assert_eq!(only_in_3b, vec![]);
    //
    //    // Assert that the first and last committees are identical
    //    assert_eq!(committee.size(), committee3.size());
    //    assert_eq!(committee.total_shards(), committee3.total_shards());
    //    for i in 0..committee.size() {
    //        let nid1 = committee.node_ids[i];
    //        let nid3 = committee3.node_ids[i];
    //        assert_eq!(nid1, nid3, "NodeId mismatch at index {i}");
    //        let shards1 = committee.shards(&nid1).unwrap();
    //        let shards3 = committee3.shards(&nid3).unwrap();
    //        assert_eq!(shards1, shards3, "Shard list mismatch for NodeId {}", nid1);
    //    }
    //
    //    println!("ActiveSet size: {} bytes", ActiveSet::<N>::size_bytes());
    //    println!("Committee size: {} bytes", Committee::<N, S>::size_bytes());
    //}

    #[test]
    fn test_many_nodes() {
        const N: usize = 256;
        const S: usize = 1024;

        // Generate N nodes with descending stake, optionally excluding one node
        let gen_items = || {
            (0..N as u64)
                .map(|i| (NodeId(N as u64 - i), 1000 + (i as u64) * 100))
                .collect::<Vec<(NodeId, u64)>>()
        };

        let items = gen_items();
        let active = ActiveSet::<N>::new(items);
        let committee = compute_next_committee::<N, S>(None, S as u16, &active);
        print_committee_table(&active, &committee);

        println!("ActiveSet size: {} bytes", ActiveSet::<N>::size_bytes());
        println!("Committee size: {} bytes", Committee::<N, S>::size_bytes());
    }
}
