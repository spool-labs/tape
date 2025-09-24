use std::cmp::Ordering;
use std::collections::{BTreeMap, BinaryHeap};
use crate::types::NodeId;

#[derive(Clone, Debug, Default)]
pub struct Committee {
    // Sorted by NodeId
    shards_by_node: BTreeMap<NodeId, Vec<u16>>,
}

impl Committee {
    // Create an empty committee
    pub fn empty() -> Self {
        Self {
            shards_by_node: BTreeMap::new(),
        }
    }

    // new committee with sequential shard assignment from 0..N-1,
    // with assigned_number being a map of node -> number_of_shards (u16).
    pub fn new(assigned_number: BTreeMap<NodeId, u16>) -> Committee {
        let mut shard_idx: u16 = 0;
        let mut map = BTreeMap::new();
        for (node_id, count) in assigned_number.iter() {
            let cnt = *count as usize;
            let mut v = Vec::with_capacity(cnt);
            for _ in 0..cnt {
                v.push(shard_idx);
                shard_idx = shard_idx + 1;
            }
            map.insert(*node_id, v);
        }
        Committee { shards_by_node: map }
    }

    pub fn from(
        active_set: &ActiveSet,
        shard_count: u16,
    ) -> Self {
        let dist = stake_weighted_shard_counts(active_set, shard_count);
        Self::new(dist)
    }

    pub fn from_previous(
        previous: &Committee,
        active_set: &ActiveSet,
        shard_count: u16,
    ) -> Committee {
        let dist = stake_weighted_shard_counts(active_set, shard_count);
        if previous.size() == 0 {
            Committee::new(dist)
        } else {
            previous.transition(dist)
        }
    }

    // Transition committee with minimal movement of shards:
    // Keep existing shards where possible, free those for removed or reduced assignments,
    // and assign freed shards to nodes that need more or newly added nodes.
    pub fn transition(&self, new_assignments: BTreeMap<NodeId, u16>) -> Committee {
        let mut new_assignments = new_assignments; // mutable local copy
        let mut shards_by_node: BTreeMap<NodeId, Vec<u16>> = BTreeMap::new();
        let mut to_move: Vec<u16> = Vec::new();

        // Total shards in new committee:
        let new_total_shards: u64 = new_assignments.values().map(|&s| s as u64).sum();

        // Current shards count:
        let mut current_total_shards: u64 = 0;

        // First pass: examine nodes present in old committee
        for (node_id, prev_shards) in self.shards_by_node.iter() {
            current_total_shards += prev_shards.len() as u64;

            // Determine assigned length in new assignments (if present)
            let assigned_len_opt = new_assignments.remove(node_id);
            match assigned_len_opt {
                None => {
                    // Node removed or assigned 0: free all its shards
                    to_move.extend(prev_shards.iter().copied());
                }
                Some(assigned_len) if assigned_len == 0 => {
                    // Node has zero shards now: free all its shards
                    to_move.extend(prev_shards.iter().copied());
                }
                Some(assigned_len) => {
                    let curr_len = prev_shards.len() as u16;
                    if curr_len == assigned_len {
                        // No change: copy shards over
                        shards_by_node.insert(*node_id, prev_shards.clone());
                    } else if curr_len > assigned_len {
                        // Reduce: remove extra shards from end
                        let keep = assigned_len as usize;
                        let mut node_shards = prev_shards.clone();
                        // Remove from the end to free
                        let to_free = node_shards.split_off(keep);
                        to_move.extend(to_free.into_iter());
                        shards_by_node.insert(*node_id, node_shards);
                    } else {
                        // curr_len < assigned_len: need more shards later
                        // Keep current shards and record that this node needs more
                        shards_by_node.insert(*node_id, prev_shards.clone());
                        // Re-insert with remaining needed count (assigned_len - curr_len)
                        let need_more = assigned_len - curr_len;
                        new_assignments.insert(*node_id, need_more);
                    }
                }
            }
        }

        // Check shard count consistency
        assert_eq!(new_total_shards, current_total_shards);

        // Now new_assignments contains only nodes that still need shards > 0,
        // including truly new nodes and nodes that had a deficit.

        fill_deficits(&mut shards_by_node, &mut to_move, &new_assignments);

        Committee {
            shards_by_node,
        }
    }

    pub fn contains(&self, node_id: &NodeId) -> bool {
        self.shards_by_node.contains_key(node_id)
    }

    pub fn shards(&self, node_id: &NodeId) -> Option<&Vec<u16>> {
        self.shards_by_node.get(node_id)
    }

    pub fn size(&self) -> usize {
        self.shards_by_node.len()
    }

    pub fn total_shards(&self) -> usize {
        self.shards_by_node.values().map(|v| v.len()).sum()
    }
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

pub fn max_shards_per_node(node_count: u64, shard_count: u64) -> u64 {
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

// Implementation of the D'Hondt method (aka Jefferson method).
// node_priorities: tie-breakers for nodes: higher means higher precedence
// shard_count: total shards to distribute
// stake: stake per node
fn dhondt(node_priorities: &[u64], shard_count: u16, stake: &[u64]) -> Vec<u16> {
    let node_count = stake.len();
    if node_count == 0 {
        return Vec::new();
    }
    let total_stake: u128 = stake.iter().map(|&x| x as u128).sum();

    assert!(total_stake > 0);

    let max_shards = max_shards_per_node(node_count as u64, shard_count as u64);

    // Hagenbach-Bischoff initial assignment
    // distribution number = total_stake/(shard_count + 1) + 1
    let dist_number = (total_stake as u128 / (shard_count as u128 + 1)) + 1;
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
    while distributed < shard_count as u64 {
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

#[derive(Clone, Debug)]
pub struct ActiveSet {
    // sorted by NodeId
    pub stake_by_node: BTreeMap<NodeId, u64>,
}

impl ActiveSet {
    pub fn new(mut items: Vec<(NodeId, u64)>) -> Self {
        let mut map = BTreeMap::new();
        items.sort_by_key(|(id, _)| *id);
        for (id, st) in items.into_iter() {
            map.insert(id, st);
        }
        Self { stake_by_node: map }
    }
}

// Perform apportionment given an active set and number of shards.
// Returns sorted mapping of NodeId -> shards allocated (>0 only).
pub fn stake_weighted_shard_counts(active_set: &ActiveSet, shard_count: u16) -> BTreeMap<NodeId, u16> {
    let node_count = active_set.stake_by_node.len();
    if node_count == 0 || shard_count == 0 {
        return BTreeMap::new();
    }

    let node_ids: Vec<NodeId> = active_set.stake_by_node.keys().cloned().collect();
    let stakes: Vec<u64> = node_ids.iter().map(|k| active_set.stake_by_node[k]).collect();

    // priorities: node_count - i (1-based preference), so earlier node (lower index) gets higher tie-breaker.
    let node_priorities: Vec<u64> = (0..node_count)
        .map(|i| (node_count - i) as u64)
        .collect();

    let shards_vec = dhondt(&node_priorities, shard_count, &stakes);
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

    fn map_from_vec(v: Vec<(NodeId, u16)>) -> BTreeMap<NodeId, u16> {
        let mut m = BTreeMap::new();
        for (k, val) in v {
            m.insert(k, val);
        }
        m
    }

    #[test]
    fn test_max_shards_per_node_threshold() {
        // At least 20 nodes: max shards per node is total/10
        assert_eq!(max_shards_per_node(20, 1000), 100);
        assert_eq!(max_shards_per_node(25, 1000), 100);
        // Below threshold: scaled up limit
        // ceil((1000*20)/(5*10)) = ceil(20000/50)= ceil(400)=400
        assert_eq!(max_shards_per_node(5, 1000), 400);
    }

    #[test]
    fn test_dhondt_basic_even() {
        let stake = vec![25_000_u64, 25_000, 25_000, 25_000];
        let priorities: Vec<u64> = (0..4).map(|i| 4 - i).collect();
        assert_eq!(dhondt(&priorities, 4, &stake), vec![1, 1, 1, 1]);

        let res = dhondt(&priorities, 1000, &stake);
        assert_eq!(res.iter().map(|&x| x as u64).sum::<u64>(), 1000);
        assert_eq!(res, vec![250, 250, 250, 250]);
    }

    #[test]
    fn test_dhondt_basic_uneven() {
        let stake = vec![50_000_u64, 30_000, 15_000, 5_000];
        let priorities: Vec<u64> = (0..4).map(|i| 4 - i).collect();
        assert_eq!(dhondt(&priorities, 4, &stake), vec![2, 2, 0, 0]);

        let res = dhondt(&priorities, 1000, &stake);
        assert_eq!(res.iter().map(|&x| x as u64).sum::<u64>(), 1000);
        assert_eq!(res, vec![500, 300, 150, 50]);
    }

    #[test]
    fn test_apportionment_equal_three_nodes() {
        // Expect a tie-broken split 4,3,3 for 10 shards
        let active = ActiveSet::new(vec![(NodeId(1), 1000), (NodeId(2), 1000), (NodeId(3), 1000)]);
        let res = stake_weighted_shard_counts(&active, 10);
        assert_eq!(res.values().copied().sum::<u16>(), 10);
        let v: Vec<u16> = [NodeId(1), NodeId(2), NodeId(3)]
            .iter()
            .map(|nid| *res.get(nid).unwrap_or(&0))
            .collect();
        assert_eq!(v, vec![4, 3, 3]);
    }

    #[test]
    fn test_compute_single_node() {
        let active = ActiveSet::new(vec![(NodeId(10), 1_000_000)]);
        let c = Committee::from(&active, 10);
        assert_eq!(c.size(), 1);
        assert_eq!(c.total_shards(), 10);
        assert_eq!(c.shards(&NodeId(10)).unwrap().len(), 10);
    }

    #[test]
    fn test_compute_even_distribution() {
        let active = ActiveSet::new(vec![(NodeId(1), 1000), (NodeId(2), 1000), (NodeId(3), 1000)]);
        let c = Committee::from(&active, 6);
        assert_eq!(c.size(), 3);
        assert_eq!(c.shards(&NodeId(1)).unwrap().len(), 2);
        assert_eq!(c.shards(&NodeId(2)).unwrap().len(), 2);
        assert_eq!(c.shards(&NodeId(3)).unwrap().len(), 2);
    }

    #[test]
    fn test_compute_uneven_distribution() {
        let active =
            ActiveSet::new(vec![(NodeId(1), 4000), (NodeId(2), 2000), (NodeId(3), 1000)]);
        let c = Committee::from(&active, 10);
        assert_eq!(c.size(), 3);
        assert_eq!(c.shards(&NodeId(1)).unwrap().len(), 6);
        assert_eq!(c.shards(&NodeId(2)).unwrap().len(), 3);
        assert_eq!(c.shards(&NodeId(3)).unwrap().len(), 1);
    }

    #[test]
    fn test_committee_initialize_and_transition_preserve() {
        // new with reversed order: 3..0, each 2 shards for total 8
        let init_map = map_from_vec(vec![
            (NodeId(3), 2),
            (NodeId(2), 2),
            (NodeId(1), 2),
            (NodeId(0), 2),
        ]);
        let c1 = Committee::new(init_map);
        assert_eq!(c1.size(), 4);
        assert_eq!(c1.shards(&NodeId(0)).unwrap(), &vec![0, 1]);
        assert_eq!(c1.shards(&NodeId(1)).unwrap(), &vec![2, 3]);
        assert_eq!(c1.shards(&NodeId(2)).unwrap(), &vec![4, 5]);
        assert_eq!(c1.shards(&NodeId(3)).unwrap(), &vec![6, 7]);

        // Remove two last nodes: new size 2, assign 4 shards each
        let t_map = map_from_vec(vec![(NodeId(3), 4), (NodeId(2), 4)]);
        let c2 = c1.transition(t_map);
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

        let c1 = Committee::new(map_from_vec(vec![
            (n1, 2),
            (n2, 2),
            (n3, 2),
            (n4, 2),
            (n5, 2),
        ]));
        assert_eq!(c1.size(), 5);
        assert_eq!(c1.shards(&n1).unwrap(), &vec![0, 1]);
        assert_eq!(c1.shards(&n2).unwrap(), &vec![2, 3]);
        assert_eq!(c1.shards(&n3).unwrap(), &vec![4, 5]);
        assert_eq!(c1.shards(&n4).unwrap(), &vec![6, 7]);
        assert_eq!(c1.shards(&n5).unwrap(), &vec![8, 9]);

        // Transition to 4/3/3 for n1,n2,n3
        let c2 = c1.transition(map_from_vec(vec![(n1, 4), (n2, 3), (n3, 3)]));
        assert_eq!(c2.size(), 3);
        let s1 = c2.shards(&n1).unwrap().clone();
        let s2 = c2.shards(&n2).unwrap().clone();
        let s3 = c2.shards(&n3).unwrap().clone();
        assert!(s1.contains(&0) && s1.contains(&1));
        assert!(s2.contains(&2) && s2.contains(&3));
        assert!(s3.contains(&4) && s3.contains(&5));

        // Transition to n2,n3,n4,n5: 3,3,2,2
        let c3 = c2.transition(map_from_vec(vec![(n2, 3), (n3, 3), (n4, 2), (n5, 2)]));
        assert_eq!(c3.size(), 4);
        assert_eq!(c3.shards(&n2).unwrap(), &s2);
        assert_eq!(c3.shards(&n3).unwrap(), &s3);
        assert_eq!(c3.shards(&n4).unwrap().len(), 2);
        assert_eq!(c3.shards(&n5).unwrap().len(), 2);

        // Finally transition to only n1 owning all 10 shards
        let c4 = c3.transition(map_from_vec(vec![(n1, 10)]));
        assert_eq!(c4.size(), 1);
        let s = c4.shards(&n1).unwrap();
        for i in 0..10 {
            assert!(s.contains(&(i as u16)));
        }
    }

    #[test]
    fn test_compute_next_committee_transition() {
        let active = ActiveSet::new(vec![
            (NodeId(1), 1000),
            (NodeId(2), 2000),
            (NodeId(3), 3000),
        ]);
        let c_first = Committee::from(&active, 6);
        assert_eq!(c_first.total_shards(), 6);

        // Next epoch: different active set (drop node 1, keep others)
        let active2 = ActiveSet::new(vec![(NodeId(2), 2000), (NodeId(3), 3000)]);
        let c_second = Committee::from_previous(&c_first, &active2, 6);
        assert_eq!(c_second.size(), 2);
        assert_eq!(c_second.total_shards(), 6);
    }
}
