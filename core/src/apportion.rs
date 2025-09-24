use std::cmp::Ordering;
use std::collections::{BTreeMap, BinaryHeap};
use crate::types::NodeId;

// Assign shards to nodes based on their stake.
// Returns sorted mapping of NodeId -> shards allocated (>0 only).
pub fn assign_shards(
    stake_by_node: &BTreeMap<NodeId, u64>, 
    shard_count: u16
) -> BTreeMap<NodeId, u16> {
    let node_count = stake_by_node.len();
    if node_count == 0 || shard_count == 0 {
        return BTreeMap::new();
    }

    let node_ids: Vec<NodeId> = stake_by_node
        .keys()
        .cloned()
        .collect();

    let stakes: Vec<u64> = node_ids
        .iter()
        .map(|k| stake_by_node[k])
        .collect();

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

// Reassign shards according to new target counts while minimizing movement.
// Keep existing shards where possible, free those for removed or reduced assignments,
// and assign freed shards to nodes that need more or newly added nodes.
pub fn move_shards(
    shards_by_node: &BTreeMap<NodeId, Vec<u16>>,
    target_counts: BTreeMap<NodeId, u16>,
) -> BTreeMap<NodeId, Vec<u16>> {

    let mut new_assignment: BTreeMap<NodeId, Vec<u16>> = BTreeMap::new();
    let mut target_counts = target_counts;
    let mut to_move: Vec<u16> = Vec::new();

    let total_shards: u64 = target_counts
        .values()
        .map(|&s| s as u64)
        .sum();

    let mut new_total_shards: u64 = 0;

    // First pass: try to preserve existing assignments
    for (node_id, prev_shards) in shards_by_node.iter() {
        new_total_shards += prev_shards.len() as u64;

        match target_counts.remove(node_id) {
            None => {
                to_move.extend(prev_shards.iter().copied());
            }
            Some(assigned_count) if assigned_count == 0 => {
                to_move.extend(prev_shards.iter().copied());
            }
            Some(assigned_count) => {
                let current_count = prev_shards.len() as u16;
                if current_count == assigned_count {
                    new_assignment.insert(*node_id, prev_shards.clone());
                } else if current_count > assigned_count {
                    let keep = assigned_count as usize;
                    let mut node_shards = prev_shards.clone();
                    let to_free = node_shards.split_off(keep);
                    to_move.extend(to_free.into_iter());
                    new_assignment.insert(*node_id, node_shards);
                } else {
                    new_assignment.insert(*node_id, prev_shards.clone());
                    let need_more = assigned_count - current_count;
                    target_counts.insert(*node_id, need_more);
                }
            }
        }
    }

    // All shards must be accounted for
    assert_eq!(total_shards, new_total_shards);

    // Fill remaining needs by consuming from `to_move` 
    // (LIFO to preserve existing order semantics).
    for (&node_id, &needed) in target_counts.iter() {
        let need = needed as usize;
        if need == 0 {
            continue;
        }
        let mut curr = new_assignment.remove(&node_id).unwrap_or_default();

        for _ in 0..need {
            let shard = to_move.pop().expect("Not enough freed shards to reassign");
            curr.push(shard);
        }

        new_assignment.insert(node_id, curr);
    }

    new_assignment
}

pub fn map_shard_indices(assigned_number: BTreeMap<NodeId, u16>) -> BTreeMap<NodeId, Vec<u16>> {
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
    map
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
        let left = self.quotient.numer.saturating_mul(other.quotient.denom);
        let right = other.quotient.numer.saturating_mul(self.quotient.denom);

        match left.cmp(&right) {
            Ordering::Greater => Ordering::Greater,
            Ordering::Less => Ordering::Less,
            Ordering::Equal => {
                match self.tie_breaker.cmp(&other.tie_breaker) {
                    Ordering::Greater => Ordering::Greater,
                    Ordering::Less => Ordering::Less,
                    Ordering::Equal => {
                        other.index.cmp(&self.index)
                    }
                }
            }
        }
    }
}

pub fn max_shards_per_node(node_count: u64, shard_count: u64) -> u64 {
    const MIN_NODES: u64 = 20;
    const MAX_PER_NODE_SHARE: u64 = 10;

    if shard_count == 0 || node_count == 0 {
        return 0;
    }

    if node_count >= MIN_NODES {
        shard_count / MAX_PER_NODE_SHARE
    } else {
        let num = shard_count.saturating_mul(MIN_NODES);
        let den = node_count.saturating_mul(MAX_PER_NODE_SHARE);
        num.saturating_add(den - 1) / den
    }
}

pub fn dhondt(node_priorities: &[u64], shard_count: u16, stake: &[u64]) -> Vec<u16> {
    let node_count = stake.len();
    if node_count == 0 {
        return Vec::new();
    }
    let total_stake: u128 = stake.iter().map(|&x| x as u128).sum();

    assert!(total_stake > 0);

    let max_shards = max_shards_per_node(node_count as u64, shard_count as u64);

    let dist_number = (total_stake as u128 / (shard_count as u128 + 1)) + 1;
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


#[cfg(test)]
mod tests {
    use super::*;

    fn total_shard_count(shards_by_node: &BTreeMap<NodeId, Vec<u16>>) -> usize {
        shards_by_node.values().map(|v| v.len()).sum()
    }

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
        let stake_map: BTreeMap<NodeId, u64> = [
            (NodeId(1), 1000),
            (NodeId(2), 1000),
            (NodeId(3), 1000),
        ].into();
        let res = assign_shards(&stake_map, 10);
        assert_eq!(res.values().copied().sum::<u16>(), 10);
        let v: Vec<u16> = [NodeId(1), NodeId(2), NodeId(3)]
            .iter()
            .map(|nid| *res.get(nid).unwrap_or(&0))
            .collect();
        assert_eq!(v, vec![4, 3, 3]);
    }

    #[test]
    fn test_compute_single_node() {
        let stake_map: BTreeMap<NodeId, u64> = [(NodeId(10), 1_000_000)].into();
        let shard_counts = assign_shards(&stake_map, 10);
        let shards_map = map_shard_indices(shard_counts);
        assert_eq!(shards_map.len(), 1);
        assert_eq!(total_shard_count(&shards_map), 10);
        assert_eq!(shards_map.get(&NodeId(10)).unwrap().len(), 10);
    }

    #[test]
    fn test_compute_even_distribution() {
        let stake_map: BTreeMap<NodeId, u64> = [
            (NodeId(1), 1000),
            (NodeId(2), 1000),
            (NodeId(3), 1000),
        ].into();
        let shard_counts = assign_shards(&stake_map, 6);
        let shards_map = map_shard_indices(shard_counts);
        assert_eq!(shards_map.len(), 3);
        assert_eq!(shards_map.get(&NodeId(1)).unwrap().len(), 2);
        assert_eq!(shards_map.get(&NodeId(2)).unwrap().len(), 2);
        assert_eq!(shards_map.get(&NodeId(3)).unwrap().len(), 2);
    }

    #[test]
    fn test_compute_uneven_distribution() {
        let stake_map: BTreeMap<NodeId, u64> = [
            (NodeId(1), 4000),
            (NodeId(2), 2000),
            (NodeId(3), 1000),
        ].into();
        let shard_counts = assign_shards(&stake_map, 10);
        let shards_map = map_shard_indices(shard_counts);
        assert_eq!(shards_map.len(), 3);
        assert_eq!(shards_map.get(&NodeId(1)).unwrap().len(), 6);
        assert_eq!(shards_map.get(&NodeId(2)).unwrap().len(), 3);
        assert_eq!(shards_map.get(&NodeId(3)).unwrap().len(), 1);
    }

    #[test]
    fn test_initialize_and_reassign_preserve() {
        let init_map: BTreeMap<NodeId, u16> = [
            (NodeId(3), 2),
            (NodeId(2), 2),
            (NodeId(1), 2),
            (NodeId(0), 2),
        ].into();
        let shards_map1 = map_shard_indices(init_map);
        assert_eq!(shards_map1.len(), 4);
        assert_eq!(shards_map1.get(&NodeId(0)).unwrap(), &vec![0, 1]);
        assert_eq!(shards_map1.get(&NodeId(1)).unwrap(), &vec![2, 3]);
        assert_eq!(shards_map1.get(&NodeId(2)).unwrap(), &vec![4, 5]);
        assert_eq!(shards_map1.get(&NodeId(3)).unwrap(), &vec![6, 7]);

        let t_map: BTreeMap<NodeId, u16> = [(NodeId(3), 4), (NodeId(2), 4)].into();
        let shards_map2 = move_shards(&shards_map1, t_map);
        assert_eq!(shards_map2.len(), 2);

        let s3 = shards_map2.get(&NodeId(3)).unwrap();
        assert!(s3.contains(&6) && s3.contains(&7));
        assert!(s3.contains(&0) && s3.contains(&1));

        let s2 = shards_map2.get(&NodeId(2)).unwrap();
        assert!(s2.contains(&4) && s2.contains(&5));
        assert!(s2.contains(&2) && s2.contains(&3));
    }

    #[test]
    fn test_default_scenario_reassign_chain() {
        let n1 = NodeId(1);
        let n2 = NodeId(2);
        let n3 = NodeId(3);
        let n4 = NodeId(4);
        let n5 = NodeId(5);

        let shards_map1 = map_shard_indices(
            [(n1, 2), (n2, 2), (n3, 2), (n4, 2), (n5, 2)].into()
        );
        assert_eq!(shards_map1.len(), 5);
        assert_eq!(shards_map1.get(&n1).unwrap(), &vec![0, 1]);
        assert_eq!(shards_map1.get(&n2).unwrap(), &vec![2, 3]);
        assert_eq!(shards_map1.get(&n3).unwrap(), &vec![4, 5]);
        assert_eq!(shards_map1.get(&n4).unwrap(), &vec![6, 7]);
        assert_eq!(shards_map1.get(&n5).unwrap(), &vec![8, 9]);

        let shards_map2 = move_shards(&shards_map1, [(n1, 4), (n2, 3), (n3, 3)].into());
        assert_eq!(shards_map2.len(), 3);
        let s1 = shards_map2.get(&n1).unwrap().clone();
        let s2 = shards_map2.get(&n2).unwrap().clone();
        let s3 = shards_map2.get(&n3).unwrap().clone();
        assert!(s1.contains(&0) && s1.contains(&1));
        assert!(s2.contains(&2) && s2.contains(&3));
        assert!(s3.contains(&4) && s3.contains(&5));

        let shards_map3 = move_shards(&shards_map2, [(n2, 3), (n3, 3), (n4, 2), (n5, 2)].into());
        assert_eq!(shards_map3.len(), 4);
        assert_eq!(shards_map3.get(&n2).unwrap(), &s2);
        assert_eq!(shards_map3.get(&n3).unwrap(), &s3);
        assert_eq!(shards_map3.get(&n4).unwrap().len(), 2);
        assert_eq!(shards_map3.get(&n5).unwrap().len(), 2);

        let shards_map4 = move_shards(&shards_map3, [(n1, 10)].into());
        assert_eq!(shards_map4.len(), 1);
        let s = shards_map4.get(&n1).unwrap();
        for i in 0..10 {
            assert!(s.contains(&(i as u16)));
        }
    }

    #[test]
    fn test_compute_next_reassign() {
        let stake_map: BTreeMap<NodeId, u64> = [
            (NodeId(1), 1000),
            (NodeId(2), 2000),
            (NodeId(3), 3000),
        ].into();
        let shard_counts = assign_shards(&stake_map, 6);
        let shards_map_first = map_shard_indices(shard_counts);
        assert_eq!(total_shard_count(&shards_map_first), 6);

        let stake_map2: BTreeMap<NodeId, u64> = [(NodeId(2), 2000), (NodeId(3), 3000)].into();
        let shards_map_second = if shards_map_first.is_empty() {
            let shard_counts = assign_shards(&stake_map2, 6);
            map_shard_indices(shard_counts)
        } else {
            let shard_counts = assign_shards(&stake_map2, 6);
            move_shards(&shards_map_first, shard_counts)
        };
        assert_eq!(shards_map_second.len(), 2);
        assert_eq!(total_shard_count(&shards_map_second), 6);
    }
}
