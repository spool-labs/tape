use std::collections::BTreeMap;
use crate::types::NodeId;
use super::dhondt::allocate_shards;

/// Assign shards to nodes based on their stake.
/// Returns sorted mapping of NodeId -> shards allocated (>0 only).
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

    // Run D'Hondt allocation on stake weights
    let shards_vec = allocate_shards(&stakes, shard_count);

    let mut distribution = BTreeMap::new();
    for (i, &sh) in shards_vec.iter().enumerate() {
        if sh > 0 {
            distribution.insert(node_ids[i], sh);
        }
    }

    distribution
}

/// Move shards according to new target counts while minimizing movement.
/// Keep existing shards where possible, free those for removed or reduced assignments,
/// and assign freed shards to nodes that need more or newly added nodes.
pub fn move_shards(
    shards_by_node: &BTreeMap<NodeId, Vec<u16>>,
    target_counts: BTreeMap<NodeId, u16>,
) -> BTreeMap<NodeId, Vec<u16>> {
    let shard_to_node = from_vec(shards_by_node);
    let res = move_shards_new2(&shard_to_node, &target_counts);
    to_vec(&res)
}

pub fn move_shards_new2(
    shard_to_node: &BTreeMap<u16, NodeId>,
    target_counts: &BTreeMap<NodeId, u16>,
) -> BTreeMap<u16, NodeId> {
    // Result: shard_id -> NodeId
    let mut result: BTreeMap<u16, NodeId> = BTreeMap::new();

    // Remaining shards each node still needs to receive
    let mut remaining: BTreeMap<NodeId, u16> = target_counts.clone();

    // Shards that must be moved elsewhere
    let mut to_move: Vec<u16> = Vec::new();

    // Sanity: total shard count must match
    let total_target: u64 = target_counts.values().map(|&s| s as u64).sum();
    let total_current: u64 = shard_to_node.len() as u64;
    assert_eq!(
        total_target, total_current,
        "Target shard counts must sum to the total number of shards"
    );

    // First pass: keep shards on their current node when that node still has remaining capacity
    for (&shard_id, &node_id) in shard_to_node.iter() {
        match remaining.get_mut(&node_id) {
            Some(rem) if *rem > 0 => {
                result.insert(shard_id, node_id);
                *rem -= 1;
            }
            _ => {
                // Node removed or already satisfied; free this shard for reassignment
                to_move.push(shard_id);
            }
        }
    }

    // Second pass: assign freed shards to nodes that still need more (LIFO to preserve prior semantics)
    for (&node_id, &need) in remaining.iter() {
        for _ in 0..need {
            let shard = to_move.pop().expect("Not enough freed shards to reassign");
            result.insert(shard, node_id);
        }
    }

    debug_assert_eq!(result.len(), shard_to_node.len());
    result
}

pub fn move_shards_new(
    shard_to_node: &BTreeMap<u16, NodeId>,
    target_counts: &BTreeMap<NodeId, u16>,
) -> BTreeMap<NodeId, Vec<u16>> {
    // Result: NodeId -> Vec<shard_id>
    let mut result: BTreeMap<NodeId, Vec<u16>> = BTreeMap::new();

    // Remaining shards each node still needs to receive
    let mut remaining: BTreeMap<NodeId, u16> = target_counts.clone();

    // Shards that must be moved elsewhere
    let mut to_move: Vec<u16> = Vec::new();

    // Sanity: total shard count must match
    let total_target: u64 = target_counts.values().map(|&s| s as u64).sum();
    let total_current: u64 = shard_to_node.len() as u64;
    assert_eq!(
        total_target, total_current,
        "Target shard counts must sum to the total number of shards"
    );

    // First pass: keep shards on their current node when that node still has remaining capacity
    for (&shard_id, &node_id) in shard_to_node.iter() {
        match remaining.get_mut(&node_id) {
            Some(rem) if *rem > 0 => {
                result.entry(node_id).or_default().push(shard_id);
                *rem -= 1;
            }
            _ => {
                // Node removed or already satisfied; free this shard for reassignment
                to_move.push(shard_id);
            }
        }
    }

    // Second pass: assign freed shards to nodes that still need more (LIFO to preserve prior semantics)
    for (&node_id, &need) in remaining.iter() {
        let need = need as usize;
        if need == 0 {
            continue;
        }
        let mut curr = result.remove(&node_id).unwrap_or_default();
        for _ in 0..need {
            let shard = to_move.pop().expect("Not enough freed shards to reassign");
            curr.push(shard);
        }
        result.insert(node_id, curr);
    }

    result
}

pub fn move_shards_old(
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

        let mut curr = new_assignment
            .remove(&node_id)
            .unwrap_or_default();

        for _ in 0..need {
            let shard = to_move.pop().expect("Not enough freed shards to reassign");
            curr.push(shard);
        }

        new_assignment.insert(node_id, curr);
    }

    new_assignment
}

pub fn from_vec(
    shards_by_node: &BTreeMap<NodeId, Vec<u16>>,
) -> BTreeMap<u16, NodeId> {
    let mut out = BTreeMap::new();
    for (node_id, shard_list) in shards_by_node {
        for &sh in shard_list {
            if out.insert(sh, *node_id).is_some() {
                panic!("Duplicate shard id {sh} encountered while inverting assignments");
            }
        }
    }
    out
}

pub fn to_vec(
    shard_to_node: &BTreeMap<u16, NodeId>,
) -> BTreeMap<NodeId, Vec<u16>> {
    let mut out : BTreeMap<NodeId, Vec<u16>> = BTreeMap::new();
    for (&sh, &node_id) in shard_to_node {
        out.entry(node_id).or_default().push(sh);
    }
    out
}

pub fn map_shard_indices2(assigned_number: &BTreeMap<NodeId, u16>) -> BTreeMap<u16, NodeId> {
    let mut shard_to_node: BTreeMap<u16, NodeId> = BTreeMap::new();
    let mut next_shard: u16 = 0;
    for (node, count) in assigned_number.iter() {
        for _ in 0..*count {
            shard_to_node.insert(next_shard, *node);
            next_shard = next_shard.saturating_add(1);
        }
    }
    shard_to_node
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

#[cfg(test)]
mod tests {
    use super::*;

    fn total_shard_count(shards_by_node: &BTreeMap<NodeId, Vec<u16>>) -> usize {
        shards_by_node.values().map(|v| v.len()).sum()
    }

    #[test]
    fn test_single() {
        let stake_map: BTreeMap<NodeId, u64> = [
            (NodeId(10), 1_000_000)
        ].into();

        let shard_counts = assign_shards(&stake_map, 10);
        let shards_map = map_shard_indices(shard_counts);

        assert_eq!(shards_map.len(), 1);
        assert_eq!(total_shard_count(&shards_map), 10);
        assert_eq!(shards_map.get(&NodeId(10)).unwrap().len(), 10);
    }

    #[test]
    fn test_equal() {
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
    fn test_even() {
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
    fn test_uneven() {
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
    fn test_reassign() {
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

        let target: BTreeMap<NodeId, u16> = [
            (NodeId(3), 4), 
            (NodeId(2), 4)
        ].into();

        let shards_map2 = move_shards(&shards_map1, target);
        assert_eq!(shards_map2.len(), 2);

        let s3 = shards_map2.get(&NodeId(3)).unwrap();
        assert!(s3.contains(&6) && s3.contains(&7));
        assert!(s3.contains(&0) && s3.contains(&1));

        let s2 = shards_map2.get(&NodeId(2)).unwrap();
        assert!(s2.contains(&4) && s2.contains(&5));
        assert!(s2.contains(&2) && s2.contains(&3));
    }

    #[test]
    fn test_reassign_reduce() {
        let initial_stakes: BTreeMap<NodeId, u64> = [
            (NodeId(1), 1000),
            (NodeId(2), 2000),
            (NodeId(3), 3000),
        ].into();

        let shard_counts = assign_shards(&initial_stakes, 6);
        let initial_shard_map = map_shard_indices(shard_counts);
        assert_eq!(total_shard_count(&initial_shard_map), 6);

        let updated_stakes: BTreeMap<NodeId, u64> = [
            (NodeId(2), 2000),
            (NodeId(3), 3000),
        ].into();

        let shard_counts = assign_shards(&updated_stakes, 6);
        let updated_shard_map = move_shards(&initial_shard_map, shard_counts);

        assert_eq!(updated_shard_map.len(), 2);
        assert_eq!(total_shard_count(&updated_shard_map), 6);
    }

    #[test]
    fn test_reassign_chain() {
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
    fn test_many() {
        fn print_table_header() {
            println!(
                "{:<8} | {:>12} | {:>6} | {}",
                "NodeId", "Stake", "Shards", "ShardIds"
            );
            println!("{}", "-".repeat(8 + 3 + 12 + 3 + 6 + 3 + 40));
        }

        // Generate 100 nodes with stakes from 1000 to 100,000
        let initial_stakes: BTreeMap<NodeId, u64> = (1..=100)
            .map(|i| (NodeId(100 - i), i as u64 * 1000))
            .collect();
        
        let shard_counts = assign_shards(&initial_stakes, 1000);
        let initial_shard_map = map_shard_indices(shard_counts);
        assert_eq!(total_shard_count(&initial_shard_map), 1000);

        print_table_header();
        for (node_id, shard_ids) in &initial_shard_map {
            let stake = initial_stakes.get(node_id).unwrap_or(&0);
            println!(
                "{:<8} | {:>12} | {:>6} | {:?}",
                format!("{:?}", node_id),
                stake,
                shard_ids.len(),
                shard_ids
            );
        }

        // Updated stakes: keep only nodes 51 to 100
        let updated_stakes: BTreeMap<NodeId, u64> = (51..=100)
            .map(|i| (NodeId(100 - i), i as u64 * 1000))
            .collect();
        
        let shard_counts = assign_shards(&updated_stakes, 1000);
        let updated_shard_map = move_shards(&initial_shard_map, shard_counts);
        
        assert_eq!(updated_shard_map.len(), 50);
        assert_eq!(total_shard_count(&updated_shard_map), 1000);

        // Print updated shard map
        println!("\nAfter reassignment:");
        print_table_header();
        for (node_id, shard_ids) in &updated_shard_map {
            let stake = updated_stakes.get(node_id).unwrap_or(&0);
            println!(
                "{:<8} | {:>12} | {:>6} | {:?}",
                format!("{:?}", node_id),
                stake,
                shard_ids.len(),
                shard_ids
            );
        }
    }
}
