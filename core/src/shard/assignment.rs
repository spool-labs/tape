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
    current: &[NodeId],
    target_counts: &BTreeMap<NodeId, u16>,
) -> Vec<NodeId> {
    let total_current: usize = current.len();
    let total_target: usize = target_counts
        .values()
        .map(|&s| s as usize)
        .sum();

    assert_eq!(
        total_target, total_current,
        "Target shard counts must sum to the total number of shards"
    );

    // Start with current mapping; keep slots as-is unless they must be moved.
    let mut result: Vec<NodeId> = current.to_vec();

    // Remaining shards each node still needs to receive
    let mut remaining: BTreeMap<NodeId, u16> = target_counts.clone();

    // Shards that must be moved elsewhere
    let mut to_move: Vec<usize> = Vec::new();

    // First pass: keep shards on their current node when that node still has remaining capacity
    for (shard_id, &node_id) in current.iter().enumerate() {
        match remaining.get_mut(&node_id) {
            Some(rem) if *rem > 0 => {
                // Keep this shard with the same node; just decrement remaining
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
            result[shard] = node_id;
        }
    }

    debug_assert!(to_move.is_empty(), "Unassigned shards remain after reassignment");
    result
}

/// Map per-node assigned shard counts into a Vec<NodeId> where index is shard_id.
/// The order is deterministic (ascending NodeId order), so each node's shards occupy contiguous ranges.
pub fn map_shard_indices(assigned_number: &BTreeMap<NodeId, u16>) -> Vec<NodeId> {
    let total: usize = assigned_number.values().map(|&c| c as usize).sum();
    let mut shard_to_node: Vec<NodeId> = Vec::with_capacity(total);
    for (node, count) in assigned_number.iter() {
        for _ in 0..*count {
            shard_to_node.push(*node);
        }
    }
    shard_to_node
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    fn total_count(shard_to_node: &[NodeId]) -> usize {
        shard_to_node.len()
    }

    fn shard_count(
        shard_to_node: &[NodeId], 
        node_id: NodeId
    ) -> usize {
        shard_to_node
            .iter()
            .filter(|&&n| n == node_id)
            .count()
    }

    fn shards(
        shard_to_node: &[NodeId], 
        node_id: NodeId
    ) -> Vec<u16> {
        shard_to_node
            .iter()
            .enumerate()
            .filter_map(|(shard_id, &n)| if n == node_id { Some(shard_id as u16) } else { None })
            .collect()
    }

    #[test]
    fn test_single() {
        let stake_map: BTreeMap<NodeId, u64> = [
            (NodeId(10), 1_000_000)
        ].into();

        let shard_counts = assign_shards(&stake_map, 10);
        let shards_map = map_shard_indices(&shard_counts);

        assert_eq!(total_count(&shards_map), 10);
        assert_eq!(shard_count(&shards_map, NodeId(10)), 10);
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
        let shards_map = map_shard_indices(&shard_counts);

        assert_eq!(shard_count(&shards_map, NodeId(1)), 2);
        assert_eq!(shard_count(&shards_map, NodeId(2)), 2);
        assert_eq!(shard_count(&shards_map, NodeId(3)), 2);
    }

    #[test]
    fn test_uneven() {
        let stake_map: BTreeMap<NodeId, u64> = [
            (NodeId(1), 4000),
            (NodeId(2), 2000),
            (NodeId(3), 1000),
        ].into();

        let shard_counts = assign_shards(&stake_map, 10);
        let shards_map = map_shard_indices(&shard_counts);

        assert_eq!(shard_count(&shards_map, NodeId(1)), 6);
        assert_eq!(shard_count(&shards_map, NodeId(2)), 3);
        assert_eq!(shard_count(&shards_map, NodeId(3)), 1);
    }

    #[test]
    fn test_reassign() {
        let init_map: BTreeMap<NodeId, u16> = [
            (NodeId(3), 2),
            (NodeId(2), 2),
            (NodeId(1), 2),
            (NodeId(0), 2),
        ].into();

        let shards_map1 = map_shard_indices(&init_map);

        assert_eq!(shards(&shards_map1, NodeId(0)), vec![0, 1]);
        assert_eq!(shards(&shards_map1, NodeId(1)), vec![2, 3]);
        assert_eq!(shards(&shards_map1, NodeId(2)), vec![4, 5]);
        assert_eq!(shards(&shards_map1, NodeId(3)), vec![6, 7]);

        let target: BTreeMap<NodeId, u16> = [
            (NodeId(3), 4), 
            (NodeId(2), 4)
        ].into();

        let shards_map2 = move_shards(&shards_map1, &target);

        let s3 = shards(&shards_map2, NodeId(3));
        assert!(s3.contains(&6) && s3.contains(&7));
        assert!(s3.contains(&0) && s3.contains(&1));

        let s2 = shards(&shards_map2, NodeId(2));
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
        let initial_shard_map = map_shard_indices(&shard_counts);
        assert_eq!(total_count(&initial_shard_map), 6);

        let updated_stakes: BTreeMap<NodeId, u64> = [
            (NodeId(2), 2000),
            (NodeId(3), 3000),
        ].into();

        let shard_counts = assign_shards(&updated_stakes, 6);
        let updated_shard_map = move_shards(&initial_shard_map, &shard_counts);

        assert_eq!(total_count(&updated_shard_map), 6);
    }

    #[test]
    fn test_reassign_chain() {
        let n1 = NodeId(1);
        let n2 = NodeId(2);
        let n3 = NodeId(3);
        let n4 = NodeId(4);
        let n5 = NodeId(5);

        let shards_map1 = map_shard_indices(
            &[(n1, 2), (n2, 2), (n3, 2), (n4, 2), (n5, 2)].into()
        );
        assert_eq!(shards_map1.len(), 10);
        assert_eq!(shards(&shards_map1, n1), vec![0, 1]);
        assert_eq!(shards(&shards_map1, n2), vec![2, 3]);
        assert_eq!(shards(&shards_map1, n3), vec![4, 5]);
        assert_eq!(shards(&shards_map1, n4), vec![6, 7]);
        assert_eq!(shards(&shards_map1, n5), vec![8, 9]);

        let shards_map2 = move_shards(&shards_map1, &[(n1, 4), (n2, 3), (n3, 3)].into());
        let s1 = shards(&shards_map2, n1);
        let s2 = shards(&shards_map2, n2);
        let s3 = shards(&shards_map2, n3);
        assert!(s1.contains(&0) && s1.contains(&1));
        assert!(s2.contains(&2) && s2.contains(&3));
        assert!(s3.contains(&4) && s3.contains(&5));

        let shards_map3 = move_shards(&shards_map2, &[(n2, 3), (n3, 3), (n4, 2), (n5, 2)].into());
        assert_eq!(shards(&shards_map3, n2), s2);
        assert_eq!(shards(&shards_map3, n3), s3);
        assert_eq!(shards(&shards_map3, n4).len(), 2);
        assert_eq!(shards(&shards_map3, n5).len(), 2);

        let shards_map4 = move_shards(&shards_map3, &[(n1, 10)].into());
        let s = shards(&shards_map4, n1);
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
        let initial_shard_map = map_shard_indices(&shard_counts);
        assert_eq!(total_count(&initial_shard_map), 1000);

        print_table_header();
        for node_id in initial_stakes.keys() {
            let stake = initial_stakes.get(node_id).unwrap_or(&0);
            let shards_for_node = shards(&initial_shard_map, *node_id);
            println!(
                "{:<8} | {:>12} | {:>6} | {:?}",
                format!("{:?}", node_id),
                stake,
                shards_for_node.len(),
                shards_for_node
            );
        }

        // Updated stakes: keep only nodes 51 to 100
        let updated_stakes: BTreeMap<NodeId, u64> = (51..=100)
            .map(|i| (NodeId(100 - i), i as u64 * 1000))
            .collect();

        let shard_counts = assign_shards(&updated_stakes, 1000);
        let updated_shard_map = move_shards(&initial_shard_map, &shard_counts);

        // Verify total shards and correct number of nodes
        assert_eq!(total_count(&updated_shard_map), 1000);
        let unique_nodes: HashSet<NodeId> = updated_shard_map.iter().cloned().collect();
        assert_eq!(unique_nodes.len(), 50, "Expected 50 nodes in updated_shard_map");

        // Verify shard counts match target_counts
        for (node_id, &count) in &shard_counts {
            assert_eq!(
                shard_count(&updated_shard_map, *node_id),
                count as usize,
                "Shard count mismatch for node {:?}", node_id
            );
        }

        // Verify nodes not in updated_stakes have no shards
        for node_id in initial_stakes.keys() {
            if !updated_stakes.contains_key(node_id) {
                assert_eq!(
                    shard_count(&updated_shard_map, *node_id),
                    0,
                    "Node {:?} should have no shards", node_id
                );
            }
        }

        // Print updated shard map
        println!("\nAfter reassignment:");
        print_table_header();
        for node_id in updated_stakes.keys() {
            let stake = updated_stakes.get(node_id).unwrap_or(&0);
            let shards_for_node = shards(&updated_shard_map, *node_id);
            println!(
                "{:<8} | {:>12} | {:>6} | {:?}",
                format!("{:?}", node_id),
                stake,
                shards_for_node.len(),
                shards_for_node
            );
        }
    }
}
