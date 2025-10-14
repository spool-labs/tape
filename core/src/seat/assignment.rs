use std::collections::BTreeMap;
use crate::types::NodeId;
use super::dhondt::allocate_seats;
use crate::types::*;

/// Assign seats to nodes based on their stake.
/// Returns sorted mapping of NodeId -> seats allocated (>0 only).
pub fn assign_seats(
    stake_by_node: &BTreeMap<NodeId, u64>, 
    seat_count: u16
) -> BTreeMap<NodeId, u16> {
    let node_count = stake_by_node.len();
    if node_count == 0 || seat_count == 0 {
        return BTreeMap::new();
    }

    let node_ids: Vec<NodeId> = stake_by_node
        .keys()
        .cloned()
        .collect();

    let stakes: Vec<Coin<TAPE>> = node_ids
        .iter()
        .map(|k| stake_by_node[k].into())
        .collect();

    let seats_vec = allocate_seats(&stakes, seat_count);

    let mut distribution = BTreeMap::new();
    for (i, &sh) in seats_vec.iter().enumerate() {
        if sh > 0 {
            distribution.insert(node_ids[i], sh);
        }
    }

    distribution
}

/// Move seats according to new target counts while minimizing movement.
/// Keep existing seats where possible, free those for removed or reduced assignments,
/// and assign freed seats to nodes that need more or newly added nodes.
pub fn move_seats(
    current_seats: &[NodeId],
    target_counts: &BTreeMap<NodeId, u16>,
) -> Vec<NodeId> {

    // Start with current_seats mapping; keep slots as-is unless they must be moved.
    let mut result: Vec<NodeId> = current_seats.to_vec();

    // Remaining seats each node still needs to receive
    let mut remaining: BTreeMap<NodeId, u16> = target_counts.clone();

    // seats that must be moved elsewhere
    let mut to_move: Vec<usize> = Vec::new();

    // First pass: keep seats on their current_seats node when that node still has remaining capacity
    for (seat_id, &node_id) in current_seats.iter().enumerate() {
        match remaining.get_mut(&node_id) {
            Some(rem) if *rem > 0 => {
                // Keep this seat with the same node; just decrement remaining
                *rem -= 1;
            }
            _ => {
                // Node removed or already satisfied; free this seat for reassignment
                to_move.push(seat_id);
            }
        }
    }

    // Second pass: assign freed seats to nodes that still need more (LIFO to preserve prior semantics)
    for (&node_id, &need) in remaining.iter() {
        for _ in 0..need {
            let seat = to_move.pop().expect("Not enough freed seats to reassign");
            result[seat] = node_id;
        }
    }

    debug_assert!(to_move.is_empty(), "Unassigned seats remain after reassignment");
    result
}

/// Map per-node assigned seat counts into a Vec<NodeId> where index is seat_id.
/// The order is deterministic (ascending NodeId order), so each node's seats occupy contiguous ranges.
pub fn map_seat_indices(assigned_number: &BTreeMap<NodeId, u16>) -> Vec<NodeId> {
    let total: usize = assigned_number.values().map(|&c| c as usize).sum();
    let mut seat_to_node: Vec<NodeId> = Vec::with_capacity(total);
    for (node, count) in assigned_number.iter() {
        for _ in 0..*count {
            seat_to_node.push(*node);
        }
    }
    seat_to_node
}


pub fn move_seats2<const SEATS: usize, const NODES: usize>(
    current_seats: &[u8; SEATS],   // &[u8; 1000], seat_index -> node_idx
    target_counts: &[u16; NODES],  // &[u16; 256], node_idx -> desired seat count
) -> [u8; SEATS] {
    debug_assert!(NODES <= 256, "NODES must be <= 256 for u8 node indices");

    let mut result = *current_seats;
    let mut remaining = *target_counts;
    let mut to_move = Vec::new();

    // In this pass we try to keep each seat with its current node if that node still needs seats.
    // What we're doing:
    //   - For each seat, look at its current node and see if that node still has remaining demand.
    //   - If yes: keep the seat (minimizes movement) and decrement that node's remaining demand.
    //   - If no: free the seat by pushing it onto the to_move stack for later reassignment.
    // Outcome:
    //   - Seats that can be kept stay in place.
    //   - Seats that must move are collected in to_move.
    //   - remaining[] now reflects only the seats that still need to be assigned in pass two.
    for seat in 0..SEATS {
        let idx = current_seats[seat] as usize;
        if remaining[idx] > 0 {
            remaining[idx] -= 1;
        } else {
            to_move.push(seat as u16);
        }
    }

    debug_assert!(
        to_move.len() == remaining.iter().map(|&x| x as usize).sum::<usize>(),
        "Mismatch between freed seats and remaining demand"
    );

    // In this pass we assign freed seats to the nodes that still need them. 
    // What we're doing:
    //   - Iterate all possible node indices; for each, assign as many seats as remaining[node_idx].
    //   - Seats are taken from the to_move stack in LIFO order.
    for node_idx in 0..NODES {
        let need = remaining[node_idx] as usize;
        for _ in 0..need {
            let seat = to_move
                .pop()
                .expect("Not enough freed seats to reassign") as usize;
            result[seat] = node_idx as u8;
        }
    }

    debug_assert!(to_move.is_empty(), "Unassigned seats remain after reassignment");
    result
}

// /// Combine members from the current committee and the next leader set into a single Vec.
// pub fn merge_members(
//     current: &AppointedSet,
//     next: &LeaderSet
// ) -> Vec<CommitteeMember> {
//     let mut mappings = Vec::new();
//
//     // First add all members from the current committee to the mappings array
//     for member in committee.inner.iter_members() {
//         mappings.push(member);
//     }
//
//     // Then add members from the leader set
//     for index in 0..epoch.leaders.size() {
//         let member = &epoch.leaders.members[index];
//
//         // Check if this member was already in the array
//         let previous = mappings
//             .iter()
//             .position(|&m| m.id == member.id);
//
//         // If yes, use the latest CommitteeMember
//         // (in case the BlsPubkey changed)
//         if let Some(index) = previous {
//             mappings[index] = member; 
//         } else {
//             mappings.push(member);
//         }
//     }
//
//     mappings
// }


#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    fn total_count(seat_to_node: &[NodeId]) -> usize {
        seat_to_node.len()
    }

    fn seat_count(
        seat_to_node: &[NodeId], 
        node_id: NodeId
    ) -> usize {
        seat_to_node
            .iter()
            .filter(|&&n| n == node_id)
            .count()
    }

    fn seats(
        seat_to_node: &[NodeId], 
        node_id: NodeId
    ) -> Vec<u16> {
        seat_to_node
            .iter()
            .enumerate()
            .filter_map(|(seat_id, &n)| if n == node_id { Some(seat_id as u16) } else { None })
            .collect()
    }

    #[test]
    fn test_single() {
        let stake_map: BTreeMap<NodeId, u64> = [
            (NodeId(10), 1_000_000)
        ].into();

        let seat_counts = assign_seats(&stake_map, 10);
        let seats_map = map_seat_indices(&seat_counts);

        assert_eq!(total_count(&seats_map), 10);
        assert_eq!(seat_count(&seats_map, NodeId(10)), 10);
    }

    #[test]
    fn test_equal() {
        let stake_map: BTreeMap<NodeId, u64> = [
            (NodeId(1), 1000),
            (NodeId(2), 1000),
            (NodeId(3), 1000),
        ].into();

        let res = assign_seats(&stake_map, 10);
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

        let seat_counts = assign_seats(&stake_map, 6);
        let seats_map = map_seat_indices(&seat_counts);

        assert_eq!(seat_count(&seats_map, NodeId(1)), 2);
        assert_eq!(seat_count(&seats_map, NodeId(2)), 2);
        assert_eq!(seat_count(&seats_map, NodeId(3)), 2);
    }

    #[test]
    fn test_uneven() {
        let stake_map: BTreeMap<NodeId, u64> = [
            (NodeId(1), 4000),
            (NodeId(2), 2000),
            (NodeId(3), 1000),
        ].into();

        let seat_counts = assign_seats(&stake_map, 10);
        let seats_map = map_seat_indices(&seat_counts);

        assert_eq!(seat_count(&seats_map, NodeId(1)), 6);
        assert_eq!(seat_count(&seats_map, NodeId(2)), 3);
        assert_eq!(seat_count(&seats_map, NodeId(3)), 1);
    }

    #[test]
    fn test_reassign() {
        let init_map: BTreeMap<NodeId, u16> = [
            (NodeId(3), 2),
            (NodeId(2), 2),
            (NodeId(1), 2),
            (NodeId(0), 2),
        ].into();

        let seats_map1 = map_seat_indices(&init_map);

        assert_eq!(seats(&seats_map1, NodeId(0)), vec![0, 1]);
        assert_eq!(seats(&seats_map1, NodeId(1)), vec![2, 3]);
        assert_eq!(seats(&seats_map1, NodeId(2)), vec![4, 5]);
        assert_eq!(seats(&seats_map1, NodeId(3)), vec![6, 7]);

        let target: BTreeMap<NodeId, u16> = [
            (NodeId(3), 4), 
            (NodeId(2), 4)
        ].into();

        let seats_map2 = move_seats(&seats_map1, &target);

        let s3 = seats(&seats_map2, NodeId(3));
        assert!(s3.contains(&6) && s3.contains(&7));
        assert!(s3.contains(&0) && s3.contains(&1));

        let s2 = seats(&seats_map2, NodeId(2));
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

        let seat_counts = assign_seats(&initial_stakes, 6);
        let initial_seat_map = map_seat_indices(&seat_counts);
        assert_eq!(total_count(&initial_seat_map), 6);

        let updated_stakes: BTreeMap<NodeId, u64> = [
            (NodeId(2), 2000),
            (NodeId(3), 3000),
        ].into();

        let seat_counts = assign_seats(&updated_stakes, 6);
        let updated_seat_map = move_seats(&initial_seat_map, &seat_counts);

        assert_eq!(total_count(&updated_seat_map), 6);
    }

    #[test]
    fn test_reassign_chain() {
        let n1 = NodeId(1);
        let n2 = NodeId(2);
        let n3 = NodeId(3);
        let n4 = NodeId(4);
        let n5 = NodeId(5);

        let seats_map1 = map_seat_indices(
            &[(n1, 2), (n2, 2), (n3, 2), (n4, 2), (n5, 2)].into()
        );
        assert_eq!(seats_map1.len(), 10);
        assert_eq!(seats(&seats_map1, n1), vec![0, 1]);
        assert_eq!(seats(&seats_map1, n2), vec![2, 3]);
        assert_eq!(seats(&seats_map1, n3), vec![4, 5]);
        assert_eq!(seats(&seats_map1, n4), vec![6, 7]);
        assert_eq!(seats(&seats_map1, n5), vec![8, 9]);

        let seats_map2 = move_seats(&seats_map1, &[(n1, 4), (n2, 3), (n3, 3)].into());
        let s1 = seats(&seats_map2, n1);
        let s2 = seats(&seats_map2, n2);
        let s3 = seats(&seats_map2, n3);
        assert!(s1.contains(&0) && s1.contains(&1));
        assert!(s2.contains(&2) && s2.contains(&3));
        assert!(s3.contains(&4) && s3.contains(&5));

        let seats_map3 = move_seats(&seats_map2, &[(n2, 3), (n3, 3), (n4, 2), (n5, 2)].into());
        assert_eq!(seats(&seats_map3, n2), s2);
        assert_eq!(seats(&seats_map3, n3), s3);
        assert_eq!(seats(&seats_map3, n4).len(), 2);
        assert_eq!(seats(&seats_map3, n5).len(), 2);

        let seats_map4 = move_seats(&seats_map3, &[(n1, 10)].into());
        let s = seats(&seats_map4, n1);
        for i in 0..10 {
            assert!(s.contains(&(i as u16)));
        }
    }

    #[test]
    fn test_many() {
        fn print_table_header() {
            println!(
                "{:<8} | {:>12} | {:>6} | {}",
                "NodeId", "Stake", "Seats", "SeatIds"
            );
            println!("{}", "-".repeat(8 + 3 + 12 + 3 + 6 + 3 + 40));
        }

        // Generate 100 nodes with stakes from 1000 to 100,000
        let initial_stakes: BTreeMap<NodeId, u64> = (1..=100)
            .map(|i| (NodeId(100 - i), i as u64 * 1000))
            .collect();

        let seat_counts = assign_seats(&initial_stakes, 1000);
        let initial_seat_map = map_seat_indices(&seat_counts);
        assert_eq!(total_count(&initial_seat_map), 1000);

        print_table_header();
        for node_id in initial_stakes.keys() {
            let stake = initial_stakes.get(node_id).unwrap_or(&0);
            let seats_for_node = seats(&initial_seat_map, *node_id);
            println!(
                "{:<8} | {:>12} | {:>6} | {:?}",
                format!("{:?}", node_id),
                stake,
                seats_for_node.len(),
                seats_for_node
            );
        }

        // Updated stakes: keep only nodes 51 to 100
        let updated_stakes: BTreeMap<NodeId, u64> = (51..=100)
            .map(|i| (NodeId(100 - i), i as u64 * 1000))
            .collect();

        let seat_counts = assign_seats(&updated_stakes, 1000);
        let updated_seat_map = move_seats(&initial_seat_map, &seat_counts);

        // Verify total seats and correct number of nodes
        assert_eq!(total_count(&updated_seat_map), 1000);
        let unique_nodes: HashSet<NodeId> = updated_seat_map.iter().cloned().collect();
        assert_eq!(unique_nodes.len(), 50, "Expected 50 nodes in updated_seat_map");

        // Verify seat counts match target_counts
        for (node_id, &count) in &seat_counts {
            assert_eq!(
                seat_count(&updated_seat_map, *node_id),
                count as usize,
                "seat count mismatch for node {:?}", node_id
            );
        }

        // Verify nodes not in updated_stakes have no seats
        for node_id in initial_stakes.keys() {
            if !updated_stakes.contains_key(node_id) {
                assert_eq!(
                    seat_count(&updated_seat_map, *node_id),
                    0,
                    "Node {:?} should have no seats", node_id
                );
            }
        }

        // Print updated seat map
        println!("\nAfter reassignment:");
        print_table_header();
        for node_id in updated_stakes.keys() {
            let stake = updated_stakes.get(node_id).unwrap_or(&0);
            let seats_for_node = seats(&updated_seat_map, *node_id);
            println!(
                "{:<8} | {:>12} | {:>6} | {:?}",
                format!("{:?}", node_id),
                stake,
                seats_for_node.len(),
                seats_for_node
            );
        }
    }
}
