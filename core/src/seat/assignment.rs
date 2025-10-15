use crate::types::NodeId;

/// Move seats according to new target counts while minimizing movement.
pub fn shift_seats<const SEATS: usize, const NODES: usize>(
    current_seats: &[u8; SEATS],
    current_members: &[NodeId],
    next_members: &[NodeId],
    next_seat_counts: &[u16],
) -> [u8; SEATS] {

    debug_assert!(NODES <= 256);
    debug_assert!(next_members.len() == next_seat_counts.len());
    debug_assert!(current_members.len() <= u8::MAX as usize);

    // First, we create a mapping of current + next members to a unique index space of 0..NODES.
    // map(u8) -> unique(current_members + next_members)
    // (This is an optimization to avoid using Map or HashMap, due to stack and compute limits.)

    // TODO: this could be a function on its own

    let mut unique_set = [u8::MAX; NODES]; 
    let mut unique_len = current_members.len();
    let mut targets = [0u16; NODES]; // target seat counts for the unique set

    // For each next member
    for next_index in 0..next_members.len() {
        // Get its NodeId
        let id = next_members[next_index];

        // Find the member index in the current set (if any)
        let unique_index = match find_member(current_members, id) {
            Some(index) => index,

            // If not found, we're adding a new member
            None => {
                let index = unique_len;
                unique_len += 1;
                debug_assert!(unique_len <= NODES, "capacity exceeded");
                index
            }
        };

        unique_set[unique_index] = next_index as u8;
        targets[unique_index] = next_seat_counts[next_index];
    }

    // Move seats to match targets while minimizing movement. Keep seats with their current owner
    // if possible; otherwise free them and reassign.

    let mut seat_map = *current_seats;
    let mut remaining = targets;
    let mut to_move: Vec<u16> = Vec::new();

    // Keep seats when owner still needs them; otherwise free them.
    // TODO: this could be a function on its own

    for seat_index in 0..SEATS {
        let index = current_seats[seat_index] as usize;
        if remaining[index] > 0 {
            remaining[index] -= 1;
        } else {
            to_move.push(seat_index as u16);
        }
    }

    debug_assert!(
        to_move.len() == remaining.iter().map(|&x| x as usize).sum::<usize>(),
        "Mismatch between freed seats and remaining demand"
    );

    // Assign freed seats to nodes still needing seats.
    // TODO: this could be a function on its own

    for member_index in 0..NODES {
        let num_seats = remaining[member_index] as usize;
        for _ in 0..num_seats {
            let seat_index = to_move
                .pop()
                .expect("Not enough freed seats to reassign") as usize;

            seat_map[seat_index] = member_index as u8;
        }
    }

    debug_assert!(to_move.is_empty(), "Unassigned seats remain after reassignment");

    // Map from the unique index space back to next indices.
    // map(unique(current_members + next_members)) -> map(next_members)
    // (Again, this is an optimization to avoid using Map or HashMap)

    // TODO: this could be a function on its own

    let mut result = [0u8; SEATS];
    for seat_index in 0..SEATS {
        let unique_index = seat_map[seat_index] as usize;
        let next_index = unique_set[unique_index];
        debug_assert!(next_index != u8::MAX, "seat mapped to non-next member");
        result[seat_index] = next_index;
    }

    result
}

/// Linear search of member id in current member list.
fn find_member(members: &[NodeId], id: NodeId) -> Option<usize> {
    for i in 0..members.len() {
        if members[i] == id {
            return Some(i);
        }
    }
    None
}

fn seat_map<const SEATS: usize>(
    seat_counts: &[u16],
) -> [u8; SEATS] {
    let total: usize = seat_counts.iter().map(|&c| c as usize).sum();
    assert_eq!(total, SEATS);

    let mut out = [0u8; SEATS];
    let mut pos = 0usize;

    for (i, &c) in seat_counts.iter().enumerate() {
        for _ in 0..c {
            out[pos] = i as u8;
            pos += 1;
        }
    }

    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{BTreeMap, HashSet};
    use crate::seat::dhondt::allocate_seats;
    use crate::types::*;

    // Common helpers

    fn total_count(seat_to_node: &[NodeId]) -> usize {
        seat_to_node.len()
    }

    fn seat_count(seat_to_node: &[NodeId], node_id: NodeId) -> usize {
        seat_to_node.iter().filter(|&&n| n == node_id).count()
    }

    fn seat_list(seat_to_node: &[NodeId], node_id: NodeId) -> Vec<u16> {
        seat_to_node
            .iter()
            .enumerate()
            .filter_map(|(seat_id, &n)| if n == node_id { Some(seat_id as u16) } else { None })
            .collect()
    }

    fn out_to_nodes<const SEATS: usize>(out: &[u8; SEATS], next: &[NodeId]) -> Vec<NodeId> {
        out.iter().map(|&i| next[i as usize]).collect()
    }

    fn leaders_from_stakes(stake_map: &BTreeMap<NodeId, u64>) -> Vec<NodeId> {
        stake_map.keys().cloned().collect() // BTreeMap keeps ascending NodeId
    }

    fn dhondt_counts_for(next: &[NodeId], stake_map: &BTreeMap<NodeId, u64>, seats: u16) -> Vec<u16> {
        let stakes: Vec<Coin<TAPE>> = next.iter().map(|id| stake_map[id].into()).collect();
        allocate_seats(&stakes, seats)
    }

    fn counts_map(next: &[NodeId], counts: &[u16]) -> BTreeMap<NodeId, u16> {
        let mut m = BTreeMap::new();
        for (i, id) in next.iter().enumerate() {
            if counts[i] > 0 {
                m.insert(*id, counts[i]);
            }
        }
        m
    }


    #[test]
    fn test_single() {
        const SEATS: usize = 10;
        const NODES: usize = 256;

        let current = vec![NodeId(10)];
        let next = vec![NodeId(10)];
        let counts = vec![SEATS as u16];

        // Start with all seats owned by the single current member
        let seats = [0u8; SEATS];

        let out = shift_seats::<SEATS, NODES>(&seats, &current, &next, &counts);
        let node_seats = out_to_nodes(&out, &next);

        assert_eq!(total_count(&node_seats), 10);
        assert_eq!(seat_count(&node_seats, NodeId(10)), 10);
    }

    #[test]
    fn test_equal() {
        const SEATS: usize = 10;
        const NODES: usize = 256;

        let stake_map: BTreeMap<NodeId, u64> = [
            (NodeId(1), 1000),
            (NodeId(2), 1000),
            (NodeId(3), 1000),
        ].into();

        let next = leaders_from_stakes(&stake_map);
        let counts = dhondt_counts_for(&next, &stake_map, SEATS as u16);
        assert_eq!(counts.iter().copied().sum::<u16>(), SEATS as u16);

        // Arbitrary initial: all seats assigned to the first current member
        let current = next.clone();
        let seats = [0u8; SEATS];

        let out = shift_seats::<SEATS, NODES>(&seats, &current, &next, &counts);
        let node_seats = out_to_nodes(&out, &next);

        let v: Vec<u16> = [NodeId(1), NodeId(2), NodeId(3)]
            .iter()
            .map(|nid| seat_count(&node_seats, *nid) as u16)
            .collect();

        assert_eq!(v, vec![4, 3, 3]);
    }

    #[test]
    fn test_even() {
        const SEATS: usize = 6;
        const NODES: usize = 256;

        let stake_map: BTreeMap<NodeId, u64> = [
            (NodeId(1), 1000),
            (NodeId(2), 1000),
            (NodeId(3), 1000),
        ].into();

        let next = leaders_from_stakes(&stake_map);
        let counts = dhondt_counts_for(&next, &stake_map, SEATS as u16);

        let current = next.clone();
        let seats = [0u8; SEATS];

        let out = shift_seats::<SEATS, NODES>(&seats, &current, &next, &counts);
        let node_seats = out_to_nodes(&out, &next);

        assert_eq!(seat_count(&node_seats, NodeId(1)), 2);
        assert_eq!(seat_count(&node_seats, NodeId(2)), 2);
        assert_eq!(seat_count(&node_seats, NodeId(3)), 2);
    }

    #[test]
    fn test_uneven() {
        const SEATS: usize = 10;
        const NODES: usize = 256;

        let stake_map: BTreeMap<NodeId, u64> = [
            (NodeId(1), 4000),
            (NodeId(2), 2000),
            (NodeId(3), 1000),
        ].into();

        let next = leaders_from_stakes(&stake_map);
        let counts = dhondt_counts_for(&next, &stake_map, SEATS as u16);

        let current = next.clone();
        let seats = [0u8; SEATS];

        let out = shift_seats::<SEATS, NODES>(&seats, &current, &next, &counts);
        let node_seats = out_to_nodes(&out, &next);

        assert_eq!(seat_count(&node_seats, NodeId(1)), 6);
        assert_eq!(seat_count(&node_seats, NodeId(2)), 3);
        assert_eq!(seat_count(&node_seats, NodeId(3)), 1);
    }

    #[test]
    fn test_reassign() {
        const SEATS: usize = 8;
        const NODES: usize = 256;

        let n0 = NodeId(0);
        let n1 = NodeId(1);
        let n2 = NodeId(2);
        let n3 = NodeId(3);

        let current = vec![n0, n1, n2, n3];

        // Initial: each has 2 contiguous seats: [0,1]=n0, [2,3]=n1, [4,5]=n2, [6,7]=n3
        let initial_counts = vec![2, 2, 2, 2];
        let seats = seat_map::<SEATS>(&initial_counts);

        println!("Initial seats: {:?}", seats);

        // Target: n3:4, n2:4
        let next = vec![n3, n2];
        let counts = vec![4, 4];

        let out = shift_seats::<SEATS, NODES>(&seats, &current, &next, &counts);
        let node_seats = out_to_nodes(&out, &next);

        let s3 = seat_list(&node_seats, n3);
        assert!(s3.contains(&6) && s3.contains(&7));
        assert!(s3.contains(&0) && s3.contains(&1));

        let s2 = seat_list(&node_seats, n2);
        assert!(s2.contains(&4) && s2.contains(&5));
        assert!(s2.contains(&2) && s2.contains(&3));
    }

    #[test]
    fn test_reassign_reduce() {
        const SEATS: usize = 6;
        const NODES: usize = 256;

        // Initial stakes: nodes 1,2,3 with 1k,2k,3k
        let initial_stakes: BTreeMap<NodeId, u64> = [
            (NodeId(1), 1000),
            (NodeId(2), 2000),
            (NodeId(3), 3000),
        ].into();

        let leaders1 = leaders_from_stakes(&initial_stakes);
        let counts1 = dhondt_counts_for(&leaders1, &initial_stakes, SEATS as u16);

        // Build initial seats contiguously
        let seats1 = seat_map::<SEATS>(&counts1);

        // Shift to itself (no-op, but validates path)
        let out1 = shift_seats::<SEATS, NODES>(&seats1, &leaders1, &leaders1, &counts1);

        assert_eq!(out1.len(), SEATS);

        // Updated stakes: only nodes 2 and 3 remain
        let updated_stakes: BTreeMap<NodeId, u64> = [
            (NodeId(2), 2000),
            (NodeId(3), 3000),
        ].into();

        let leaders2 = leaders_from_stakes(&updated_stakes);
        let counts2 = dhondt_counts_for(&leaders2, &updated_stakes, SEATS as u16);

        // Reassign from previous seats/current to new next
        let out2 = shift_seats::<SEATS, NODES>(&out1, &leaders1, &leaders2, &counts2);
        let node_seats = out_to_nodes(&out2, &leaders2);

        assert_eq!(total_count(&node_seats), SEATS);
        // Verify counts match target
        for (i, nid) in leaders2.iter().enumerate() {
            assert_eq!(seat_count(&node_seats, *nid) as u16, counts2[i]);
        }
    }

    #[test]
    fn test_reassign_chain() {
        const SEATS: usize = 10;
        const NODES: usize = 256;

        let n1 = NodeId(1);
        let n2 = NodeId(2);
        let n3 = NodeId(3);
        let n4 = NodeId(4);
        let n5 = NodeId(5);

        let current = vec![n1, n2, n3, n4, n5];

        // Initial: each 2 contiguous seats
        let seats0 = seat_map::<SEATS>(&[2, 2, 2, 2, 2]);

        // Step 1: next n1:4, n2:3, n3:3
        let leaders1 = vec![n1, n2, n3];
        let counts1 = vec![4, 3, 3];

        let out1 = shift_seats::<SEATS, NODES>(&seats0, &current, &leaders1, &counts1);
        let map1 = out_to_nodes(&out1, &leaders1);

        assert_eq!(map1.len(), 10);
        let s1 = seat_list(&map1, n1);
        let s2 = seat_list(&map1, n2);
        let s3 = seat_list(&map1, n3);

        // Original seats should still be included
        assert!(s1.contains(&0) && s1.contains(&1));
        assert!(s2.contains(&2) && s2.contains(&3));
        assert!(s3.contains(&4) && s3.contains(&5));

        // Step 2: next n2:3, n3:3, n4:2, n5:2
        let leaders2 = vec![n2, n3, n4, n5];
        let counts2 = vec![3, 3, 2, 2];

        let out2 = shift_seats::<SEATS, NODES>(&out1, &current, &leaders2, &counts2);
        let map2 = out_to_nodes(&out2, &leaders2);

        assert_eq!(seat_list(&map2, n2), s2);
        assert_eq!(seat_list(&map2, n3), s3);
        assert_eq!(seat_list(&map2, n4).len(), 2);
        assert_eq!(seat_list(&map2, n5).len(), 2);

        // Step 3: next n1:10
        let leaders3 = vec![n1];
        let counts3 = vec![10];

        let out3 = shift_seats::<SEATS, NODES>(&out2, &current, &leaders3, &counts3);
        let map3 = out_to_nodes(&out3, &leaders3);

        let s = seat_list(&map3, n1);
        for i in 0..10 {
            assert!(s.contains(&(i as u16)));
        }
    }

    #[test]
    fn test_many() {
        const SEATS: usize = 1000;
        const NODES: usize = 256;

        fn print_table_header() {
            println!(
                "{:<8} | {:>12} | {:>6} | {}",
                "NodeId", "Stake", "Seats", "SeatIds"
            );
            println!("{}", "-".repeat(8 + 3 + 12 + 3 + 6 + 3 + 40));
        }

        // Generate 100 nodes with stakes from 1000 to 100,000 (descending NodeId keys)
        let initial_stakes: BTreeMap<NodeId, u64> = (1..=100)
            .map(|i| (NodeId(100 - i), i as u64 * 1000))
            .collect();

        let leaders1 = initial_stakes.keys().cloned().collect::<Vec<_>>();
        let counts1 = dhondt_counts_for(&leaders1, &initial_stakes, SEATS as u16);
        let seats1 = seat_map::<SEATS>(&counts1);

        // No-op shift to validate base
        let out1 = shift_seats::<SEATS, NODES>(&seats1, &leaders1, &leaders1, &counts1);
        let map1 = out_to_nodes(&out1, &leaders1);
        assert_eq!(total_count(&map1), SEATS);

        print_table_header();
        for node_id in leaders1.iter() {
            let stake = initial_stakes.get(node_id).unwrap_or(&0);
            let seats_for_node = seat_list(&map1, *node_id);
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

        let leaders2 = updated_stakes.keys().cloned().collect::<Vec<_>>();
        let counts2 = dhondt_counts_for(&leaders2, &updated_stakes, SEATS as u16);

        // Reassign from previous seats/current to new next
        let out2 = shift_seats::<SEATS, NODES>(&out1, &leaders1, &leaders2, &counts2);
        let map2 = out_to_nodes(&out2, &leaders2);

        // Verify total seats and correct number of nodes
        assert_eq!(total_count(&map2), SEATS);
        let unique_nodes: HashSet<NodeId> = map2.iter().cloned().collect();
        assert_eq!(unique_nodes.len(), 50, "Expected 50 nodes in updated seat map");

        // Verify seat counts match target_counts
        let seat_counts_map = counts_map(&leaders2, &counts2);
        for (node_id, &count) in &seat_counts_map {
            assert_eq!(
                seat_count(&map2, *node_id),
                count as usize,
                "seat count mismatch for node {:?}", node_id
            );
        }

        // Verify nodes not in updated_stakes have no seats
        for node_id in initial_stakes.keys() {
            if !updated_stakes.contains_key(node_id) {
                assert_eq!(
                    seat_count(&map2, *node_id),
                    0,
                    "Node {:?} should have no seats", node_id
                );
            }
        }

        // Print updated seat map
        println!("\nAfter reassignment:");
        print_table_header();
        for node_id in leaders2.iter() {
            let stake = updated_stakes.get(node_id).unwrap_or(&0);
            let seats_for_node = seat_list(&map2, *node_id);
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
