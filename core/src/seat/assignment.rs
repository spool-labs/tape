use crate::types::NodeId;

pub type SeatMapping = u8;
pub type SeatIndex = u16;
pub type SeatCount = u16;
pub type Member = NodeId;

const REMOVED: u8 = u8::MAX;
const MEMBER_LIMIT: usize = u8::MAX as usize;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SeatAssignmentError {
    CountMismatch,
    MemberLimit,
    TotalMismatch,
    BalanceMismatch,
    InsufficientFree,
    BadIndex,
    NotNext,
}

/// Reassign seats from current members to next members with minimal disruption.
pub fn reassign_seats(
    current_seats: &[SeatMapping],
    current_members: &[Member],
    next_members: &[Member],
    next_seat_counts: &[SeatCount],
) -> Result<Vec<SeatMapping>, SeatAssignmentError> {
    if current_members.len() >= MEMBER_LIMIT {
        return Err(SeatAssignmentError::MemberLimit);
    }

    // Merge current and next members into a unique set, and adjust the next_seat_counts to be
    // relative to that unique set.
    let (unique_set, target_counts) = 
        get_union_set(
            current_members, 
            next_members, 
            next_seat_counts
        )?;

    // Verify total required seats equals number of current seats
    let total_required: usize = target_counts
        .iter()
        .map(|&x| x as usize)
        .sum();

    if total_required != current_seats.len() {
        return Err(SeatAssignmentError::TotalMismatch);
    }

    // Free current seats based on target counts in the unique set.
    let (free, remaining) = free_seats(current_seats, &target_counts);

    // Ensure freed seats exactly match the remaining demand
    let needed: usize = remaining
        .iter()
        .map(|&x| x as usize)
        .sum();

    if free.len() != needed {
        return Err(SeatAssignmentError::BalanceMismatch);
    }

    // Assign free seats to members still needing seats
    let result = assign_seats(current_seats, &free, &remaining)?;

    // Remap seats into the the right index space
    remap_index_space(&result, &unique_set)
}

/// Create a union set of (current + next members), flagging those that are removed. 
/// Then adjust the next_seat_counts to be relative to that union set.
fn get_union_set(
    current_members: &[Member],
    next_members: &[Member],
    next_seat_counts: &[SeatCount],
) -> Result<(Vec<SeatMapping>, Vec<SeatCount>), SeatAssignmentError> {

    if next_members.len() != next_seat_counts.len() {
        return Err(SeatAssignmentError::CountMismatch);
    }

    let mut members = vec![REMOVED; current_members.len()];
    let mut seats = vec![0; current_members.len()];

    // Start with existing current_members [0..current_members.len())
    // Append new members as needed
    for (next_index, &id) in next_members.iter().enumerate() {
        let unique_index = match find_member(current_members, id) {
            Some(idx) => idx,
            None => {
                members.push(REMOVED);
                seats.push(0);
                members.len() - 1
            }
        };
        members[unique_index] = next_index as u8;
        seats[unique_index] = next_seat_counts[next_index];
    }

    if members.len() >= MEMBER_LIMIT {
        return Err(SeatAssignmentError::MemberLimit);
    }

    Ok((members, seats))
}

/// Free seats that are no longer needed, and decrement remaining demand for retained seats.
/// Keep seats with their current owner if possible; otherwise free them and reassign.
fn free_seats(
    seats: &[SeatMapping],
    required_counts: &[SeatCount],
) -> (Vec<SeatIndex>, Vec<SeatCount>) {
    let mut freed = Vec::new();
    let mut remaining = required_counts.to_vec();

    // For each current seat
    for seat_index in 0..seats.len() {
        let owner = seats[seat_index] as usize;

        // If the owner still needs seats, retain it.
        if owner < remaining.len() && remaining[owner] > 0 {
            remaining[owner] -= 1;

        // Otherwise, free it for reassignment.
        } else {
            freed.push(seat_index as SeatIndex);
        }
    }

    (freed, remaining)
}

/// Assign freed seats to nodes still needing seats.
fn assign_seats(
    current_seats: &[SeatMapping],
    free_seats: &[SeatIndex],
    required_counts: &[SeatCount],
) -> Result<Vec<SeatMapping>, SeatAssignmentError> {

    let mut result = current_seats.to_vec();
    let mut free_seats = free_seats.to_vec();

    let total: usize = required_counts.iter().map(|&x| x as usize).sum();

    if total > free_seats.len() {
        return Err(SeatAssignmentError::InsufficientFree);
    }

    // For each member with unallocated seats, assign from freed seats.
    for member_index in 0..required_counts.len() {
        // Number of seats still needed for this member.
        let count = required_counts[member_index] as usize;
        for _ in 0..count {
            let seat_index = free_seats
                .pop()
                .expect("logic error: validated above that freed seats are sufficient") as usize;

            result[seat_index] = member_index as u8;
        }
    }

    Ok(result)
}

/// Remap from union set indices to next member indices.
fn remap_index_space(
    seat_map: &[SeatMapping],
    union_set: &[SeatMapping],
) -> Result<Vec<u8>, SeatAssignmentError> {
    let seat_count = seat_map.len();
    let mut result = Vec::with_capacity(seat_count);

    for seat_index in 0..seat_count {
        let union_index = seat_map[seat_index] as usize;
        if union_index >= union_set.len() {
            return Err(SeatAssignmentError::BadIndex);
        }
        let next_index = union_set[union_index];

        if next_index == u8::MAX {
            return Err(SeatAssignmentError::NotNext);
        }

        result.push(next_index);
    }

    Ok(result)
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

/// Helper to create an initial seat map from seat counts, assigning seats contiguously.
pub fn to_seat_map(
    seat_counts: &[SeatCount],
) -> Vec<SeatMapping> {
    let total: usize = seat_counts.iter().map(|&c| c as usize).sum();

    let mut result = vec![0u8; total];
    let mut pos = 0usize;

    for (i, &c) in seat_counts.iter().enumerate() {
        for _ in 0..c {
            result[pos] = i as u8;
            pos += 1;
        }
    }

    result
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

    fn out_to_nodes(out: &[u8], next: &[NodeId]) -> Vec<NodeId> {
        out.iter().map(|&i| next[i as usize]).collect()
    }

    fn leaders_from_stakes(stake_map: &BTreeMap<NodeId, u64>) -> Vec<NodeId> {
        stake_map.keys().cloned().collect() // BTreeMap keeps ascending NodeId
    }

    fn dhondt_counts_for(next: &[NodeId], stake_map: &BTreeMap<NodeId, u64>, seats: u16) -> Vec<u16> {
        let stakes: Vec<_> = next.iter().map(|id| stake_map[id].into()).collect();
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

    pub fn members(ids: impl IntoIterator<Item=u64>) -> Vec<NodeId> {
        ids.into_iter().map(NodeId).collect()
    }

    #[test]
    fn test_seat_map() {
        assert_eq!(to_seat_map(&[0]), &[]);
        assert_eq!(to_seat_map(&[0, 0, 0]), &[]);
        assert_eq!(to_seat_map(&[1, 2, 3]), &[0, 1, 1, 2, 2, 2]);
        assert_eq!(to_seat_map(&[3, 0, 2]), &[0, 0, 0, 2, 2]);
        assert_eq!(to_seat_map(&[1, 1, 1, 1]), &[0, 1, 2, 3]);
        assert_eq!(to_seat_map(&[5]), &[0, 0, 0, 0, 0]);
        assert_eq!(to_seat_map(&[1, 0, 0, 1]), &[0, 3]);
        assert_eq!(to_seat_map(&[2, 0, 1, 0, 3]), &[0, 0, 2, 4, 4, 4]);
    }

    #[test]
    fn test_union_simple() {
        let current = members([1, 3, 2]);
        let next = members([3, 4, 5]);
        let counts = vec![3, 2, 5];

        let (members, seats) = get_union_set(&current, &next, &counts).unwrap();

        assert_eq!(members, [REMOVED, 0, REMOVED, 1, 2]);
        assert_eq!(seats, [0, 3, 0, 2, 5]);
    }

    #[test]
    fn test_union_same() {
        let current = members([1, 2, 3]);
        let next = members([1, 2, 3]);
        let counts = vec![3, 2, 5];
        let (members, seats) = get_union_set(&current, &next, &counts).unwrap();
        assert_eq!(members, [0, 1, 2]);
        assert_eq!(seats, [3, 2, 5]);
    }

    #[test]
    fn test_union_replaced() {
        let current = members([1, 2, 3]);
        let next = members([4, 5, 6]);
        let counts = vec![3, 2, 5];
        let (members, seats) = get_union_set(&current, &next, &counts).unwrap();
        assert_eq!(members, [REMOVED, REMOVED, REMOVED, 0, 1, 2]);
        assert_eq!(seats, [0, 0, 0, 3, 2, 5]);
    }

    #[test]
    fn test_union_reverse() {
        let current = members([1, 2, 3, 4, 5]);
        let next = members([5, 4, 3, 2, 1]);
        let counts = vec![1, 1, 1, 1, 1];
        let (members, seats) = get_union_set(&current, &next, &counts).unwrap();
        assert_eq!(members, [4, 3, 2, 1, 0]);
        assert_eq!(seats, [1, 1, 1, 1, 1]);
    }

    #[test]
    fn test_free_seats() {
        let current = vec![0, 0, 1, 1, 2, 2];
        let required = vec![2, 2, 2];
        let (free, remaining) = free_seats(&current, &required);
        assert_eq!(free, vec![]);
        assert_eq!(remaining, vec![0, 0, 0]);
    }

    #[test]
    fn test_free_reduce() {
        let current = vec![0, 0, 1, 1, 2, 2];
        let required = vec![1, 1, 1];
        let (free, remaining) = free_seats(&current, &required);
        assert_eq!(free.len(), 3);
        assert_eq!(remaining, vec![0, 0, 0]);
    }

    #[test]
    fn test_free_increase() {
        let current = vec![0, 0, 1, 1, 2, 2];
        let required = vec![3, 2, 2];
        let (free, remaining) = free_seats(&current, &required);
        assert_eq!(free, vec![]);
        assert_eq!(remaining, vec![1, 0, 0]);
    }

    #[test]
    fn test_free_all() {
        let current = vec![0, 0, 1, 1, 2, 2];
        let required = vec![0, 0, 0];
        let (free, remaining) = free_seats(&current, &required);
        assert_eq!(free.len(), 6);
        assert_eq!(remaining, vec![0, 0, 0]);
    }

    #[test]
    fn test_free_some() {
        let current = vec![0, 0, 1, 1, 2, 2];
        let required = vec![1, 0, 1];
        let (free, remaining) = free_seats(&current, &required);
        assert_eq!(free.len(), 4);
        assert_eq!(remaining, vec![0, 0, 0]);
    }

    #[test]
    fn test_free_excess() {
        let current = vec![0, 0, 1, 1, 2, 2];
        let required = vec![4, 3, 2];
        let (free, remaining) = free_seats(&current, &required);
        assert_eq!(free.len(), 0);
        assert_eq!(remaining, vec![2, 1, 0]);
    }

    #[test]
    fn test_assign_seats() {
        let current = vec![0, 0, 1, 1, 2, 2];
        let free = vec![2, 3, 4];
        let required = vec![1, 1, 1];

        let out = assign_seats(&current, &free, &required).unwrap();
        assert_eq!(out, vec![0, 0, 2, 1, 0, 2]);
    }

    #[test]
    fn test_assign_seats_all() {
        let current = vec![5, 5, 6, 7, 9, 8];
        let free = vec![0, 1, 2, 3, 4, 5]; // all seats freed
        let required = vec![2, 2, 2];

        let out = assign_seats(&current, &free, &required).unwrap();
        assert_eq!(out, vec![2, 2, 1, 1, 0, 0]);
    }

    #[test]
    fn test_assign_seats_some() {
        let current = vec![0, 1, 2, 3, 4, 5];
        let free = vec![3, 4, 5];
        let required = vec![1, 1, 0, 0, 0, 0];

        let out = assign_seats(&current, &free, &required).unwrap();
        assert_eq!(out, vec![0, 1, 2, 3, 1, 0]);
    }

    #[test]
    fn test_single() {
        let current = vec![NodeId(42)];
        let next = vec![NodeId(42)];
        let counts = vec![10 as u16];

        // Start with all seats owned by the single current member
        let seats = to_seat_map(&counts);

        let out = reassign_seats(&seats, &current, &next, &counts).unwrap();
        let node_seats = out_to_nodes(&out, &next);

        assert_eq!(total_count(&node_seats), 10);
        assert_eq!(seat_count(&node_seats, NodeId(42)), 10);
    }

    #[test]
    fn test_equal() {
        const SEATS: usize = 10;

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
        let seats = vec![0u8; SEATS];

        let out = reassign_seats(&seats, &current, &next, &counts).unwrap();
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

        let stake_map: BTreeMap<NodeId, u64> = [
            (NodeId(1), 1000),
            (NodeId(2), 1000),
            (NodeId(3), 1000),
        ].into();

        let next = leaders_from_stakes(&stake_map);
        let counts = dhondt_counts_for(&next, &stake_map, SEATS as u16);

        let current = next.clone();
        let seats = vec![0u8; SEATS];

        let out = reassign_seats(&seats, &current, &next, &counts).unwrap();
        let node_seats = out_to_nodes(&out, &next);

        assert_eq!(seat_count(&node_seats, NodeId(1)), 2);
        assert_eq!(seat_count(&node_seats, NodeId(2)), 2);
        assert_eq!(seat_count(&node_seats, NodeId(3)), 2);
    }

    #[test]
    fn test_uneven() {
        const SEATS: usize = 10;

        let stake_map: BTreeMap<NodeId, u64> = [
            (NodeId(1), 4000),
            (NodeId(2), 2000),
            (NodeId(3), 1000),
        ].into();

        let next = leaders_from_stakes(&stake_map);
        let counts = dhondt_counts_for(&next, &stake_map, SEATS as u16);

        let current = next.clone();
        let seats = vec![0u8; SEATS];

        let out = reassign_seats(&seats, &current, &next, &counts).unwrap();
        let node_seats = out_to_nodes(&out, &next);

        assert_eq!(seat_count(&node_seats, NodeId(1)), 6);
        assert_eq!(seat_count(&node_seats, NodeId(2)), 3);
        assert_eq!(seat_count(&node_seats, NodeId(3)), 1);
    }

    #[test]
    fn test_reassign() {
        let n0 = NodeId(0);
        let n1 = NodeId(1);
        let n2 = NodeId(2);
        let n3 = NodeId(3);

        let current = vec![n0, n1, n2, n3];

        // Initial: each has 2 contiguous seats: [0,1]=n0, [2,3]=n1, [4,5]=n2, [6,7]=n3
        let initial_counts = vec![2, 2, 2, 2];
        let seats = to_seat_map(&initial_counts);

        // Target: n3:4, n2:4
        let next = vec![n3, n2];
        let counts = vec![4, 4];

        let out = reassign_seats(&seats, &current, &next, &counts).unwrap();
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

        // Initial stakes: nodes 1,2,3 with 1k,2k,3k
        let initial_stakes: BTreeMap<NodeId, u64> = [
            (NodeId(1), 1000),
            (NodeId(2), 2000),
            (NodeId(3), 3000),
        ].into();

        let leaders1 = leaders_from_stakes(&initial_stakes);
        let counts1 = dhondt_counts_for(&leaders1, &initial_stakes, SEATS as u16);

        // Build initial seats contiguously
        let seats1 = to_seat_map(&counts1);

        // Shift to itself (no-op, but validates path)
        let out1 = reassign_seats(&seats1, &leaders1, &leaders1, &counts1).unwrap();
        assert_eq!(out1.len(), SEATS);

        // Updated stakes: only nodes 2 and 3 remain
        let updated_stakes: BTreeMap<NodeId, u64> = [
            (NodeId(2), 2000),
            (NodeId(3), 3000),
        ].into();

        let leaders2 = leaders_from_stakes(&updated_stakes);
        let counts2 = dhondt_counts_for(&leaders2, &updated_stakes, SEATS as u16);

        // Reassign from previous seats/current to new next
        let out2 = reassign_seats(&out1, &leaders1, &leaders2, &counts2).unwrap();
        let node_seats = out_to_nodes(&out2, &leaders2);

        assert_eq!(total_count(&node_seats), SEATS);
        // Verify counts match target
        for (i, nid) in leaders2.iter().enumerate() {
            assert_eq!(seat_count(&node_seats, *nid) as u16, counts2[i]);
        }
    }

    #[test]
    fn test_reassign_chain() {
        let n1 = NodeId(1);
        let n2 = NodeId(2);
        let n3 = NodeId(3);
        let n4 = NodeId(4);
        let n5 = NodeId(5);

        let current = vec![n1, n2, n3, n4, n5];

        // Initial: each 2 contiguous seats
        let seats0 = to_seat_map(&[2, 2, 2, 2, 2]);

        // Step 1: next n1:4, n2:3, n3:3
        let leaders1 = vec![n1, n2, n3];
        let counts1 = vec![4, 3, 3];

        let out1 = reassign_seats(&seats0, &current, &leaders1, &counts1).unwrap();
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

        // Note: keeping 'current' as the current_members here matches the original test’s behavior
        // and preserves indices because leaders1 is a prefix of current in the same order.
        let out2 = reassign_seats(&out1, &current, &leaders2, &counts2).unwrap();
        let map2 = out_to_nodes(&out2, &leaders2);

        assert_eq!(seat_list(&map2, n2), s2);
        assert_eq!(seat_list(&map2, n3), s3);
        assert_eq!(seat_list(&map2, n4).len(), 2);
        assert_eq!(seat_list(&map2, n5).len(), 2);

        // Step 3: next n1:10
        let leaders3 = vec![n1];
        let counts3 = vec![10];

        let out3 = reassign_seats(&out2, &current, &leaders3, &counts3).unwrap();
        let map3 = out_to_nodes(&out3, &leaders3);

        let s = seat_list(&map3, n1);
        for i in 0..10 {
            assert!(s.contains(&(i as u16)));
        }
    }

    #[test]
    fn test_many() {
        const SEATS: usize = 1000;

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
        let seats1 = to_seat_map(&counts1);

        // No-op shift to validate base
        let out1 = reassign_seats(&seats1, &leaders1, &leaders1, &counts1).unwrap();
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
        let out2 = reassign_seats(&out1, &leaders1, &leaders2, &counts2).unwrap();
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
