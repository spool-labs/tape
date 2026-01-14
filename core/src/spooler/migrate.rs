use crate::types::NodeId;
use super::{SpoolCount, SpoolIndex, SpoolMapping, SpoolerError};

pub const REMOVED: u8 = u8::MAX;
const MEMBER_LIMIT: usize = u8::MAX as usize;

/// Reassign spools from current members to next members with minimal disruption.
pub fn migrate_spools(
    current_spools: &[SpoolMapping],
    current_members: &[NodeId],
    next_members: &[NodeId],
    next_spool_counts: &[SpoolCount],
) -> Result<Vec<SpoolMapping>, SpoolerError> {
    if current_members.len() >= MEMBER_LIMIT {
        return Err(SpoolerError::MemberLimit);
    }

    let (unique_set, target_counts) = get_union_set(current_members, next_members, next_spool_counts)?;

    let total_required: usize = target_counts.iter().map(|&x| x as usize).sum();
    if total_required != current_spools.len() {
        return Err(SpoolerError::TotalMismatch);
    }

    let (free, remaining) = free_spools(current_spools, &target_counts);

    let needed: usize = remaining.iter().map(|&x| x as usize).sum();
    if free.len() != needed {
        return Err(SpoolerError::BalanceMismatch);
    }

    let result = assign_spools(current_spools, &free, &remaining)?;
    remap_index_space(&result, &unique_set)
}

/// Create a union set of (current + next members), flagging those that are removed.
/// Then adjust the next_spool_counts to be relative to that union set.
pub fn get_union_set(
    current_members: &[NodeId],
    next_members: &[NodeId],
    next_spool_counts: &[SpoolCount],
) -> Result<(Vec<SpoolMapping>, Vec<SpoolCount>), SpoolerError> {
    if next_members.len() != next_spool_counts.len() {
        return Err(SpoolerError::CountMismatch);
    }

    let mut members = vec![REMOVED; current_members.len()];
    let mut spools = vec![0; current_members.len()];

    for (next_index, &id) in next_members.iter().enumerate() {
        let unique_index = match find_member(current_members, id) {
            Some(idx) => idx,
            None => {
                members.push(REMOVED);
                spools.push(0);
                members.len() - 1
            }
        };
        members[unique_index] = next_index as u8;
        spools[unique_index] = next_spool_counts[next_index];
    }

    if members.len() >= MEMBER_LIMIT {
        return Err(SpoolerError::MemberLimit);
    }

    Ok((members, spools))
}

fn free_spools(
    spools: &[SpoolMapping],
    required_counts: &[SpoolCount],
) -> (Vec<SpoolIndex>, Vec<SpoolCount>) {
    let mut freed = Vec::new();
    let mut remaining = required_counts.to_vec();

    for spool_index in 0..spools.len() {
        let owner = spools[spool_index] as usize;
        if owner < remaining.len() && remaining[owner] > 0 {
            remaining[owner] -= 1;
        } else {
            freed.push(spool_index as SpoolIndex);
        }
    }

    (freed, remaining)
}

pub fn assign_spools(
    current_spools: &[SpoolMapping],
    free_spools: &[SpoolIndex],
    required_counts: &[SpoolCount],
) -> Result<Vec<SpoolMapping>, SpoolerError> {
    let mut result = current_spools.to_vec();
    let mut free_spools = free_spools.to_vec();

    let total: usize = required_counts.iter().map(|&x| x as usize).sum();
    if total > free_spools.len() {
        return Err(SpoolerError::InsufficientFree);
    }

    for member_index in 0..required_counts.len() {
        let count = required_counts[member_index] as usize;
        for _ in 0..count {
            let spool_index = free_spools.pop().expect("validated sufficient") as usize;
            result[spool_index] = member_index as u8;
        }
    }

    Ok(result)
}

pub fn remap_index_space(
    spool_map: &[SpoolMapping],
    union_set: &[SpoolMapping],
) -> Result<Vec<u8>, SpoolerError> {
    let spool_count = spool_map.len();
    let mut result = Vec::with_capacity(spool_count);

    for spool_index in 0..spool_count {
        let union_index = spool_map[spool_index] as usize;
        if union_index >= union_set.len() {
            return Err(SpoolerError::BadIndex);
        }
        let next_index = union_set[union_index];

        if next_index == u8::MAX {
            return Err(SpoolerError::NotNext);
        }

        result.push(next_index);
    }

    Ok(result)
}

pub fn find_member(members: &[NodeId], id: NodeId) -> Option<usize> {
    for i in 0..members.len() {
        if members[i] == id {
            return Some(i);
        }
    }
    None
}

pub fn to_spool_map(spool_counts: &[SpoolCount]) -> Vec<SpoolMapping> {
    let total: usize = spool_counts.iter().map(|&c| c as usize).sum();
    let mut result = vec![0u8; total];
    let mut pos = 0usize;

    for (i, &c) in spool_counts.iter().enumerate() {
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
    use crate::spooler::dhondt::DhondtSpooler;
    use crate::spooler::Spooler;
    use crate::types::*;

    fn total_count(spool_to_node: &[NodeId]) -> usize {
        spool_to_node.len()
    }

    fn spool_count(spool_to_node: &[NodeId], node_id: NodeId) -> usize {
        spool_to_node.iter().filter(|&&n| n == node_id).count()
    }

    fn spool_list(spool_to_node: &[NodeId], node_id: NodeId) -> Vec<u16> {
        spool_to_node
            .iter()
            .enumerate()
            .filter_map(|(spool_id, &n)| if n == node_id { Some(spool_id as u16) } else { None })
            .collect()
    }

    fn out_to_nodes(out: &[u8], next: &[NodeId]) -> Vec<NodeId> {
        out.iter().map(|&i| next[i as usize]).collect()
    }

    fn leaders_from_stakes(stake_map: &BTreeMap<NodeId, u64>) -> Vec<NodeId> {
        stake_map.keys().cloned().collect()
    }

    fn dhondt_counts_for(next: &[NodeId], stake_map: &BTreeMap<NodeId, u64>, spools: u16) -> Vec<u16> {
        let stakes: Vec<_> = next.iter().map(|id| stake_map[id].into()).collect();
        let mut s = DhondtSpooler::default();
        s.allocate(&stakes, spools).unwrap()
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
    fn test_spool_map() {
        assert_eq!(to_spool_map(&[0]), &[]);
        assert_eq!(to_spool_map(&[0, 0, 0]), &[]);
        assert_eq!(to_spool_map(&[1, 2, 3]), &[0, 1, 1, 2, 2, 2]);
        assert_eq!(to_spool_map(&[3, 0, 2]), &[0, 0, 0, 2, 2]);
        assert_eq!(to_spool_map(&[1, 1, 1, 1]), &[0, 1, 2, 3]);
        assert_eq!(to_spool_map(&[5]), &[0, 0, 0, 0, 0]);
        assert_eq!(to_spool_map(&[1, 0, 0, 1]), &[0, 3]);
        assert_eq!(to_spool_map(&[2, 0, 1, 0, 3]), &[0, 0, 2, 4, 4, 4]);
    }

    #[test]
    fn test_union_simple() {
        let current = members([1, 3, 2]);
        let next = members([3, 4, 5]);
        let counts = vec![3, 2, 5];

        let (members, spools) = get_union_set(&current, &next, &counts).unwrap();

        assert_eq!(members, [REMOVED, 0, REMOVED, 1, 2]);
        assert_eq!(spools, [0, 3, 0, 2, 5]);
    }

    #[test]
    fn test_union_same() {
        let current = members([1, 2, 3]);
        let next = members([1, 2, 3]);
        let counts = vec![3, 2, 5];
        let (members, spools) = get_union_set(&current, &next, &counts).unwrap();
        assert_eq!(members, [0, 1, 2]);
        assert_eq!(spools, [3, 2, 5]);
    }

    #[test]
    fn test_union_replaced() {
        let current = members([1, 2, 3]);
        let next = members([4, 5, 6]);
        let counts = vec![3, 2, 5];
        let (members, spools) = get_union_set(&current, &next, &counts).unwrap();
        assert_eq!(members, [REMOVED, REMOVED, REMOVED, 0, 1, 2]);
        assert_eq!(spools, [0, 0, 0, 3, 2, 5]);
    }

    #[test]
    fn test_union_reverse() {
        let current = members([1, 2, 3, 4, 5]);
        let next = members([5, 4, 3, 2, 1]);
        let counts = vec![1, 1, 1, 1, 1];
        let (members, spools) = get_union_set(&current, &next, &counts).unwrap();
        assert_eq!(members, [4, 3, 2, 1, 0]);
        assert_eq!(spools, [1, 1, 1, 1, 1]);
    }

    #[test]
    fn test_free_spools() {
        let current = vec![0, 0, 1, 1, 2, 2];
        let required = vec![2, 2, 2];
        let (free, remaining) = super::free_spools(&current, &required);
        assert_eq!(free, vec![]);
        assert_eq!(remaining, vec![0, 0, 0]);
    }

    #[test]
    fn test_free_reduce() {
        let current = vec![0, 0, 1, 1, 2, 2];
        let required = vec![1, 1, 1];
        let (free, remaining) = super::free_spools(&current, &required);
        assert_eq!(free.len(), 3);
        assert_eq!(remaining, vec![0, 0, 0]);
    }

    #[test]
    fn test_free_increase() {
        let current = vec![0, 0, 1, 1, 2, 2];
        let required = vec![3, 2, 2];
        let (free, remaining) = super::free_spools(&current, &required);
        assert_eq!(free, vec![]);
        assert_eq!(remaining, vec![1, 0, 0]);
    }

    #[test]
    fn test_free_all() {
        let current = vec![0, 0, 1, 1, 2, 2];
        let required = vec![0, 0, 0];
        let (free, remaining) = super::free_spools(&current, &required);
        assert_eq!(free.len(), 6);
        assert_eq!(remaining, vec![0, 0, 0]);
    }

    #[test]
    fn test_free_some() {
        let current = vec![0, 0, 1, 1, 2, 2];
        let required = vec![1, 0, 1];
        let (free, remaining) = super::free_spools(&current, &required);
        assert_eq!(free.len(), 4);
        assert_eq!(remaining, vec![0, 0, 0]);
    }

    #[test]
    fn test_free_excess() {
        let current = vec![0, 0, 1, 1, 2, 2];
        let required = vec![4, 3, 2];
        let (free, remaining) = super::free_spools(&current, &required);
        assert_eq!(free.len(), 0);
        assert_eq!(remaining, vec![2, 1, 0]);
    }

    #[test]
    fn test_assign_spools() {
        let current = vec![0, 0, 1, 1, 2, 2];
        let free = vec![2, 3, 4];
        let required = vec![1, 1, 1];

        let out = super::assign_spools(&current, &free, &required).unwrap();
        assert_eq!(out, vec![0, 0, 2, 1, 0, 2]);
    }

    #[test]
    fn test_assign_spools_all() {
        let current = vec![5, 5, 6, 7, 9, 8];
        let free = vec![0, 1, 2, 3, 4, 5];
        let required = vec![2, 2, 2];

        let out = super::assign_spools(&current, &free, &required).unwrap();
        assert_eq!(out, vec![2, 2, 1, 1, 0, 0]);
    }

    #[test]
    fn test_assign_spools_some() {
        let current = vec![0, 1, 2, 3, 4, 5];
        let free = vec![3, 4, 5];
        let required = vec![1, 1, 0, 0, 0, 0];

        let out = super::assign_spools(&current, &free, &required).unwrap();
        assert_eq!(out, vec![0, 1, 2, 3, 1, 0]);
    }

    #[test]
    fn test_single() {
        let current = vec![NodeId(42)];
        let next = vec![NodeId(42)];
        let counts = vec![10 as u16];

        let spools = to_spool_map(&counts);

        let out = migrate_spools(&spools, &current, &next, &counts).unwrap();
        let node_spools = out_to_nodes(&out, &next);

        assert_eq!(total_count(&node_spools), 10);
        assert_eq!(spool_count(&node_spools, NodeId(42)), 10);
    }

    #[test]
    fn test_equal() {
        const SPOOLS: usize = 10;

        let stake_map: BTreeMap<NodeId, u64> = [
            (NodeId(1), 1000),
            (NodeId(2), 1000),
            (NodeId(3), 1000),
        ].into();

        let next = leaders_from_stakes(&stake_map);
        let counts = dhondt_counts_for(&next, &stake_map, SPOOLS as u16);
        assert_eq!(counts.iter().copied().sum::<u16>(), SPOOLS as u16);

        let current = next.clone();
        let spools = vec![0u8; SPOOLS];

        let out = migrate_spools(&spools, &current, &next, &counts).unwrap();
        let node_spools = out_to_nodes(&out, &next);

        let v: Vec<u16> = [NodeId(1), NodeId(2), NodeId(3)]
            .iter()
            .map(|nid| spool_count(&node_spools, *nid) as u16)
            .collect();

        assert_eq!(v, vec![4, 3, 3]);
    }

    #[test]
    fn test_even() {
        const SPOOLS: usize = 6;

        let stake_map: BTreeMap<NodeId, u64> = [
            (NodeId(1), 1000),
            (NodeId(2), 1000),
            (NodeId(3), 1000),
        ].into();

        let next = leaders_from_stakes(&stake_map);
        let counts = dhondt_counts_for(&next, &stake_map, SPOOLS as u16);

        let current = next.clone();
        let spools = vec![0u8; SPOOLS];

        let out = migrate_spools(&spools, &current, &next, &counts).unwrap();
        let node_spools = out_to_nodes(&out, &next);

        assert_eq!(spool_count(&node_spools, NodeId(1)), 2);
        assert_eq!(spool_count(&node_spools, NodeId(2)), 2);
        assert_eq!(spool_count(&node_spools, NodeId(3)), 2);
    }

    #[test]
    fn test_uneven() {
        const SPOOLS: usize = 10;

        let stake_map: BTreeMap<NodeId, u64> = [
            (NodeId(1), 4000),
            (NodeId(2), 2000),
            (NodeId(3), 1000),
        ].into();

        let next = leaders_from_stakes(&stake_map);
        let counts = dhondt_counts_for(&next, &stake_map, SPOOLS as u16);

        let current = next.clone();
        let spools = vec![0u8; SPOOLS];

        let out = migrate_spools(&spools, &current, &next, &counts).unwrap();
        let node_spools = out_to_nodes(&out, &next);

        assert_eq!(spool_count(&node_spools, NodeId(1)), 5);
        assert_eq!(spool_count(&node_spools, NodeId(2)), 4);
        assert_eq!(spool_count(&node_spools, NodeId(3)), 1);
    }

    #[test]
    fn test_reassign_reduce() {
        const SPOOLS: usize = 6;

        let initial_stakes: BTreeMap<NodeId, u64> = [
            (NodeId(1), 1000),
            (NodeId(2), 2000),
            (NodeId(3), 3000),
        ].into();

        let leaders1 = leaders_from_stakes(&initial_stakes);
        let counts1 = dhondt_counts_for(&leaders1, &initial_stakes, SPOOLS as u16);

        let spools1 = to_spool_map(&counts1);
        let out1 = migrate_spools(&spools1, &leaders1, &leaders1, &counts1).unwrap();
        assert_eq!(out1.len(), SPOOLS);

        let updated_stakes: BTreeMap<NodeId, u64> = [
            (NodeId(2), 2000),
            (NodeId(3), 3000),
        ].into();

        let leaders2 = leaders_from_stakes(&updated_stakes);
        let counts2 = dhondt_counts_for(&leaders2, &updated_stakes, SPOOLS as u16);

        let out2 = migrate_spools(&out1, &leaders1, &leaders2, &counts2).unwrap();
        let node_spools = out_to_nodes(&out2, &leaders2);

        assert_eq!(total_count(&node_spools), SPOOLS);
        for (i, nid) in leaders2.iter().enumerate() {
            assert_eq!(spool_count(&node_spools, *nid) as u16, counts2[i]);
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
        let spools0 = to_spool_map(&[2, 2, 2, 2, 2]);

        let leaders1 = vec![n1, n2, n3];
        let counts1 = vec![4, 3, 3];

        let out1 = migrate_spools(&spools0, &current, &leaders1, &counts1).unwrap();
        let map1 = out_to_nodes(&out1, &leaders1);
        assert_eq!(map1.len(), 10);
        let s1 = spool_list(&map1, n1);
        let s2 = spool_list(&map1, n2);
        let s3 = spool_list(&map1, n3);

        assert!(s1.contains(&0) && s1.contains(&1));
        assert!(s2.contains(&2) && s2.contains(&3));
        assert!(s3.contains(&4) && s3.contains(&5));

        let leaders2 = vec![n2, n3, n4, n5];
        let counts2 = vec![3, 3, 2, 2];

        let out2 = migrate_spools(&out1, &current, &leaders2, &counts2).unwrap();
        let map2 = out_to_nodes(&out2, &leaders2);

        assert_eq!(spool_list(&map2, n2), s2);
        assert_eq!(spool_list(&map2, n3), s3);
        assert_eq!(spool_list(&map2, n4).len(), 2);
        assert_eq!(spool_list(&map2, n5).len(), 2);

        let leaders3 = vec![n1];
        let counts3 = vec![10];

        let out3 = migrate_spools(&out2, &current, &leaders3, &counts3).unwrap();
        let map3 = out_to_nodes(&out3, &leaders3);

        let s = spool_list(&map3, n1);
        for i in 0..10 {
            assert!(s.contains(&(i as u16)));
        }
    }

    #[test]
    fn test_many() {
        const SPOOLS: usize = 1000;

        fn print_table_header() {
            println!(
                "{:<8} | {:>12} | {:>6} | {}",
                "NodeId", "Stake", "Spools", "SpoolIds"
            );
            println!("{}", "-".repeat(8 + 3 + 12 + 3 + 6 + 3 + 40));
        }

        let initial_stakes: BTreeMap<NodeId, u64> = (1..=100)
            .map(|i| (NodeId(100 - i), i as u64 * 1000))
            .collect();

        let leaders1 = initial_stakes.keys().cloned().collect::<Vec<_>>();
        let counts1 = dhondt_counts_for(&leaders1, &initial_stakes, SPOOLS as u16);
        let spools1 = to_spool_map(&counts1);

        let out1 = migrate_spools(&spools1, &leaders1, &leaders1, &counts1).unwrap();
        let map1 = out_to_nodes(&out1, &leaders1);
        assert_eq!(total_count(&map1), SPOOLS);

        print_table_header();
        for node_id in leaders1.iter() {
            let stake = initial_stakes.get(node_id).unwrap_or(&0);
            let spools_for_node = spool_list(&map1, *node_id);
            println!(
                "{:<8} | {:>12} | {:>6} | {:?}",
                format!("{:?}", node_id),
                stake,
                spools_for_node.len(),
                spools_for_node
            );
        }

        let updated_stakes: BTreeMap<NodeId, u64> = (51..=100)
            .map(|i| (NodeId(100 - i), i as u64 * 1000))
            .collect();

        let leaders2 = updated_stakes.keys().cloned().collect::<Vec<_>>();
        let counts2 = dhondt_counts_for(&leaders2, &updated_stakes, SPOOLS as u16);

        let out2 = migrate_spools(&out1, &leaders1, &leaders2, &counts2).unwrap();
        let map2 = out_to_nodes(&out2, &leaders2);

        assert_eq!(total_count(&map2), SPOOLS);
        let unique_nodes: HashSet<NodeId> = map2.iter().cloned().collect();
        assert_eq!(unique_nodes.len(), 50);

        let spool_counts_map = counts_map(&leaders2, &counts2);
        for (node_id, &count) in &spool_counts_map {
            assert_eq!(spool_count(&map2, *node_id), count as usize);
        }

        for node_id in initial_stakes.keys() {
            if !updated_stakes.contains_key(node_id) {
                assert_eq!(spool_count(&map2, *node_id), 0);
            }
        }
    }
}
