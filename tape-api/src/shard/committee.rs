use std::collections::BTreeMap;
use bytemuck::{Pod, Zeroable};

use super::{ 
    NodeId,
    StakeLeaderSet, 
    stake_weighted_shard_counts 
};

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

    /// Create an empty committee.
    pub fn empty() -> Self {
        let mut out: Self = Zeroable::zeroed();
        out.len = 0;
        out
    }

    /// Create a new committee from a map of NodeId -> shard count.
    pub fn new(node_shards: BTreeMap<NodeId, u16>) -> Self {
        let node_count = node_shards.len();
        assert!(node_count <= N, "Too many nodes for N");

        let total_shards: usize = node_shards.values().map(|&s| s as usize).sum();
        assert!(total_shards <= S, "Too many shards for S");

        let (ids, counts, offsets, shards) = pack_shards(node_shards.into_iter());

        let mut out = Self::empty();
        out.write_fixed(&ids, &counts, &offsets, &shards);
        out
    }

    /// Compute a new committee from stake leaders and shard count, optionally reusing
    /// the prior committee to minimize shard movement.
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

        let (mut new_cmt, mut to_move, remaining_needs) = compute_allocations(new_assignments, &prev_layout);

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
        self.node_ids[0..self.len as usize].binary_search(node_id).ok()
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
        (0..self.len as usize).map(|i| self.shard_counts[i] as usize).sum()
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
fn pack_shards(
    assigned: impl IntoIterator<Item = (NodeId, u16)>,
) -> (Vec<NodeId>, Vec<u16>, Vec<u16>, Vec<u16>) {
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

#[cfg(test)]
mod tests {
    use super::*;

    const MAX_NODES: usize = 256;
    const NUM_SHARDS: usize = 1000;

    type TestStakeLeaderSet = StakeLeaderSet<{ MAX_NODES }>;
    type TestCommittee = Committee<{ MAX_NODES }, { NUM_SHARDS }>;

    #[test]
    fn test_single_node() {
        let leaders = TestStakeLeaderSet::new(vec![(NodeId(10), 1_000_000)]);
        let committee = TestCommittee::from(None, &leaders, 10);
        assert_eq!(committee.size(), 1);
        assert_eq!(committee.total_shards(), 10);
        assert_eq!(committee.shards(&NodeId(10)).unwrap().len(), 10);
    }

    #[test]
    fn test_compute_even_distribution() {
        let leaders = TestStakeLeaderSet::new(vec![
            (NodeId(1), 1000), 
            (NodeId(2), 1000), 
            (NodeId(3), 1000)
        ]);

        let committee = TestCommittee::from(None, &leaders, 6);
        assert_eq!(committee.size(), 3);
        assert_eq!(committee.shards(&NodeId(1)).unwrap().len(), 2);
        assert_eq!(committee.shards(&NodeId(2)).unwrap().len(), 2);
        assert_eq!(committee.shards(&NodeId(3)).unwrap().len(), 2);
    }

    #[test]
    fn test_compute_uneven_distribution() {
        let leaders = TestStakeLeaderSet::new(vec![
            (NodeId(1), 4000), 
            (NodeId(2), 2000), 
            (NodeId(3), 1000)
        ]);

        let committee = TestCommittee::from(None, &leaders, 10);
        assert_eq!(committee.size(), 3);
        assert_eq!(committee.shards(&NodeId(1)).unwrap().len(), 6);
        assert_eq!(committee.shards(&NodeId(2)).unwrap().len(), 3);
        assert_eq!(committee.shards(&NodeId(3)).unwrap().len(), 1);
    }

    #[test]
    fn test_consistent_shard_assignment() {
        let init_map: BTreeMap<NodeId, u16> = [
            (NodeId(3), 2),
            (NodeId(2), 2),
            (NodeId(1), 2),
            (NodeId(0), 2)
        ].into();

        let c1 = TestCommittee::new(init_map);

        assert_eq!(c1.size(), 4);
        assert_eq!(c1.shards(&NodeId(0)).unwrap(), &[0u16, 1]); // << to be removed
        assert_eq!(c1.shards(&NodeId(1)).unwrap(), &[2u16, 3]); // << to be removed
        assert_eq!(c1.shards(&NodeId(2)).unwrap(), &[4u16, 5]);
        assert_eq!(c1.shards(&NodeId(3)).unwrap(), &[6u16, 7]);

        // Remove two last nodes: new size 2, assign 4 shards each
        let t_map: BTreeMap<NodeId, u16> = [(NodeId(3), 4), (NodeId(2), 4)].into();
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
    fn test_complex_shard_assignment() {
        let n1 = NodeId(1);
        let n2 = NodeId(2);
        let n3 = NodeId(3);
        let n4 = NodeId(4);
        let n5 = NodeId(5);

        let c1 = TestCommittee::new([(n1, 2), (n2, 2), (n3, 2), (n4, 2), (n5, 2)].into());

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
