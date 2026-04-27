//! Epoch-over-epoch spool migration.
//!
//! 1000 spools are partitioned into 50 groups of 20. Each group must map its
//! 20 spools to 20 *distinct* nodes, so no node holds more than one spool per
//! group (and therefore at most 50 spools overall).
//!
//! Migration runs per-group in three phases:
//!
//! 1. **Retention** -- spools whose previous owner is still present in the next
//!    epoch and still has capacity are kept in place, minimising churn.
//!
//! 2. **Must-take with eviction** -- nodes that *must* receive a spool in this
//!    group (their target count has not been met by any prior group) claim a
//!    free slot. If no free slot exists, the least-critical retained spool is
//!    evicted to make room.
//!
//! 3. **Fill remaining** -- any slots still unassigned are handed out via a
//!    max-heap ordered by (remaining need, target, node id) to the nodes that
//!    have capacity left and are not yet used in this group.
//!
//! The algorithm is designed for a constrained environment (4 KB stack per
//! frame, 32 KB heap) and uses fixed-size bitmasks instead of hash sets.

use tape_core::types::NodeId;
use tape_core::erasure::{MEMBER_COUNT, SPOOL_COUNT, SPOOL_GROUP_COUNT, SPOOL_GROUP_SIZE};
use tape_core::spooler::{SpoolCount, SpoolMapping, SpoolerError};
use tape_crypto::hash::{Hash, hashv};
use crate::MAX_SPOOLS_PER_NODE;
const MAX_NODES: usize = MEMBER_COUNT;
const MIN_NODES: usize = SPOOL_GROUP_SIZE;

type NodeIndex = u8;
type SpoolOffset = usize;

/// Number of remaining-count buckets (0..=MAX_SPOOLS_PER_NODE).
const BUCKET_COUNT: usize = MAX_SPOOLS_PER_NODE as usize + 1;

/// A bitmask over groups: bit `g` is set when group `g` is included.
type GroupSet = u64;

/// Bits per word in the bitmask.
const NODESET_WORD_BITS: usize = 64;

/// Number of u64 words needed to cover MAX_NODES bits.
const NODESET_WORDS: usize = (MAX_NODES + NODESET_WORD_BITS - 1) / NODESET_WORD_BITS;

/// Bitmask covering indices 0..MAX_NODES, using NODESET_WORDS u64 words.
struct NodeSet([u64; NODESET_WORDS]);

impl Default for NodeSet {
    fn default() -> Self {
        Self([0; NODESET_WORDS])
    }
}

impl NodeSet {
    #[inline]
    fn test(&self, index: usize) -> bool {
        let (word, bit) = (index / NODESET_WORD_BITS, index % NODESET_WORD_BITS);
        (self.0[word] >> bit) & 1 == 1
    }

    #[inline]
    fn set(&mut self, index: usize) {
        let (word, bit) = (index / NODESET_WORD_BITS, index % NODESET_WORD_BITS);
        self.0[word] |= 1u64 << bit;
    }

    #[inline]
    fn clear(&mut self, index: usize) {
        let (word, bit) = (index / NODESET_WORD_BITS, index % NODESET_WORD_BITS);
        self.0[word] &= !(1u64 << bit);
    }
}

/// Rotate a GroupSet left by `r` positions within the SPOOL_GROUP_COUNT-bit field.
#[inline]
fn rotate_groups_left(x: GroupSet, r: u32) -> GroupSet {
    const BITS: u32 = SPOOL_GROUP_COUNT as u32;
    const MASK: GroupSet = (1 << SPOOL_GROUP_COUNT) - 1;
    let r = r % BITS;
    if r == 0 {
        x & MASK
    } else {
        ((x << r) | (x >> (BITS - r))) & MASK
    }
}

/// Rotate a GroupSet right by `r` positions within the SPOOL_GROUP_COUNT-bit field.
#[inline]
fn rotate_groups_right(x: GroupSet, r: u32) -> GroupSet {
    const BITS: u32 = SPOOL_GROUP_COUNT as u32;
    const MASK: GroupSet = (1 << SPOOL_GROUP_COUNT) - 1;
    let r = r % BITS;
    if r == 0 {
        x & MASK
    } else {
        ((x >> r) | (x << (BITS - r))) & MASK
    }
}

/// Take the lowest `k` set bits from a GroupSet (in increasing bit-index order).
#[inline]
fn take_k_lowest_bits(mut x: GroupSet, k: u32) -> GroupSet {
    let mut out: GroupSet = 0;
    let mut remaining = k;
    while remaining > 0 && x != 0 {
        let lowest = x & x.wrapping_neg();
        out |= lowest;
        x ^= lowest;
        remaining -= 1;
    }
    out
}

/// Deterministic per-node offset into the group ring, derived from node identity and a seed hash.
///
/// The seed is typically the slot hash at epoch transition, making the offset unpredictable
/// until the transition executes on-chain.
#[inline]
fn group_offset(node_id: NodeId, seed: &Hash) -> u32 {
    let h = hashv(&[seed.as_ref(), &node_id.0.to_le_bytes()]);
    let val = u64::from_le_bytes(h.0[..8].try_into().unwrap());
    (val % (SPOOL_GROUP_COUNT as u64)) as u32
}

/// Mutable per-node bookkeeping used throughout group processing.
struct NodeState {
    node_id: NodeId,
    target: SpoolCount,
    remaining: SpoolCount,
}

impl NodeState {
    #[inline]
    fn can_accept(&self, index: usize, used: &NodeSet) -> bool {
        self.remaining > 0 && !used.test(index)
    }
}

/// Priority: higher remaining first, then higher target (stake proxy), then lower node_id.
#[derive(Eq, PartialEq, Copy, Clone)]
struct FillEntry {
    remaining: SpoolCount,
    target: SpoolCount,
    node_id: NodeId,
    node_index: NodeIndex,
}

impl Ord for FillEntry {
    #[inline]
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.remaining
            .cmp(&other.remaining)
            .then_with(|| self.target.cmp(&other.target))
            .then_with(|| other.node_id.cmp(&self.node_id))
    }
}

impl PartialOrd for FillEntry {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

struct RetainedEntry {
    offset: SpoolOffset,
    node_index: NodeIndex,
}

/// Maintains nodes partitioned by their `remaining` spool count.
struct RemainingBuckets {
    buckets: [Vec<NodeIndex>; BUCKET_COUNT],
    positions: Vec<u8>,
}

impl RemainingBuckets {
    fn new(nodes: &[NodeState]) -> Self {
        let mut buckets: [Vec<NodeIndex>; BUCKET_COUNT] = std::array::from_fn(|_| Vec::new());
        let mut positions = vec![0u8; nodes.len()];
        for (i, node) in nodes.iter().enumerate() {
            let r = node.remaining as usize;
            positions[i] = buckets[r].len() as u8;
            buckets[r].push(i as u8);
        }
        Self { buckets, positions }
    }

    #[inline]
    fn move_node(
        &mut self,
        node_index: NodeIndex,
        old_remaining: SpoolCount,
        new_remaining: SpoolCount,
    ) {
        let old_bucket = &mut self.buckets[old_remaining as usize];
        let pos = self.positions[node_index as usize] as usize;
        old_bucket.swap_remove(pos);
        if pos < old_bucket.len() {
            self.positions[old_bucket[pos] as usize] = pos as u8;
        }
        let new_bucket = &mut self.buckets[new_remaining as usize];
        self.positions[node_index as usize] = new_bucket.len() as u8;
        new_bucket.push(node_index);
    }

    #[inline]
    fn nodes_with_remaining(&self, remaining: usize) -> &[NodeIndex] {
        &self.buckets[remaining]
    }
}

fn eviction_order(
    a: &RetainedEntry,
    b: &RetainedEntry,
    nodes: &[NodeState],
    remaining_groups: usize,
) -> std::cmp::Ordering {
    let a_node = &nodes[a.node_index as usize];
    let b_node = &nodes[b.node_index as usize];

    let a_was_critical = (a_node.remaining + 1) as usize == remaining_groups;
    let b_was_critical = (b_node.remaining + 1) as usize == remaining_groups;

    b_was_critical
        .cmp(&a_was_critical)
        .then_with(|| b_node.remaining.cmp(&a_node.remaining))
        .then_with(|| b_node.target.cmp(&a_node.target))
        .then_with(|| a_node.node_id.cmp(&b_node.node_id))
}

struct MigrationContext {
    nodes: Vec<NodeState>,
    prev_owner: Vec<Option<NodeIndex>>,
    retain_mask: Vec<GroupSet>,
    planned_retentions: Vec<SpoolCount>,
    retain_nodes_per_group: Vec<Vec<NodeIndex>>,
    buckets: RemainingBuckets,
    result: Vec<SpoolMapping>,
    retained: Vec<RetainedEntry>,
    unassigned: Vec<SpoolOffset>,
    must_take: Vec<NodeIndex>,
    candidates: Vec<FillEntry>,
}

fn validate(
    current_spools: &[SpoolMapping],
    next_members: &[NodeId],
    next_spool_counts: &[SpoolCount],
) -> Result<(), SpoolerError> {
    if current_spools.len() != SPOOL_COUNT {
        return Err(SpoolerError::TotalMismatch);
    }
    if next_members.len() != next_spool_counts.len() {
        return Err(SpoolerError::CountMismatch);
    }
    if next_members.len() > MAX_NODES {
        return Err(SpoolerError::MemberLimit);
    }
    if next_members.len() < MIN_NODES {
        return Err(SpoolerError::InsufficientNodes);
    }
    let total: usize = next_spool_counts.iter().map(|&x| x as usize).sum();
    if total != SPOOL_COUNT {
        return Err(SpoolerError::TotalMismatch);
    }
    for &count in next_spool_counts {
        if count > MAX_SPOOLS_PER_NODE {
            return Err(SpoolerError::SpoolCapExceeded);
        }
    }
    Ok(())
}

fn build_node_states(next_members: &[NodeId], next_spool_counts: &[SpoolCount]) -> Vec<NodeState> {
    next_members
        .iter()
        .zip(next_spool_counts)
        .map(|(&node_id, &target)| NodeState {
            node_id,
            target,
            remaining: target,
        })
        .collect()
}

fn build_previous_owners(
    current_spools: &[SpoolMapping],
    current_members: &[NodeId],
    next_members: &[NodeId],
) -> Result<Vec<Option<NodeIndex>>, SpoolerError> {
    let current_to_next: Vec<Option<NodeIndex>> = current_members
        .iter()
        .map(|current_id| {
            next_members
                .iter()
                .position(|next_id| next_id == current_id)
                .map(|pos| pos as NodeIndex)
        })
        .collect();

    let mut prev_owner: Vec<Option<NodeIndex>> = vec![None; SPOOL_COUNT];
    for (spool, &current_index) in current_spools.iter().enumerate() {
        let ci = current_index as usize;
        if ci >= current_members.len() && !current_members.is_empty() {
            return Err(SpoolerError::BadIndex);
        }
        if ci < current_to_next.len() {
            prev_owner[spool] = current_to_next[ci];
        }
    }
    Ok(prev_owner)
}

fn compute_retain_masks(
    nodes: &[NodeState],
    prev_owner: &[Option<NodeIndex>],
    seed: &Hash,
) -> (Vec<GroupSet>, Vec<SpoolCount>) {
    let num_next = nodes.len();

    let mut previous_groups: Vec<GroupSet> = vec![0; num_next];
    for (spool, owner) in prev_owner.iter().enumerate() {
        if let Some(node_index) = *owner {
            let group = spool / SPOOL_GROUP_SIZE;
            previous_groups[node_index as usize] |= 1u64 << group;
        }
    }

    let mut retain_mask: Vec<GroupSet> = vec![0; num_next];
    for i in 0..num_next {
        let available = previous_groups[i];
        let keep = (available.count_ones() as u16).min(nodes[i].target) as u32;
        if keep == 0 {
            continue;
        }
        let offset = group_offset(nodes[i].node_id, seed);
        let rotated = rotate_groups_right(available, offset);
        let picked = take_k_lowest_bits(rotated, keep);
        retain_mask[i] = rotate_groups_left(picked, offset);
    }

    let planned_retentions: Vec<SpoolCount> = retain_mask
        .iter()
        .map(|mask| mask.count_ones() as SpoolCount)
        .collect();

    (retain_mask, planned_retentions)
}

fn build_retain_nodes_per_group(retain_mask: &[GroupSet]) -> Vec<Vec<NodeIndex>> {
    let mut per_group: Vec<Vec<NodeIndex>> = vec![vec![]; SPOOL_GROUP_COUNT];
    for (node_index, &mask) in retain_mask.iter().enumerate() {
        let mut remaining_mask = mask;
        while remaining_mask != 0 {
            let group = remaining_mask.trailing_zeros() as usize;
            per_group[group].push(node_index as u8);
            remaining_mask &= remaining_mask - 1;
        }
    }
    per_group
}

impl MigrationContext {
    fn retain(&mut self, group: usize, used: &mut NodeSet) {
        let group_start = group * SPOOL_GROUP_SIZE;
        let group_bit = 1u64 << group;

        self.retained.clear();
        self.unassigned.clear();

        for &node_index in &self.retain_nodes_per_group[group] {
            let ni = node_index as usize;
            self.planned_retentions[ni] = self.planned_retentions[ni].saturating_sub(1);
        }

        for offset in 0..SPOOL_GROUP_SIZE {
            let spool = group_start + offset;
            let mut kept = false;

            if let Some(prev_node) = self.prev_owner[spool] {
                let ni = prev_node as usize;
                if (self.retain_mask[ni] & group_bit) != 0
                    && self.nodes[ni].can_accept(ni, used)
                {
                    self.result[spool] = prev_node;
                    let old_remaining = self.nodes[ni].remaining;
                    self.nodes[ni].remaining -= 1;
                    self.buckets.move_node(prev_node, old_remaining, old_remaining - 1);
                    used.set(ni);
                    self.retained.push(RetainedEntry {
                        offset,
                        node_index: prev_node,
                    });
                    kept = true;
                }
            }

            if !kept {
                self.unassigned.push(offset);
            }
        }
    }

    fn take(
        &mut self,
        group: usize,
        remaining_groups: usize,
        used: &mut NodeSet,
    ) -> Result<(), SpoolerError> {
        let group_start = group * SPOOL_GROUP_SIZE;

        for _iteration in 0..=SPOOL_GROUP_SIZE {
            self.must_take.clear();
            if remaining_groups <= MAX_SPOOLS_PER_NODE as usize {
                for &node_index in self.buckets.nodes_with_remaining(remaining_groups) {
                    if !used.test(node_index as usize) {
                        self.must_take.push(node_index);
                    }
                }
            }

            if self.must_take.len() > SPOOL_GROUP_SIZE {
                return Err(SpoolerError::Infeasible);
            }

            if self.must_take.len() <= self.unassigned.len() {
                for &node_index in &self.must_take {
                    let offset = self.unassigned.pop().ok_or(SpoolerError::Infeasible)?;
                    let spool = group_start + offset;
                    let ni = node_index as usize;
                    self.result[spool] = node_index;
                    let old_remaining = self.nodes[ni].remaining;
                    self.nodes[ni].remaining -= 1;
                    self.buckets.move_node(node_index, old_remaining, old_remaining - 1);
                    used.set(ni);
                }
                return Ok(());
            }

            let need_evict = self.must_take.len() - self.unassigned.len();

            let nodes = &self.nodes;
            self.retained
                .sort_by(|a, b| eviction_order(a, b, nodes, remaining_groups));

            for _ in 0..need_evict {
                let entry = self.retained.pop().ok_or(SpoolerError::Infeasible)?;
                let ni = entry.node_index as usize;
                let old_remaining = self.nodes[ni].remaining;
                self.nodes[ni].remaining += 1;
                self.buckets
                    .move_node(entry.node_index, old_remaining, old_remaining + 1);
                used.clear(ni);
                self.unassigned.push(entry.offset);
            }
        }

        Err(SpoolerError::Infeasible)
    }

    fn fill(
        &mut self,
        group: usize,
        used: &mut NodeSet,
    ) -> Result<(), SpoolerError> {
        let group_start = group * SPOOL_GROUP_SIZE;
        let slots_needed = self.unassigned.len();
        if slots_needed == 0 {
            return Ok(());
        }

        self.candidates.clear();
        'primary: for r in (1..=MAX_SPOOLS_PER_NODE as usize).rev() {
            for &node_index in self.buckets.nodes_with_remaining(r) {
                let ni = node_index as usize;
                if self.nodes[ni].remaining > self.planned_retentions[ni] && !used.test(ni) {
                    self.candidates.push(FillEntry {
                        remaining: self.nodes[ni].remaining,
                        target: self.nodes[ni].target,
                        node_id: self.nodes[ni].node_id,
                        node_index,
                    });
                }
            }
            if self.candidates.len() >= slots_needed {
                break 'primary;
            }
        }

        if self.candidates.len() < slots_needed {
            self.candidates.clear();
            'fallback: for r in (1..=MAX_SPOOLS_PER_NODE as usize).rev() {
                for &node_index in self.buckets.nodes_with_remaining(r) {
                    let ni = node_index as usize;
                    if self.nodes[ni].can_accept(ni, used) {
                        self.candidates.push(FillEntry {
                            remaining: self.nodes[ni].remaining,
                            target: self.nodes[ni].target,
                            node_id: self.nodes[ni].node_id,
                            node_index,
                        });
                    }
                }
                if self.candidates.len() >= slots_needed {
                    break 'fallback;
                }
            }
        }

        if self.candidates.len() < slots_needed {
            return Err(SpoolerError::Infeasible);
        }

        self.candidates.sort_unstable_by(|a, b| b.cmp(a));

        for idx in 0..slots_needed {
            let offset = self.unassigned.pop().ok_or(SpoolerError::Infeasible)?;
            let entry = self.candidates[idx];
            let ni = entry.node_index as usize;
            let spool = group_start + offset;
            self.result[spool] = entry.node_index;
            let old_remaining = self.nodes[ni].remaining;
            self.nodes[ni].remaining -= 1;
            self.buckets
                .move_node(entry.node_index, old_remaining, old_remaining - 1);
            used.set(ni);
        }

        Ok(())
    }

    fn verify(&self) -> Result<(), SpoolerError> {
        for node in &self.nodes {
            if node.remaining != 0 {
                return Err(SpoolerError::Infeasible);
            }
        }
        Ok(())
    }
}

/// Reassign spools from the current epoch to the next epoch with group constraints
/// and minimal disruption.
pub fn migrate_spools(
    current_spools: &[SpoolMapping],
    current_members: &[NodeId],
    next_members: &[NodeId],
    next_spool_counts: &[SpoolCount],
    seed: &Hash,
) -> Result<Vec<SpoolMapping>, SpoolerError> {
    validate(current_spools, next_members, next_spool_counts)?;

    let nodes = build_node_states(next_members, next_spool_counts);
    let prev_owner = build_previous_owners(current_spools, current_members, next_members)?;
    let (retain_mask, planned_retentions) = compute_retain_masks(&nodes, &prev_owner, seed);
    let retain_nodes_per_group = build_retain_nodes_per_group(&retain_mask);
    let buckets = RemainingBuckets::new(&nodes);
    let num_next = next_members.len();

    let result = vec![0; SPOOL_COUNT];
    let retained = Vec::with_capacity(SPOOL_GROUP_SIZE);
    let unassigned = Vec::with_capacity(SPOOL_GROUP_SIZE);
    let must_take = Vec::with_capacity(MAX_NODES);
    let candidates = Vec::with_capacity(num_next);

    let mut ctx = MigrationContext {
        nodes,
        prev_owner,
        retain_mask,
        planned_retentions,
        retain_nodes_per_group,
        buckets,
        result,
        retained,
        unassigned,
        must_take,
        candidates,
    };

    for group in 0..SPOOL_GROUP_COUNT {
        let remaining_groups = SPOOL_GROUP_COUNT - group;

        debug_assert!(
            ctx.nodes
                .iter()
                .all(|n| (n.remaining as usize) <= remaining_groups),
            "infeasibility: node remaining > remaining_groups",
        );

        let mut used = NodeSet::default();
        ctx.retain(group, &mut used);
        ctx.take(group, remaining_groups, &mut used)?;
        ctx.fill(group, &mut used)?;
    }

    ctx.verify()?;
    Ok(ctx.result)
}

/// Create an initial spool assignment that satisfies group constraints.
///
/// This is equivalent to calling `migrate_spools` with no prior state.
pub fn initial_assignment(
    members: &[NodeId],
    spool_counts: &[SpoolCount],
) -> Result<Vec<SpoolMapping>, SpoolerError> {
    let dummy_current: Vec<SpoolMapping> = vec![0; SPOOL_COUNT];
    let empty: &[NodeId] = &[];
    migrate_spools(&dummy_current, empty, members, spool_counts, &Hash::default())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dhondt::DhondtSpooler;
    use tape_core::types::TAPE;
    use std::collections::HashSet;

    fn make_members(count: usize) -> Vec<NodeId> {
        (1..=count as u64).map(NodeId).collect()
    }

    fn dhondt_counts(stakes: &[TAPE], total: SpoolCount) -> Vec<SpoolCount> {
        let s = DhondtSpooler::default();
        s.allocate(stakes, total).unwrap()
    }

    fn verify_group_constraints(result: &[SpoolMapping], num_nodes: usize) {
        assert_eq!(result.len(), SPOOL_COUNT);
        for group in 0..SPOOL_GROUP_COUNT {
            let mut seen = vec![false; num_nodes];
            for offset in 0..SPOOL_GROUP_SIZE {
                let spool_idx = group * SPOOL_GROUP_SIZE + offset;
                let node = result[spool_idx] as usize;
                assert!(
                    node < num_nodes,
                    "spool {} assigned to out-of-range node {}",
                    spool_idx, node
                );
                assert!(
                    !seen[node],
                    "node {} appears twice in group {} (spool {})",
                    node, group, spool_idx
                );
                seen[node] = true;
            }
        }
    }

    fn verify_counts(result: &[SpoolMapping], expected: &[SpoolCount]) {
        let mut actual: Vec<SpoolCount> = vec![0; expected.len()];
        for &ni in result {
            actual[ni as usize] += 1;
        }
        assert_eq!(actual, expected);
    }

    fn count_changes(
        prev: &[SpoolMapping],
        prev_members: &[NodeId],
        curr: &[SpoolMapping],
        curr_members: &[NodeId],
    ) -> usize {
        let mut changes = 0;
        for i in 0..SPOOL_COUNT {
            let prev_node = prev_members[prev[i] as usize];
            let curr_node = curr_members[curr[i] as usize];
            if prev_node != curr_node {
                changes += 1;
            }
        }
        changes
    }

    fn ids(v: &[u64]) -> Vec<NodeId> {
        v.iter().copied().map(NodeId).collect()
    }

    fn uniform(n: usize, per: SpoolCount) -> Vec<SpoolCount> {
        vec![per; n]
    }

    fn fresh(m: &[NodeId], c: &[SpoolCount]) -> Vec<SpoolMapping> {
        initial_assignment(m, c).unwrap()
    }

    fn mig(
        cur: &[SpoolMapping],
        cm: &[NodeId],
        nm: &[NodeId],
        nc: &[SpoolCount],
    ) -> Vec<SpoolMapping> {
        migrate_spools(cur, cm, nm, nc, &Hash::default()).unwrap()
    }

    fn group_count(r: &[SpoolMapping], ni: SpoolMapping) -> usize {
        (0..SPOOL_GROUP_COUNT)
            .filter(|&g| {
                let base = g * SPOOL_GROUP_SIZE;
                (0..SPOOL_GROUP_SIZE).any(|s| r[base + s] == ni)
            })
            .count()
    }

    fn group_set(r: &[SpoolMapping], g: usize) -> Vec<SpoolMapping> {
        let base = g * SPOOL_GROUP_SIZE;
        let mut v: Vec<SpoolMapping> = (0..SPOOL_GROUP_SIZE).map(|s| r[base + s]).collect();
        v.sort();
        v
    }

    fn tally(r: &[SpoolMapping], n: usize) -> Vec<SpoolCount> {
        let mut c = vec![0 as SpoolCount; n];
        for &x in r {
            c[x as usize] += 1;
        }
        c
    }

    fn moved(
        prev: &[SpoolMapping],
        pm: &[NodeId],
        next: &[SpoolMapping],
        nm: &[NodeId],
    ) -> usize {
        (0..SPOOL_COUNT)
            .filter(|&i| pm[prev[i] as usize] != nm[next[i] as usize])
            .count()
    }

    fn round_robin(n: usize) -> Vec<SpoolMapping> {
        (0..SPOOL_COUNT)
            .map(|i| {
                let g = i / SPOOL_GROUP_SIZE;
                let s = i % SPOOL_GROUP_SIZE;
                ((g + s) % n) as SpoolMapping
            })
            .collect()
    }

    fn sequential_blocks(block_size: usize) -> Vec<SpoolMapping> {
        (0..SPOOL_COUNT)
            .map(|i| (i / block_size) as SpoolMapping)
            .collect()
    }

    // ----- Basic tests -----

    #[test]
    fn fresh_twenty() {
        let n = 20;
        let members = make_members(n);
        let counts: Vec<SpoolCount> = vec![50; n];
        let result = initial_assignment(&members, &counts).unwrap();
        verify_group_constraints(&result, n);
        verify_counts(&result, &counts);
    }

    #[test]
    fn fresh_large() {
        let n = 128;
        let members = make_members(n);
        let stakes: Vec<TAPE> = (1..=n as u64).map(|i| TAPE(i * 1000)).collect();
        let counts = dhondt_counts(&stakes, SPOOL_COUNT as SpoolCount);
        assert_eq!(counts.iter().map(|&x| x as usize).sum::<usize>(), SPOOL_COUNT);
        let result = initial_assignment(&members, &counts).unwrap();
        verify_group_constraints(&result, n);
        verify_counts(&result, &counts);
    }

    #[test]
    fn fresh_uneven() {
        let n = 50;
        let members = make_members(n);
        let stakes: Vec<TAPE> = (1..=n as u64).map(|i| TAPE(i * i * 100)).collect();
        let counts = dhondt_counts(&stakes, SPOOL_COUNT as SpoolCount);
        let result = initial_assignment(&members, &counts).unwrap();
        verify_group_constraints(&result, n);
        verify_counts(&result, &counts);
    }

    // ----- Stability tests -----

    #[test]
    fn identity_stable() {
        let n = 30;
        let members = make_members(n);
        let stakes: Vec<TAPE> = (1..=n as u64).map(|i| TAPE(i * 500)).collect();
        let counts = dhondt_counts(&stakes, SPOOL_COUNT as SpoolCount);

        let epoch1 = initial_assignment(&members, &counts).unwrap();
        verify_group_constraints(&epoch1, n);

        let epoch2 = migrate_spools(&epoch1, &members, &members, &counts, &Hash::default()).unwrap();
        verify_group_constraints(&epoch2, n);
        verify_counts(&epoch2, &counts);

        let changes = count_changes(&epoch1, &members, &epoch2, &members);
        assert_eq!(changes, 0, "expected zero changes for identical epochs");
    }

    #[test]
    fn removal_disruption() {
        let n = 40;
        let members1 = make_members(n);
        let stakes1: Vec<TAPE> = (1..=n as u64).map(|i| TAPE(i * 300)).collect();
        let counts1 = dhondt_counts(&stakes1, SPOOL_COUNT as SpoolCount);
        let epoch1 = initial_assignment(&members1, &counts1).unwrap();

        let members2: Vec<NodeId> = members1[..30].to_vec();
        let stakes2: Vec<TAPE> = stakes1[..30].to_vec();
        let counts2 = dhondt_counts(&stakes2, SPOOL_COUNT as SpoolCount);

        let epoch2 = migrate_spools(&epoch1, &members1, &members2, &counts2, &Hash::default()).unwrap();
        verify_group_constraints(&epoch2, 30);
        verify_counts(&epoch2, &counts2);

        let changes = count_changes(&epoch1, &members1, &epoch2, &members2);
        assert!(changes <= SPOOL_COUNT, "changes {} exceeds total spools", changes);
    }

    #[test]
    fn addition_disruption() {
        let n1 = 30;
        let members1 = make_members(n1);
        let stakes1: Vec<TAPE> = (1..=n1 as u64).map(|i| TAPE(i * 500)).collect();
        let counts1 = dhondt_counts(&stakes1, SPOOL_COUNT as SpoolCount);
        let epoch1 = initial_assignment(&members1, &counts1).unwrap();

        let n2 = 50;
        let members2 = make_members(n2);
        let stakes2: Vec<TAPE> = (1..=n2 as u64).map(|i| TAPE(i * 500)).collect();
        let counts2 = dhondt_counts(&stakes2, SPOOL_COUNT as SpoolCount);

        let epoch2 = migrate_spools(&epoch1, &members1, &members2, &counts2, &Hash::default()).unwrap();
        verify_group_constraints(&epoch2, n2);
        verify_counts(&epoch2, &counts2);
    }

    #[test]
    fn full_replace() {
        let n = 25;
        let members1 = make_members(n);
        let stakes: Vec<TAPE> = vec![TAPE(1000); n];
        let counts1 = dhondt_counts(&stakes, SPOOL_COUNT as SpoolCount);
        let epoch1 = initial_assignment(&members1, &counts1).unwrap();

        let members2: Vec<NodeId> = (101..=125).map(NodeId).collect();
        let counts2 = dhondt_counts(&stakes, SPOOL_COUNT as SpoolCount);

        let epoch2 = migrate_spools(&epoch1, &members1, &members2, &counts2, &Hash::default()).unwrap();
        verify_group_constraints(&epoch2, n);
        verify_counts(&epoch2, &counts2);
    }

    // ----- Epoch chain test -----

    #[test]
    fn epoch_chain() {
        let n = 30;
        let mut members = make_members(n);
        let stakes: Vec<TAPE> = (1..=n as u64).map(|i| TAPE(i * 1000)).collect();
        let counts = dhondt_counts(&stakes, SPOOL_COUNT as SpoolCount);
        let mut current = initial_assignment(&members, &counts).unwrap();
        verify_group_constraints(&current, n);

        for epoch in 0..5 {
            let base = (epoch + 1) * 5;
            let new_members: Vec<NodeId> = (base as u64 + 6..=base as u64 + n as u64 + 5)
                .map(NodeId)
                .collect();
            let new_stakes: Vec<TAPE> = (1..=n as u64).map(|i| TAPE(i * 1000)).collect();
            let new_counts = dhondt_counts(&new_stakes, SPOOL_COUNT as SpoolCount);

            let next =
                migrate_spools(&current, &members, &new_members, &new_counts, &Hash::default())
                    .unwrap();
            verify_group_constraints(&next, n);
            verify_counts(&next, &new_counts);

            current = next;
            members = new_members;
        }
    }

    // ----- Edge case tests -----

    #[test]
    fn minimum_nodes() {
        let n = 20;
        let members = make_members(n);
        let counts: Vec<SpoolCount> = vec![50; n];
        let result = initial_assignment(&members, &counts).unwrap();
        verify_group_constraints(&result, n);
        verify_counts(&result, &counts);
    }

    #[test]
    fn err_few_nodes() {
        let members = make_members(19);
        let counts: Vec<SpoolCount> = vec![52; 19];
        let err = initial_assignment(&members, &counts).unwrap_err();
        assert_eq!(err, SpoolerError::InsufficientNodes);
    }

    #[test]
    fn err_many_nodes() {
        let members = make_members(129);
        let counts: Vec<SpoolCount> = vec![0; 129];
        let err = initial_assignment(&members, &counts).unwrap_err();
        assert_eq!(err, SpoolerError::MemberLimit);
    }

    #[test]
    fn err_total() {
        let members = make_members(20);
        let counts: Vec<SpoolCount> = vec![49; 20];
        let err = initial_assignment(&members, &counts).unwrap_err();
        assert_eq!(err, SpoolerError::TotalMismatch);
    }

    #[test]
    fn err_cap() {
        let members = make_members(20);
        let mut counts: Vec<SpoolCount> = vec![50; 19];
        counts.push(50);
        counts[0] = 51;
        counts[1] = 49;
        let err = initial_assignment(&members, &counts).unwrap_err();
        assert_eq!(err, SpoolerError::SpoolCapExceeded);
    }

    #[test]
    fn err_count_mismatch() {
        let m = make_members(20);
        let c: Vec<SpoolCount> = vec![50; 19];
        assert_eq!(
            initial_assignment(&m, &c).unwrap_err(),
            SpoolerError::CountMismatch,
        );
    }

    // ----- Determinism tests -----

    #[test]
    fn deterministic_fresh() {
        let n = 50;
        let members = make_members(n);
        let stakes: Vec<TAPE> = (1..=n as u64).map(|i| TAPE(i * 777)).collect();
        let counts = dhondt_counts(&stakes, SPOOL_COUNT as SpoolCount);

        let a = initial_assignment(&members, &counts).unwrap();
        let b = initial_assignment(&members, &counts).unwrap();
        assert_eq!(a, b, "outputs must be deterministic");
    }

    #[test]
    fn deterministic_migrate() {
        let m1 = make_members(30);
        let c: Vec<SpoolCount> = [vec![50; 10], vec![25; 20]].concat();
        let r1 = fresh(&m1, &c);

        let mut m2 = m1.clone();
        for i in 25..30 {
            m2[i] = NodeId(300 + i as u64);
        }
        let a = mig(&r1, &m1, &m2, &c);
        let b = mig(&r1, &m1, &m2, &c);
        assert_eq!(a, b);
    }

    // ----- Group constraint tests -----

    #[test]
    fn one_per_group() {
        let n = 50;
        let members = make_members(n);
        let counts: Vec<SpoolCount> = vec![20; n];
        let result = initial_assignment(&members, &counts).unwrap();
        verify_group_constraints(&result, n);
        verify_counts(&result, &counts);

        for node_idx in 0..n {
            let mut groups = HashSet::new();
            for spool_idx in 0..SPOOL_COUNT {
                if result[spool_idx] as usize == node_idx {
                    groups.insert(spool_idx / SPOOL_GROUP_SIZE);
                }
            }
            assert_eq!(groups.len(), 20, "node {} in wrong number of groups", node_idx);
        }
    }

    // ----- Group structure tests -----

    #[test]
    fn fresh_composition() {
        let m = make_members(20);
        let r = fresh(&m, &uniform(20, 50));
        let expected: Vec<SpoolMapping> = (0..20).collect();
        for g in 0..SPOOL_GROUP_COUNT {
            assert_eq!(group_set(&r, g), expected, "group {}", g);
        }
    }

    #[test]
    fn fresh_saturated() {
        for &n in &[20, 25, 50, 100] {
            let m = make_members(n);
            let stakes: Vec<TAPE> = (1..=n as u64).map(|i| TAPE(i * 100)).collect();
            let c = dhondt_counts(&stakes, SPOOL_COUNT as SpoolCount);
            let r = fresh(&m, &c);
            for i in 0..n {
                assert_eq!(
                    group_count(&r, i as SpoolMapping) as SpoolCount,
                    c[i],
                    "n={} node={}", n, i,
                );
            }
        }
    }

    #[test]
    fn fresh_uniform() {
        let cases: &[(usize, SpoolCount)] = &[(20, 50), (25, 40), (50, 20)];
        for &(n, per) in cases {
            let m = make_members(n);
            let r = fresh(&m, &uniform(n, per));
            for i in 0..n {
                assert_eq!(
                    group_count(&r, i as SpoolMapping),
                    per as usize,
                    "n={} per={} node={}", n, per, i,
                );
            }
        }
    }

    #[test]
    fn fresh_tiers() {
        let m = make_members(25);
        let c: Vec<SpoolCount> = [vec![50; 5], vec![40; 10], vec![35; 10]].concat();
        let r = fresh(&m, &c);
        verify_group_constraints(&r, 25);
        assert_eq!(tally(&r, 25), c);
        for i in 0..5   { assert_eq!(group_count(&r, i as SpoolMapping), 50); }
        for i in 5..15  { assert_eq!(group_count(&r, i as SpoolMapping), 40); }
        for i in 15..25 { assert_eq!(group_count(&r, i as SpoolMapping), 35); }
    }

    #[test]
    fn fresh_minimal() {
        let m = make_members(21);
        let c: Vec<SpoolCount> = [vec![50; 19], vec![49], vec![1]].concat();
        let r = fresh(&m, &c);
        verify_group_constraints(&r, 21);
        assert_eq!(group_count(&r, 20), 1);
        assert_eq!(group_count(&r, 19), 49);
        for i in 0..19 { assert_eq!(group_count(&r, i as SpoolMapping), 50); }
    }

    #[test]
    fn fresh_zeroes() {
        let m = make_members(21);
        let c: Vec<SpoolCount> = [vec![50; 20], vec![0]].concat();
        let r = fresh(&m, &c);
        assert_eq!(group_count(&r, 20), 0);
        assert_eq!(tally(&r, 21)[20], 0);
    }

    #[test]
    fn group_diversity() {
        for &n in &[20, 25, 30, 50, 80, 128] {
            let m = make_members(n);
            let stakes: Vec<TAPE> = (1..=n as u64).map(|i| TAPE(i * 100)).collect();
            let c = dhondt_counts(&stakes, SPOOL_COUNT as SpoolCount);
            let r = fresh(&m, &c);
            for g in 0..SPOOL_GROUP_COUNT {
                let gs = group_set(&r, g);
                let mut deduped = gs.clone();
                deduped.dedup();
                assert_eq!(deduped.len(), SPOOL_GROUP_SIZE, "dup in n={} g={}", n, g);
            }
        }
    }

    // ----- Count accuracy -----

    #[test]
    fn tally_matches() {
        let m = make_members(25);
        let c: Vec<SpoolCount> = [vec![50; 5], vec![40; 10], vec![35; 10]].concat();
        assert_eq!(tally(&fresh(&m, &c), 25), c);

        let m = make_members(30);
        let c: Vec<SpoolCount> = [vec![50; 10], vec![25; 20]].concat();
        assert_eq!(tally(&fresh(&m, &c), 30), c);
    }

    #[test]
    fn tally_survives() {
        let m = make_members(30);
        let s1: Vec<TAPE> = (1..=30u64).map(|i| TAPE(i * 500)).collect();
        let c1 = dhondt_counts(&s1, SPOOL_COUNT as SpoolCount);
        let r1 = fresh(&m, &c1);

        let s2: Vec<TAPE> = (1..=30u64).map(|i| TAPE((31 - i) * 500)).collect();
        let c2 = dhondt_counts(&s2, SPOOL_COUNT as SpoolCount);
        let r2 = mig(&r1, &m, &m, &c2);
        assert_eq!(tally(&r2, 30), c2);
    }

    // ----- Retention & stability -----

    #[test]
    fn identity_sweep() {
        for &n in &[20, 30, 50, 100] {
            let m = make_members(n);
            let stakes: Vec<TAPE> = (1..=n as u64).map(|i| TAPE(i * 100)).collect();
            let c = dhondt_counts(&stakes, SPOOL_COUNT as SpoolCount);
            let r1 = fresh(&m, &c);
            let r2 = mig(&r1, &m, &m, &c);
            assert_eq!(moved(&r1, &m, &r2, &m), 0, "n={}", n);
        }
    }

    #[test]
    fn single_swap() {
        let n = 25;
        let m1 = make_members(n);
        let c = uniform(n, 40);
        let r1 = fresh(&m1, &c);

        let mut m2 = m1.clone();
        m2[n - 1] = NodeId(999);
        let r2 = mig(&r1, &m1, &m2, &c);
        verify_group_constraints(&r2, n);
        assert_eq!(moved(&r1, &m1, &r2, &m2), 40);
    }

    #[test]
    fn double_swap() {
        let n = 25;
        let m1 = make_members(n);
        let c = uniform(n, 40);
        let r1 = fresh(&m1, &c);

        let mut m2 = m1.clone();
        m2[0] = NodeId(900);
        m2[n - 1] = NodeId(901);
        let r2 = mig(&r1, &m1, &m2, &c);
        verify_group_constraints(&r2, n);

        let m = moved(&r1, &m1, &r2, &m2);
        assert!(m >= 80 && m <= 85, "moves={}", m);
    }

    #[test]
    fn survivor_stability() {
        let n = 30;
        let m1 = make_members(n);
        let c: Vec<SpoolCount> = [vec![50; 10], vec![25; 20]].concat();
        let r1 = fresh(&m1, &c);

        let mut m2 = m1.clone();
        for i in 25..30 {
            m2[i] = NodeId(200 + i as u64);
        }
        let r2 = mig(&r1, &m1, &m2, &c);

        for i in 0..SPOOL_COUNT {
            if (r1[i] as usize) < 25 {
                assert_eq!(
                    m1[r1[i] as usize],
                    m2[r2[i] as usize],
                    "spool {} should be retained",
                    i,
                );
            }
        }
    }

    #[test]
    fn stake_stability() {
        let n = 40;
        let members = make_members(n);

        let stakes1: Vec<TAPE> = (1..=n as u64).map(|i| TAPE(i * 1000)).collect();
        let counts1 = dhondt_counts(&stakes1, SPOOL_COUNT as SpoolCount);
        let epoch1 = initial_assignment(&members, &counts1).unwrap();

        let mut stakes2 = stakes1.clone();
        stakes2[n - 1] = stakes2[n - 1] - TAPE(500);
        stakes2[0] = stakes2[0] + TAPE(500);
        let counts2 = dhondt_counts(&stakes2, SPOOL_COUNT as SpoolCount);

        let epoch2 = migrate_spools(&epoch1, &members, &members, &counts2, &Hash::default()).unwrap();
        verify_group_constraints(&epoch2, n);
        verify_counts(&epoch2, &counts2);

        let changes = count_changes(&epoch1, &members, &epoch2, &members);
        let count_diff: usize = counts1
            .iter()
            .zip(counts2.iter())
            .map(|(&a, &b)| (a as i32 - b as i32).unsigned_abs() as usize)
            .sum();
        assert!(
            changes <= count_diff + 10,
            "too many changes: {} (count diff: {})",
            changes,
            count_diff
        );
    }

    // ----- Addition & removal -----

    #[test]
    fn add_nodes() {
        let m1 = make_members(25);
        let c1 = uniform(25, 40);
        let r1 = fresh(&m1, &c1);

        let m2 = make_members(30);
        let s2: Vec<TAPE> = vec![TAPE(1000); 30];
        let c2 = dhondt_counts(&s2, SPOOL_COUNT as SpoolCount);
        let r2 = mig(&r1, &m1, &m2, &c2);
        verify_group_constraints(&r2, 30);
        assert_eq!(tally(&r2, 30), c2);

        let new_spools: usize = c2[25..].iter().map(|&x| x as usize).sum();
        let m = moved(&r1, &m1, &r2, &m2);
        assert!(m >= new_spools, "moves {} < new spools {}", m, new_spools);
    }

    #[test]
    fn remove_half() {
        let m1 = make_members(40);
        let s1: Vec<TAPE> = vec![TAPE(1000); 40];
        let c1 = dhondt_counts(&s1, SPOOL_COUNT as SpoolCount);
        let r1 = fresh(&m1, &c1);

        let m2: Vec<NodeId> = m1[..20].to_vec();
        let c2 = uniform(20, 50);
        let r2 = mig(&r1, &m1, &m2, &c2);
        verify_group_constraints(&r2, 20);
        assert_eq!(tally(&r2, 20), c2);
    }

    #[test]
    fn complete_turnover() {
        let m1 = make_members(25);
        let m2 = ids(&(100..125).collect::<Vec<u64>>());
        let c = uniform(25, 40);
        let r1 = fresh(&m1, &c);
        let r2 = mig(&r1, &m1, &m2, &c);
        verify_group_constraints(&r2, 25);
        assert_eq!(moved(&r1, &m1, &r2, &m2), SPOOL_COUNT);
    }

    // ----- Rebalance -----

    #[test]
    fn rebalance_reversed() {
        let n = 30;
        let m = make_members(n);
        let s1: Vec<TAPE> = (1..=n as u64).map(|i| TAPE(i * 1000)).collect();
        let c1 = dhondt_counts(&s1, SPOOL_COUNT as SpoolCount);
        let r1 = fresh(&m, &c1);

        let s2: Vec<TAPE> = (1..=n as u64).rev().map(|i| TAPE(i * 1000)).collect();
        let c2 = dhondt_counts(&s2, SPOOL_COUNT as SpoolCount);
        let r2 = mig(&r1, &m, &m, &c2);
        verify_group_constraints(&r2, n);
        assert_eq!(tally(&r2, n), c2);

        let delta: usize = c1
            .iter()
            .zip(&c2)
            .map(|(&a, &b)| (a as i32 - b as i32).unsigned_abs() as usize)
            .sum();
        assert!(moved(&r1, &m, &r2, &m) <= delta, "moves exceed delta");
    }

    // ----- Node ID patterns -----

    #[test]
    fn nonsequential_fresh() {
        let m = ids(&[
            10, 99, 3, 500, 7, 42, 1000, 8, 2, 55,
            11, 88, 4, 501, 6, 43, 1001, 9, 1, 56,
        ]);
        let r = fresh(&m, &uniform(20, 50));
        verify_group_constraints(&r, 20);
        assert_eq!(tally(&r, 20), uniform(20, 50));
    }

    #[test]
    fn nonsequential_migrate() {
        let m1 = ids(&[
            10, 99, 3, 500, 7, 42, 1000, 8, 2, 55,
            11, 88, 4, 501, 6, 43, 1001, 9, 1, 56,
        ]);
        let r1 = fresh(&m1, &uniform(20, 50));

        let mut m2 = m1.clone();
        m2[0] = NodeId(777);
        m2[19] = NodeId(888);
        let r2 = mig(&r1, &m1, &m2, &uniform(20, 50));
        verify_group_constraints(&r2, 20);
        assert_eq!(moved(&r1, &m1, &r2, &m2), 100);
    }

    // ----- Epoch chains -----

    #[test]
    fn grow_shrink() {
        let m20 = make_members(20);
        let r1 = fresh(&m20, &uniform(20, 50));

        let m50 = make_members(50);
        let r2 = mig(&r1, &m20, &m50, &uniform(50, 20));
        verify_group_constraints(&r2, 50);

        let r3 = mig(&r2, &m50, &m20, &uniform(20, 50));
        verify_group_constraints(&r3, 20);
        assert_eq!(tally(&r3, 20), uniform(20, 50));
    }

    #[test]
    fn rolling_replace() {
        let n = 25;
        let mut m = make_members(n);
        let c = uniform(n, 40);
        let mut r = fresh(&m, &c);

        for epoch in 1u64..=5 {
            let mut m_next = m.clone();
            for i in 0..5 {
                m_next[i] = NodeId(epoch * 100 + i as u64);
            }
            let r_next = mig(&r, &m, &m_next, &c);
            verify_group_constraints(&r_next, n);
            assert_eq!(tally(&r_next, n), c);
            let mv = moved(&r, &m, &r_next, &m_next);
            assert!(mv >= 200 && mv <= 210, "epoch {} moves={}", epoch, mv);

            m = m_next;
            r = r_next;
        }
    }

    // ----- Large-scale test -----

    #[test]
    fn epoch_large() {
        let n = 128;
        let mut members = make_members(n);
        let stakes: Vec<TAPE> = (1..=n as u64).map(|i| TAPE(i * 100)).collect();
        let counts = dhondt_counts(&stakes, SPOOL_COUNT as SpoolCount);
        let mut current = initial_assignment(&members, &counts).unwrap();
        verify_group_constraints(&current, n);
        verify_counts(&current, &counts);

        for epoch in 0..3 {
            let offset = (epoch + 1) * 10;
            let new_members: Vec<NodeId> =
                (offset as u64 + 1..=offset as u64 + n as u64).map(NodeId).collect();
            let new_stakes: Vec<TAPE> = (1..=n as u64).map(|i| TAPE(i * 100)).collect();
            let new_counts = dhondt_counts(&new_stakes, SPOOL_COUNT as SpoolCount);

            let next =
                migrate_spools(&current, &members, &new_members, &new_counts, &Hash::default())
                    .unwrap();
            verify_group_constraints(&next, n);
            verify_counts(&next, &new_counts);

            current = next;
            members = new_members;
        }
    }

    // ----- Must-take / eviction integration tests -----

    #[test]
    fn forced_take() {
        let n = 20;
        let members = make_members(n);
        let counts: Vec<SpoolCount> = vec![50; n];
        let result = initial_assignment(&members, &counts).unwrap();
        verify_group_constraints(&result, n);
        verify_counts(&result, &counts);

        for node_idx in 0..n {
            for group in 0..SPOOL_GROUP_COUNT {
                let found = (0..SPOOL_GROUP_SIZE).any(|offset| {
                    result[group * SPOOL_GROUP_SIZE + offset] as usize == node_idx
                });
                assert!(found, "node {} missing from group {}", node_idx, group);
            }
        }
    }

    #[test]
    fn eviction() {
        let n = 25;
        let members1 = make_members(n);

        let mut stakes1: Vec<TAPE> = vec![TAPE(100); n];
        for i in 0..5 {
            stakes1[i] = TAPE(10_000);
        }
        let counts1 = dhondt_counts(&stakes1, SPOOL_COUNT as SpoolCount);
        let epoch1 = initial_assignment(&members1, &counts1).unwrap();
        verify_group_constraints(&epoch1, n);

        let mut stakes2: Vec<TAPE> = vec![TAPE(100); n];
        for i in (n - 5)..n {
            stakes2[i] = TAPE(10_000);
        }
        let counts2 = dhondt_counts(&stakes2, SPOOL_COUNT as SpoolCount);

        let epoch2 = migrate_spools(&epoch1, &members1, &members1, &counts2, &Hash::default()).unwrap();
        verify_group_constraints(&epoch2, n);
        verify_counts(&epoch2, &counts2);
    }

    // ----- Error paths -----

    #[test]
    fn err_bad_index() {
        let cur_members = make_members(20);
        let next_members = make_members(20);
        let counts = uniform(20, 50);
        let mut spools = vec![0 as SpoolMapping; SPOOL_COUNT];
        spools[0] = 25;
        assert_eq!(
            migrate_spools(&spools, &cur_members, &next_members, &counts, &Hash::default()).unwrap_err(),
            SpoolerError::BadIndex,
        );
    }

    // ----- Manual spool-array tests -----

    #[test]
    fn manual_identity_20() {
        let spools = round_robin(20);
        let m = make_members(20);
        let result = mig(&spools, &m, &m, &uniform(20, 50));
        assert_eq!(result, spools);
    }

    #[test]
    fn manual_identity_25() {
        let spools = round_robin(25);
        let m = make_members(25);
        let result = mig(&spools, &m, &m, &uniform(25, 40));
        assert_eq!(result, spools);
    }

    #[test]
    fn manual_identity_50() {
        let spools = round_robin(50);
        let m = make_members(50);
        let result = mig(&spools, &m, &m, &uniform(50, 20));
        assert_eq!(result, spools);
    }

    #[test]
    fn manual_interleaved() {
        let spools: Vec<SpoolMapping> = (0..SPOOL_COUNT)
            .map(|i| (i % SPOOL_GROUP_SIZE) as SpoolMapping)
            .collect();
        let m = make_members(20);
        let result = mig(&spools, &m, &m, &uniform(20, 50));
        assert_eq!(result, spools);
    }

    #[test]
    fn manual_swap_one() {
        let spools = round_robin(20);
        let m1 = make_members(20);
        let mut m2 = m1.clone();
        m2[19] = NodeId(999);
        let result = mig(&spools, &m1, &m2, &uniform(20, 50));

        for i in 0..SPOOL_COUNT {
            assert_eq!(result[i], spools[i], "spool {}", i);
        }
    }

    #[test]
    fn manual_swap_survivors() {
        let spools = round_robin(25);
        let m1 = make_members(25);
        let mut m2 = m1.clone();
        m2[24] = NodeId(999);
        let result = mig(&spools, &m1, &m2, &uniform(25, 40));
        verify_group_constraints(&result, 25);

        for i in 0..SPOOL_COUNT {
            if spools[i] != 24 {
                assert_eq!(result[i], spools[i], "spool {} retained", i);
            }
        }
        assert_eq!(tally(&result, 25)[24], 40);
    }

    #[test]
    fn manual_swap_two() {
        let spools = round_robin(20);
        let m1 = make_members(20);
        let mut m2 = m1.clone();
        m2[0] = NodeId(900);
        m2[19] = NodeId(901);
        let result = mig(&spools, &m1, &m2, &uniform(20, 50));
        verify_group_constraints(&result, 20);

        for i in 0..SPOOL_COUNT {
            if spools[i] >= 1 && spools[i] <= 18 {
                assert_eq!(result[i], spools[i], "spool {} retained", i);
            }
        }
        assert_eq!(tally(&result, 20), uniform(20, 50));
    }

    #[test]
    fn manual_single_owner() {
        let prev = vec![0 as SpoolMapping; SPOOL_COUNT];
        let prev_m = vec![NodeId(1)];
        let next_m = make_members(20);
        let result = migrate_spools(&prev, &prev_m, &next_m, &uniform(20, 50), &Hash::default()).unwrap();
        verify_group_constraints(&result, 20);

        for g in 0..SPOOL_GROUP_COUNT {
            assert_eq!(
                result[g * SPOOL_GROUP_SIZE], 0,
                "group {} slot 0 retained by node 0", g,
            );
        }
        assert_eq!(tally(&result, 20)[0], 50);
    }

    #[test]
    fn manual_block_retention() {
        let prev = sequential_blocks(50);
        let m = make_members(20);
        let result = mig(&prev, &m, &m, &uniform(20, 50));
        verify_group_constraints(&result, 20);
        assert_eq!(tally(&result, 20), uniform(20, 50));

        for k in 0..20 {
            assert_eq!(
                result[k * 50] as usize, k,
                "node {}'s first spool retained", k,
            );
        }
    }

    #[test]
    fn manual_block_pairs() {
        let prev: Vec<SpoolMapping> = (0..SPOOL_COUNT)
            .map(|i| {
                let block = i / 100;
                let half = (i % 100) / 50;
                (block * 2 + half) as SpoolMapping
            })
            .collect();
        let m = make_members(20);
        let result = mig(&prev, &m, &m, &uniform(20, 50));
        verify_group_constraints(&result, 20);
        assert_eq!(tally(&result, 20), uniform(20, 50));

        for k in 0..20 {
            let first_spool = if k % 2 == 0 { (k / 2) * 100 } else { (k / 2) * 100 + 50 };
            assert_eq!(
                result[first_spool] as usize, k,
                "node {}'s first spool at {} retained", k, first_spool,
            );
        }
    }

    #[test]
    fn manual_grow() {
        let spools = round_robin(20);
        let m1 = make_members(20);
        let m2 = make_members(25);
        let c2 = uniform(25, 40);
        let result = mig(&spools, &m1, &m2, &c2);
        verify_group_constraints(&result, 25);
        assert_eq!(tally(&result, 25), c2);

        let retained: usize = (0..SPOOL_COUNT)
            .filter(|&i| (result[i] as usize) < 20 && result[i] == spools[i])
            .count();
        assert!(
            retained >= 725 && retained <= 800,
            "expected high retention during committee growth, got {}",
            retained,
        );
    }

    #[test]
    fn manual_shrink() {
        let spools = round_robin(20);
        let m1 = make_members(20);
        let m2 = ids(&(101..=120).collect::<Vec<u64>>());
        let result = mig(&spools, &m1, &m2, &uniform(20, 50));
        verify_group_constraints(&result, 20);

        assert_eq!(moved(&spools, &m1, &result, &m2), SPOOL_COUNT);
    }

    #[test]
    fn manual_group_slices() {
        let spools = round_robin(20);
        for g in 0..SPOOL_GROUP_COUNT {
            let base = g * SPOOL_GROUP_SIZE;
            let expected: Vec<SpoolMapping> = (0..SPOOL_GROUP_SIZE)
                .map(|s| ((g + s) % 20) as SpoolMapping)
                .collect();
            let actual: Vec<SpoolMapping> = (0..SPOOL_GROUP_SIZE)
                .map(|s| spools[base + s])
                .collect();
            assert_eq!(actual, expected, "group {} raw content", g);
        }

        let m = make_members(20);
        let result = mig(&spools, &m, &m, &uniform(20, 50));
        for g in 0..SPOOL_GROUP_COUNT {
            let base = g * SPOOL_GROUP_SIZE;
            for s in 0..SPOOL_GROUP_SIZE {
                assert_eq!(
                    result[base + s], spools[base + s],
                    "group {} slot {} mismatch", g, s,
                );
            }
        }
    }

    #[test]
    fn manual_partial_overlap() {
        let spools = round_robin(25);
        let m1 = make_members(25);
        let mut m2 = m1.clone();
        for i in 20..25 {
            m2[i] = NodeId(100 + i as u64);
        }
        let result = mig(&spools, &m1, &m2, &uniform(25, 40));
        verify_group_constraints(&result, 25);
        assert_eq!(tally(&result, 25), uniform(25, 40));

        let m = moved(&spools, &m1, &result, &m2);
        assert!(m >= 200, "too few moves: {}", m);
        assert!(m <= 250, "too many moves: {}", m);
    }

    #[test]
    fn manual_half_replaced() {
        let spools = round_robin(25);
        let m1 = make_members(25);
        let mut m2 = m1.clone();
        for i in 12..25 {
            m2[i] = NodeId(100 + i as u64);
        }
        let result = mig(&spools, &m1, &m2, &uniform(25, 40));
        verify_group_constraints(&result, 25);
        assert_eq!(tally(&result, 25), uniform(25, 40));

        let m = moved(&spools, &m1, &result, &m2);
        assert!(m >= 520, "too few moves: {}", m);
        assert!(m <= 600, "too many moves: {}", m);
    }

    // ----- Stress / edge-case tests -----

    #[test]
    fn stress_must_take() {
        let n = 20;
        let members = make_members(n);
        let counts = uniform(n, 50);
        let result = initial_assignment(&members, &counts).unwrap();
        verify_group_constraints(&result, n);
        verify_counts(&result, &counts);

        for ni in 0..n {
            for g in 0..SPOOL_GROUP_COUNT {
                let base = g * SPOOL_GROUP_SIZE;
                let found = (0..SPOOL_GROUP_SIZE).any(|s| result[base + s] as usize == ni);
                assert!(found, "node {} missing from group {}", ni, g);
            }
        }
    }

    #[test]
    fn stress_grow() {
        let m1 = make_members(20);
        let c1 = uniform(20, 50);
        let r1 = fresh(&m1, &c1);

        let n2 = 128;
        let m2 = make_members(n2);
        let stakes2: Vec<TAPE> = (1..=n2 as u64).map(|i| TAPE(i * 100)).collect();
        let c2 = dhondt_counts(&stakes2, SPOOL_COUNT as SpoolCount);

        let r2 = migrate_spools(&r1, &m1, &m2, &c2, &Hash::default()).unwrap();
        verify_group_constraints(&r2, n2);
        verify_counts(&r2, &c2);

        for g in 0..SPOOL_GROUP_COUNT {
            let mut seen = HashSet::new();
            let base = g * SPOOL_GROUP_SIZE;
            for s in 0..SPOOL_GROUP_SIZE {
                seen.insert(r2[base + s]);
            }
            assert_eq!(seen.len(), SPOOL_GROUP_SIZE, "group {} has duplicates", g);
        }

        for i in 0..n2 {
            assert_eq!(
                group_count(&r2, i as SpoolMapping) as SpoolCount,
                c2[i],
                "node {} group count mismatch",
                i,
            );
        }

        assert_eq!(
            c2.iter().map(|&x| x as usize).sum::<usize>(),
            SPOOL_COUNT,
        );
    }

    #[test]
    fn stress_full_churn() {
        let n = 128;
        let m1 = make_members(n);
        let stakes: Vec<TAPE> = (1..=n as u64).map(|i| TAPE(i * 100)).collect();
        let c = dhondt_counts(&stakes, SPOOL_COUNT as SpoolCount);
        let r1 = fresh(&m1, &c);

        let m2: Vec<NodeId> = (1001..=1128).map(NodeId).collect();
        let r2 = migrate_spools(&r1, &m1, &m2, &c, &Hash::default()).unwrap();
        verify_group_constraints(&r2, n);
        verify_counts(&r2, &c);
        assert_eq!(moved(&r1, &m1, &r2, &m2), SPOOL_COUNT);
    }

    #[test]
    fn stress_rebalance() {
        let n = 128;
        let m = make_members(n);
        let s1: Vec<TAPE> = (1..=n as u64).map(|i| TAPE(i * 1000)).collect();
        let c1 = dhondt_counts(&s1, SPOOL_COUNT as SpoolCount);
        let r1 = fresh(&m, &c1);

        let s2: Vec<TAPE> = (1..=n as u64).rev().map(|i| TAPE(i * 1000)).collect();
        let c2 = dhondt_counts(&s2, SPOOL_COUNT as SpoolCount);
        let r2 = migrate_spools(&r1, &m, &m, &c2, &Hash::default()).unwrap();
        verify_group_constraints(&r2, n);
        verify_counts(&r2, &c2);

        for g in 0..SPOOL_GROUP_COUNT {
            let mut seen = HashSet::new();
            let base = g * SPOOL_GROUP_SIZE;
            for s in 0..SPOOL_GROUP_SIZE {
                seen.insert(r2[base + s]);
            }
            assert_eq!(seen.len(), SPOOL_GROUP_SIZE, "group {} has duplicates", g);
        }

        for i in 0..n {
            assert_eq!(
                group_count(&r2, i as SpoolMapping) as SpoolCount,
                c2[i],
                "node {} group count mismatch",
                i,
            );
        }
    }

    #[test]
    fn stress_epoch_bucket() {
        let n = 50;
        let mut members = make_members(n);
        let stakes: Vec<TAPE> = (1..=n as u64).map(|i| TAPE(i * 500)).collect();
        let counts = dhondt_counts(&stakes, SPOOL_COUNT as SpoolCount);
        let mut current = initial_assignment(&members, &counts).unwrap();
        verify_group_constraints(&current, n);
        verify_counts(&current, &counts);

        let churn_amounts = [5, 10, 15, 20, 25];
        for (epoch, &churn) in churn_amounts.iter().enumerate() {
            let mut new_members = members.clone();
            for i in 0..churn {
                new_members[i] = NodeId((epoch as u64 + 1) * 1000 + i as u64);
            }
            let new_stakes: Vec<TAPE> = (1..=n as u64).map(|i| TAPE(i * 500)).collect();
            let new_counts = dhondt_counts(&new_stakes, SPOOL_COUNT as SpoolCount);

            let next =
                migrate_spools(&current, &members, &new_members, &new_counts, &Hash::default())
                    .unwrap();
            verify_group_constraints(&next, n);
            verify_counts(&next, &new_counts);

            for g in 0..SPOOL_GROUP_COUNT {
                let mut seen = HashSet::new();
                let base = g * SPOOL_GROUP_SIZE;
                for s in 0..SPOOL_GROUP_SIZE {
                    seen.insert(next[base + s]);
                }
                assert_eq!(
                    seen.len(), SPOOL_GROUP_SIZE,
                    "epoch {} group {} has duplicates", epoch + 1, g,
                );
            }

            current = next;
            members = new_members;
        }
    }
}
