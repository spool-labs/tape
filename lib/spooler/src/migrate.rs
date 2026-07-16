//! Epoch-over-epoch spool migration.
//!
//! Spools are partitioned into `group_count` groups of 20. 
//! Each group must map its 20 spools to that many *distinct* nodes, so no node 
//! holds more than one spool per group.
//!
//! Migration runs per-group in three phases:
//!
//! 1. **Retention** - spools whose previous owner is still present in the next
//!    epoch and still has capacity are kept in place, minimising churn.
//!
//! 2. **Must-take with eviction** - nodes that *must* receive a spool in this
//!    group (their target count has not been met by any prior group) claim a
//!    free slot. If no free slot exists, the least-critical retained spool is
//!    evicted to make room.
//!
//! 3. **Fill remaining** - any slots still unassigned are handed out via a
//!    max-heap ordered by (remaining need, target, address) to the nodes that
//!    have capacity left and are not yet used in this group.
//!
//! The spooler runs off-chain. Determinism is load-bearing: the same input
//! addresses, counts, and seed `Hash` must produce bit-identical output across
//! platforms. No `HashMap` iteration, no float ops, no `rayon`.

use std::collections::BTreeMap;

use tape_core::erasure::GROUP_SIZE;
use tape_core::spooler::SpoolerError;
use tape_core::types::SpoolCount;
use tape_crypto::address::Address;
use tape_crypto::hash::{Hash, hashv};

const MIN_NODES: usize = GROUP_SIZE;

type NodeIndex = u32;
type SpoolOffset = usize;

/// Reassign spools from the current epoch to the next epoch with group
/// constraints and minimal disruption. Returns one `Address` per spool slot.
pub fn migrate_spools(
    group_count: usize,
    current_spools: &[Option<Address>],
    next_addresses: &[Address],
    next_spool_counts: &[SpoolCount],
    seed: &Hash,
) -> Result<Vec<Address>, SpoolerError> {
    validate(group_count, current_spools, next_addresses, next_spool_counts)?;

    let spool_count = group_count * GROUP_SIZE;

    let nodes = build_node_states(next_addresses, next_spool_counts);
    let prev_owner = build_previous_owners(current_spools, next_addresses);

    let (retain_mask, planned_retentions) =
        compute_retain_masks(&nodes, &prev_owner, seed, group_count);

    let retain_nodes_per_group = build_retain_nodes_per_group(&retain_mask, group_count);

    let buckets = RemainingBuckets::new(&nodes, group_count);
    let num_next = next_addresses.len();

    let result = vec![0 as NodeIndex; spool_count];
    let retained = Vec::with_capacity(GROUP_SIZE);
    let unassigned = Vec::with_capacity(GROUP_SIZE);
    let must_take = Vec::with_capacity(GROUP_SIZE);
    let candidates = Vec::with_capacity(num_next);

    let mut ctx = MigrationContext {
        group_count,
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

    for group in 0..group_count {
        let remaining_groups = group_count - group;

        debug_assert!(
            ctx.nodes
                .iter()
                .all(|n| n.remaining.as_usize() <= remaining_groups),
            "infeasibility: node remaining > remaining_groups",
        );

        let mut used = NodeSet::with_node_count(num_next);
        ctx.retain(group, &mut used);
        ctx.take(group, remaining_groups, &mut used)?;
        ctx.fill(group, &mut used)?;
    }

    ctx.verify()?;

    Ok(ctx
        .result
        .into_iter()
        .map(|i| next_addresses[i as usize])
        .collect())
}

/// Create an initial spool assignment that satisfies group constraints.
///
/// Equivalent to `migrate_spools` with no prior state.
pub fn initial_assignment(
    group_count: usize,
    next_addresses: &[Address],
    next_spool_counts: &[SpoolCount],
) -> Result<Vec<Address>, SpoolerError> {
    let spool_count = group_count * GROUP_SIZE;
    let dummy_current: Vec<Option<Address>> = vec![None; spool_count];

    migrate_spools(
        group_count, 
        &dummy_current, 
        next_addresses, 
        next_spool_counts, 
        &Hash::default()
    )
}

/// Dynamic per-node bitmask, sized to the active committee.
struct NodeSet {
    words: Vec<u64>,
}

impl NodeSet {
    fn with_node_count(n: usize) -> Self {
        let word_count = (n + 63) / 64;
        Self { words: vec![0u64; word_count] }
    }

    #[inline]
    fn test(&self, index: usize) -> bool {
        let (word, bit) = (index / 64, index % 64);
        (self.words[word] >> bit) & 1 == 1
    }

    #[inline]
    fn set(&mut self, index: usize) {
        let (word, bit) = (index / 64, index % 64);
        self.words[word] |= 1u64 << bit;
    }

    #[inline]
    fn clear(&mut self, index: usize) {
        let (word, bit) = (index / 64, index % 64);
        self.words[word] &= !(1u64 << bit);
    }
}

/// Per-node bitmask over the active group set. Same shape as `NodeSet` but the
/// bit space is groups, not committee members.
#[derive(Clone, PartialEq, Eq)]
struct GroupSet {
    words: Vec<u64>,
}

impl GroupSet {
    fn with_group_count(n: usize) -> Self {
        let word_count = (n + 63) / 64;
        Self { words: vec![0u64; word_count] }
    }

    #[inline]
    fn test(&self, group: usize) -> bool {
        let (word, bit) = (group / 64, group % 64);
        (self.words[word] >> bit) & 1 == 1
    }

    #[inline]
    fn set(&mut self, group: usize) {
        let (word, bit) = (group / 64, group % 64);
        self.words[word] |= 1u64 << bit;
    }

    #[inline]
    fn count_ones(&self) -> u32 {
        self.words.iter().map(|w| w.count_ones()).sum()
    }

    /// Index of the lowest set bit, if any.
    #[inline]
    fn trailing_zeros(&self) -> Option<usize> {
        for (i, w) in self.words.iter().enumerate() {
            if *w != 0 {
                return Some(i * 64 + w.trailing_zeros() as usize);
            }
        }
        None
    }

    /// Clears the lowest set bit. No-op if all bits are zero.
    #[inline]
    fn clear_lowest(&mut self) {
        for w in &mut self.words {
            if *w != 0 {
                *w &= w.wrapping_sub(1);
                return;
            }
        }
    }
}

fn rotate_groups_left(x: &GroupSet, r: u32, group_count: usize) -> GroupSet {
    let mut out = GroupSet::with_group_count(group_count);
    if group_count == 0 {
        return out;
    }
    let r = (r as usize) % group_count;
    for i in 0..group_count {
        let src = (i + group_count - r) % group_count;
        if x.test(src) {
            out.set(i);
        }
    }
    out
}

fn rotate_groups_right(x: &GroupSet, r: u32, group_count: usize) -> GroupSet {
    let mut out = GroupSet::with_group_count(group_count);
    if group_count == 0 {
        return out;
    }
    let r = (r as usize) % group_count;
    for i in 0..group_count {
        let src = (i + r) % group_count;
        if x.test(src) {
            out.set(i);
        }
    }
    out
}

fn take_k_lowest_bits(x: &GroupSet, k: u32, group_count: usize) -> GroupSet {
    let mut out = GroupSet::with_group_count(group_count);
    let mut iter = x.clone();
    let mut remaining = k;
    while remaining > 0 {
        match iter.trailing_zeros() {
            Some(pos) => {
                out.set(pos);
                iter.clear_lowest();
                remaining -= 1;
            }
            None => break,
        }
    }
    out
}

/// Deterministic per-node offset into the group ring, derived from address and seed.
///
/// The seed is typically the slot hash captured at `commit_epoch`, making the
/// offset unpredictable until the epoch boundary executes on-chain.
#[inline]
fn group_offset(address: Address, seed: &Hash, group_count: usize) -> u32 {
    let h = hashv(&[seed.as_ref(), address.as_ref()]);
    let val = u64::from_le_bytes(h.0[..8].try_into().unwrap());
    (val % (group_count as u64)) as u32
}

struct NodeState {
    address: Address,
    target: SpoolCount,
    remaining: SpoolCount,
}

impl NodeState {
    #[inline]
    fn can_accept(&self, index: usize, used: &NodeSet) -> bool {
        self.remaining > SpoolCount(0) && !used.test(index)
    }
}

/// Priority: higher remaining first, then higher target (stake proxy), then lower address.
#[derive(Eq, PartialEq, Copy, Clone)]
struct FillEntry {
    remaining: SpoolCount,
    target: SpoolCount,
    address: Address,
    node_index: NodeIndex,
}

impl Ord for FillEntry {
    #[inline]
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.remaining
            .cmp(&other.remaining)
            .then_with(|| self.target.cmp(&other.target))
            .then_with(|| other.address.cmp(&self.address))
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
    buckets: Vec<Vec<NodeIndex>>,
    positions: Vec<u32>,
}

impl RemainingBuckets {
    fn new(nodes: &[NodeState], group_count: usize) -> Self {
        let bucket_count = group_count + 1;
        let mut buckets: Vec<Vec<NodeIndex>> =
            (0..bucket_count).map(|_| Vec::new()).collect();
        let mut positions = vec![0u32; nodes.len()];
        for (i, node) in nodes.iter().enumerate() {
            let r = node.remaining.as_usize();
            positions[i] = buckets[r].len() as u32;
            buckets[r].push(i as NodeIndex);
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
        let old_bucket = &mut self.buckets[old_remaining.as_usize()];
        let pos = self.positions[node_index as usize] as usize;
        old_bucket.swap_remove(pos);
        if pos < old_bucket.len() {
            self.positions[old_bucket[pos] as usize] = pos as u32;
        }
        let new_bucket = &mut self.buckets[new_remaining.as_usize()];
        self.positions[node_index as usize] = new_bucket.len() as u32;
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

    let a_was_critical = a_node.remaining.next().as_usize() == remaining_groups;
    let b_was_critical = b_node.remaining.next().as_usize() == remaining_groups;

    b_was_critical
        .cmp(&a_was_critical)
        .then_with(|| b_node.remaining.cmp(&a_node.remaining))
        .then_with(|| b_node.target.cmp(&a_node.target))
        .then_with(|| a_node.address.cmp(&b_node.address))
}

struct MigrationContext {
    group_count: usize,
    nodes: Vec<NodeState>,
    prev_owner: Vec<Option<NodeIndex>>,
    retain_mask: Vec<GroupSet>,
    planned_retentions: Vec<SpoolCount>,
    retain_nodes_per_group: Vec<Vec<NodeIndex>>,
    buckets: RemainingBuckets,
    result: Vec<NodeIndex>,
    retained: Vec<RetainedEntry>,
    unassigned: Vec<SpoolOffset>,
    must_take: Vec<NodeIndex>,
    candidates: Vec<FillEntry>,
}

impl MigrationContext {
    fn retain(&mut self, group: usize, used: &mut NodeSet) {
        let group_start = group * GROUP_SIZE;

        self.retained.clear();
        self.unassigned.clear();

        for &node_index in &self.retain_nodes_per_group[group] {
            let ni = node_index as usize;
            self.planned_retentions[ni] = self.planned_retentions[ni].prev();
        }

        for offset in 0..GROUP_SIZE {
            let spool = group_start + offset;
            let mut kept = false;

            if let Some(prev_node) = self.prev_owner[spool] {
                let ni = prev_node as usize;
                if self.retain_mask[ni].test(group)
                    && self.nodes[ni].can_accept(ni, used)
                {
                    self.result[spool] = prev_node;
                    let old_remaining = self.nodes[ni].remaining;
                    let new_remaining = old_remaining
                        .checked_prev()
                        .expect("remaining spool count underflow");
                    self.nodes[ni].remaining = new_remaining;
                    self.buckets.move_node(prev_node, old_remaining, new_remaining);
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
        let group_start = group * GROUP_SIZE;

        for _iteration in 0..=GROUP_SIZE {
            self.must_take.clear();
            if remaining_groups <= self.group_count {
                for &node_index in self.buckets.nodes_with_remaining(remaining_groups) {
                    if !used.test(node_index as usize) {
                        self.must_take.push(node_index);
                    }
                }
            }

            if self.must_take.len() > GROUP_SIZE {
                return Err(SpoolerError::Infeasible);
            }

            if self.must_take.len() <= self.unassigned.len() {
                for &node_index in &self.must_take {
                    let offset = self.unassigned.pop().ok_or(SpoolerError::Infeasible)?;
                    let spool = group_start + offset;
                    let ni = node_index as usize;
                    self.result[spool] = node_index;
                    let old_remaining = self.nodes[ni].remaining;
                    let new_remaining = old_remaining
                        .checked_prev()
                        .expect("remaining spool count underflow");
                    self.nodes[ni].remaining = new_remaining;
                    self.buckets.move_node(node_index, old_remaining, new_remaining);
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
                let new_remaining = old_remaining
                    .checked_next()
                    .expect("remaining spool count overflow");
                self.nodes[ni].remaining = new_remaining;
                self.buckets
                    .move_node(entry.node_index, old_remaining, new_remaining);
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
        let group_start = group * GROUP_SIZE;
        let slots_needed = self.unassigned.len();
        if slots_needed == 0 {
            return Ok(());
        }

        self.candidates.clear();
        'primary: for r in (1..=self.group_count).rev() {
            for &node_index in self.buckets.nodes_with_remaining(r) {
                let ni = node_index as usize;
                if self.nodes[ni].remaining > self.planned_retentions[ni] && !used.test(ni) {
                    self.candidates.push(FillEntry {
                        remaining: self.nodes[ni].remaining,
                        target: self.nodes[ni].target,
                        address: self.nodes[ni].address,
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
            'fallback: for r in (1..=self.group_count).rev() {
                for &node_index in self.buckets.nodes_with_remaining(r) {
                    let ni = node_index as usize;
                    if self.nodes[ni].can_accept(ni, used) {
                        self.candidates.push(FillEntry {
                            remaining: self.nodes[ni].remaining,
                            target: self.nodes[ni].target,
                            address: self.nodes[ni].address,
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
            let new_remaining = old_remaining
                .checked_prev()
                .expect("remaining spool count underflow");
            self.nodes[ni].remaining = new_remaining;
            self.buckets
                .move_node(entry.node_index, old_remaining, new_remaining);
            used.set(ni);
        }

        Ok(())
    }

    fn verify(&self) -> Result<(), SpoolerError> {
        for node in &self.nodes {
            if node.remaining != SpoolCount(0) {
                return Err(SpoolerError::Infeasible);
            }
        }
        Ok(())
    }
}

fn validate(
    group_count: usize,
    current_spools: &[Option<Address>],
    next_addresses: &[Address],
    next_spool_counts: &[SpoolCount],
) -> Result<(), SpoolerError> {
    let spool_count = group_count * GROUP_SIZE;
    if current_spools.len() != spool_count {
        return Err(SpoolerError::TotalMismatch);
    }
    if next_addresses.len() != next_spool_counts.len() {
        return Err(SpoolerError::CountMismatch);
    }
    if next_addresses.len() < MIN_NODES {
        return Err(SpoolerError::InsufficientNodes);
    }
    let total: usize = next_spool_counts.iter().map(|c| c.as_usize()).sum();
    if total != spool_count {
        return Err(SpoolerError::TotalMismatch);
    }
    let max_spools_per_node = SpoolCount(group_count as u64);
    for &count in next_spool_counts {
        if count > max_spools_per_node {
            return Err(SpoolerError::SpoolCapExceeded);
        }
    }
    Ok(())
}

fn build_node_states(
    next_addresses: &[Address],
    next_spool_counts: &[SpoolCount],
) -> Vec<NodeState> {
    next_addresses
        .iter()
        .zip(next_spool_counts)
        .map(|(&address, &target)| NodeState {
            address,
            target,
            remaining: target,
        })
        .collect()
}

fn build_previous_owners(
    current_spools: &[Option<Address>],
    next_addresses: &[Address],
) -> Vec<Option<NodeIndex>> {
    let lookup: BTreeMap<Address, NodeIndex> = next_addresses
        .iter()
        .enumerate()
        .map(|(i, &a)| (a, i as NodeIndex))
        .collect();

    current_spools
        .iter()
        .map(|slot| slot.and_then(|a| lookup.get(&a).copied()))
        .collect()
}

fn compute_retain_masks(
    nodes: &[NodeState],
    prev_owner: &[Option<NodeIndex>],
    seed: &Hash,
    group_count: usize,
) -> (Vec<GroupSet>, Vec<SpoolCount>) {
    let num_next = nodes.len();

    let mut previous_groups: Vec<GroupSet> =
        (0..num_next).map(|_| GroupSet::with_group_count(group_count)).collect();
    for (spool, owner) in prev_owner.iter().enumerate() {
        if let Some(node_index) = *owner {
            let group = spool / GROUP_SIZE;
            previous_groups[node_index as usize].set(group);
        }
    }

    let mut retain_mask: Vec<GroupSet> =
        (0..num_next).map(|_| GroupSet::with_group_count(group_count)).collect();
    for i in 0..num_next {
        let available = &previous_groups[i];
        let keep = (available.count_ones() as u64).min(nodes[i].target.as_u64()) as u32;
        if keep == 0 {
            continue;
        }
        let offset = group_offset(nodes[i].address, seed, group_count);
        let rotated = rotate_groups_right(available, offset, group_count);
        let picked = take_k_lowest_bits(&rotated, keep, group_count);
        retain_mask[i] = rotate_groups_left(&picked, offset, group_count);
    }

    let planned_retentions: Vec<SpoolCount> = retain_mask
        .iter()
        .map(|mask| SpoolCount(mask.count_ones() as u64))
        .collect();

    (retain_mask, planned_retentions)
}

fn build_retain_nodes_per_group(retain_mask: &[GroupSet], group_count: usize) -> Vec<Vec<NodeIndex>> {
    let mut per_group: Vec<Vec<NodeIndex>> = vec![vec![]; group_count];
    for (node_index, mask) in retain_mask.iter().enumerate() {
        let mut remaining_mask = mask.clone();
        while let Some(group) = remaining_mask.trailing_zeros() {
            per_group[group].push(node_index as NodeIndex);
            remaining_mask.clear_lowest();
        }
    }
    per_group
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::dhondt::DhondtSpooler;
    use std::collections::HashSet;
    use tape_core::types::TAPE;

    const SPOOL_GROUP_COUNT: usize = 50;
    const SPOOL_COUNT: usize = SPOOL_GROUP_COUNT * GROUP_SIZE;

    fn addr(seed: u64) -> Address {
        let mut bytes = [0u8; 32];
        bytes[..8].copy_from_slice(&seed.to_le_bytes());
        Address::new(bytes)
    }

    fn make_addresses(count: usize) -> Vec<Address> {
        (1..=count as u64).map(addr).collect()
    }

    fn dhondt_counts(stakes: &[TAPE], total: SpoolCount) -> Vec<SpoolCount> {
        DhondtSpooler::default().allocate(stakes, total).unwrap()
    }

    fn verify_group_constraints(result: &[Address], expected_addresses: &[Address]) {
        assert_eq!(result.len(), SPOOL_COUNT);
        let valid: HashSet<Address> = expected_addresses.iter().copied().collect();
        for group in 0..SPOOL_GROUP_COUNT {
            let mut seen = HashSet::new();
            for offset in 0..GROUP_SIZE {
                let a = result[group * GROUP_SIZE + offset];
                assert!(valid.contains(&a), "spool address not in committee");
                assert!(seen.insert(a), "duplicate {a:?} in group {group}");
            }
        }
    }

    fn verify_counts(result: &[Address], addresses: &[Address], expected: &[SpoolCount]) {
        assert_eq!(tally(result, addresses), expected);
    }

    fn tally(result: &[Address], addresses: &[Address]) -> Vec<SpoolCount> {
        let lookup: BTreeMap<Address, usize> =
            addresses.iter().enumerate().map(|(i, &a)| (a, i)).collect();
        let mut c = vec![SpoolCount(0); addresses.len()];
        for a in result {
            let i = *lookup.get(a).expect("address not found");
            c[i] = c[i].checked_next().expect("spool count overflow");
        }
        c
    }

    fn count_changes(prev: &[Address], curr: &[Address]) -> usize {
        prev.iter().zip(curr).filter(|(a, b)| a != b).count()
    }

    fn uniform(n: usize, per: SpoolCount) -> Vec<SpoolCount> {
        vec![per; n]
    }

    fn fresh(addresses: &[Address], counts: &[SpoolCount]) -> Vec<Address> {
        initial_assignment(SPOOL_GROUP_COUNT, addresses, counts).unwrap()
    }

    fn mig(
        cur: &[Address],
        nm: &[Address],
        nc: &[SpoolCount],
    ) -> Vec<Address> {
        let cur_opt: Vec<Option<Address>> = cur.iter().copied().map(Some).collect();
        migrate_spools(SPOOL_GROUP_COUNT, &cur_opt, nm, nc, &Hash::default()).unwrap()
    }

    fn group_count(r: &[Address], a: Address) -> usize {
        (0..SPOOL_GROUP_COUNT)
            .filter(|&g| {
                let base = g * GROUP_SIZE;
                (0..GROUP_SIZE).any(|s| r[base + s] == a)
            })
            .count()
    }

    fn group_set(r: &[Address], g: usize) -> Vec<Address> {
        let base = g * GROUP_SIZE;
        let mut v: Vec<Address> = (0..GROUP_SIZE).map(|s| r[base + s]).collect();
        v.sort();
        v
    }

    fn round_robin(addresses: &[Address]) -> Vec<Address> {
        let n = addresses.len();
        (0..SPOOL_COUNT)
            .map(|i| {
                let g = i / GROUP_SIZE;
                let s = i % GROUP_SIZE;
                addresses[(g + s) % n]
            })
            .collect()
    }

    fn sequential_blocks(addresses: &[Address], block_size: usize) -> Vec<Address> {
        (0..SPOOL_COUNT).map(|i| addresses[i / block_size]).collect()
    }

    // ----- Basic tests -----

    #[test]
    fn fresh_twenty() {
        let addrs = make_addresses(20);
        let counts: Vec<SpoolCount> = vec![SpoolCount(50); 20];
        let r = fresh(&addrs, &counts);
        verify_group_constraints(&r, &addrs);
        verify_counts(&r, &addrs, &counts);
    }

    #[test]
    fn fresh_large() {
        let n = 128;
        let addrs = make_addresses(n);
        let stakes: Vec<TAPE> = (1..=n as u64).map(|i| TAPE(i * 1000)).collect();
        let counts = dhondt_counts(&stakes, SpoolCount(SPOOL_COUNT as u64));
        assert_eq!(counts.iter().map(|c| c.as_usize()).sum::<usize>(), SPOOL_COUNT);
        let r = fresh(&addrs, &counts);
        verify_group_constraints(&r, &addrs);
        verify_counts(&r, &addrs, &counts);
    }

    #[test]
    fn fresh_uneven() {
        let n = 50;
        let addrs = make_addresses(n);
        let stakes: Vec<TAPE> = (1..=n as u64).map(|i| TAPE(i * i * 100)).collect();
        let counts = dhondt_counts(&stakes, SpoolCount(SPOOL_COUNT as u64));
        let r = fresh(&addrs, &counts);
        verify_group_constraints(&r, &addrs);
        verify_counts(&r, &addrs, &counts);
    }

    // ----- Stability tests -----

    #[test]
    fn identity_stable() {
        let n = 30;
        let addrs = make_addresses(n);
        let stakes: Vec<TAPE> = (1..=n as u64).map(|i| TAPE(i * 500)).collect();
        let counts = dhondt_counts(&stakes, SpoolCount(SPOOL_COUNT as u64));

        let r1 = fresh(&addrs, &counts);
        verify_group_constraints(&r1, &addrs);

        let r2 = mig(&r1, &addrs, &counts);
        verify_group_constraints(&r2, &addrs);
        verify_counts(&r2, &addrs, &counts);

        assert_eq!(count_changes(&r1, &r2), 0, "expected zero changes");
    }

    #[test]
    fn removal_disruption() {
        let n = 40;
        let addrs1 = make_addresses(n);
        let stakes1: Vec<TAPE> = (1..=n as u64).map(|i| TAPE(i * 300)).collect();
        let counts1 = dhondt_counts(&stakes1, SpoolCount(SPOOL_COUNT as u64));
        let r1 = fresh(&addrs1, &counts1);

        let addrs2: Vec<Address> = addrs1[..30].to_vec();
        let stakes2: Vec<TAPE> = stakes1[..30].to_vec();
        let counts2 = dhondt_counts(&stakes2, SpoolCount(SPOOL_COUNT as u64));

        let r2 = mig(&r1, &addrs2, &counts2);
        verify_group_constraints(&r2, &addrs2);
        verify_counts(&r2, &addrs2, &counts2);

        assert!(count_changes(&r1, &r2) <= SPOOL_COUNT);
    }

    #[test]
    fn addition_disruption() {
        let n1 = 30;
        let addrs1 = make_addresses(n1);
        let stakes1: Vec<TAPE> = (1..=n1 as u64).map(|i| TAPE(i * 500)).collect();
        let counts1 = dhondt_counts(&stakes1, SpoolCount(SPOOL_COUNT as u64));
        let r1 = fresh(&addrs1, &counts1);

        let n2 = 50;
        let addrs2 = make_addresses(n2);
        let stakes2: Vec<TAPE> = (1..=n2 as u64).map(|i| TAPE(i * 500)).collect();
        let counts2 = dhondt_counts(&stakes2, SpoolCount(SPOOL_COUNT as u64));

        let r2 = mig(&r1, &addrs2, &counts2);
        verify_group_constraints(&r2, &addrs2);
        verify_counts(&r2, &addrs2, &counts2);
    }

    #[test]
    fn full_replace() {
        let n = 25;
        let addrs1 = make_addresses(n);
        let stakes: Vec<TAPE> = vec![TAPE(1000); n];
        let counts1 = dhondt_counts(&stakes, SpoolCount(SPOOL_COUNT as u64));
        let r1 = fresh(&addrs1, &counts1);

        let addrs2: Vec<Address> = (101..=125).map(addr).collect();
        let counts2 = dhondt_counts(&stakes, SpoolCount(SPOOL_COUNT as u64));

        let r2 = mig(&r1, &addrs2, &counts2);
        verify_group_constraints(&r2, &addrs2);
        verify_counts(&r2, &addrs2, &counts2);
    }

    // ----- Epoch chain test -----

    #[test]
    fn epoch_chain() {
        let n = 30;
        let addrs0 = make_addresses(n);
        let stakes: Vec<TAPE> = (1..=n as u64).map(|i| TAPE(i * 1000)).collect();
        let counts = dhondt_counts(&stakes, SpoolCount(SPOOL_COUNT as u64));
        let mut current = fresh(&addrs0, &counts);
        verify_group_constraints(&current, &addrs0);

        for epoch in 0..5u64 {
            let base = (epoch + 1) * 5;
            let new_addrs: Vec<Address> =
                (base + 6..=base + n as u64 + 5).map(addr).collect();
            let new_stakes: Vec<TAPE> = (1..=n as u64).map(|i| TAPE(i * 1000)).collect();
            let new_counts = dhondt_counts(&new_stakes, SpoolCount(SPOOL_COUNT as u64));

            current = mig(&current, &new_addrs, &new_counts);
            verify_group_constraints(&current, &new_addrs);
            verify_counts(&current, &new_addrs, &new_counts);
        }
    }

    // ----- Edge cases -----

    #[test]
    fn minimum_nodes() {
        let n = 20;
        let addrs = make_addresses(n);
        let counts: Vec<SpoolCount> = vec![SpoolCount(50); n];
        let r = fresh(&addrs, &counts);
        verify_group_constraints(&r, &addrs);
        verify_counts(&r, &addrs, &counts);
    }

    #[test]
    fn err_few_nodes() {
        let m = make_addresses(19);
        let c: Vec<SpoolCount> = vec![SpoolCount(52); 19];
        assert_eq!(initial_assignment(SPOOL_GROUP_COUNT, &m, &c).unwrap_err(), SpoolerError::InsufficientNodes);
    }

    #[test]
    fn err_total() {
        let m = make_addresses(20);
        let c: Vec<SpoolCount> = vec![SpoolCount(49); 20];
        assert_eq!(initial_assignment(SPOOL_GROUP_COUNT, &m, &c).unwrap_err(), SpoolerError::TotalMismatch);
    }

    #[test]
    fn err_cap() {
        let m = make_addresses(20);
        let mut c: Vec<SpoolCount> = vec![SpoolCount(50); 20];
        c[0] = SpoolCount(51);
        c[1] = SpoolCount(49);
        assert_eq!(initial_assignment(SPOOL_GROUP_COUNT, &m, &c).unwrap_err(), SpoolerError::SpoolCapExceeded);
    }

    #[test]
    fn err_count_mismatch() {
        let m = make_addresses(20);
        let c: Vec<SpoolCount> = vec![SpoolCount(50); 19];
        assert_eq!(initial_assignment(SPOOL_GROUP_COUNT, &m, &c).unwrap_err(), SpoolerError::CountMismatch);
    }

    // ----- Determinism tests -----

    #[test]
    fn deterministic_fresh() {
        let n = 50;
        let addrs = make_addresses(n);
        let stakes: Vec<TAPE> = (1..=n as u64).map(|i| TAPE(i * 777)).collect();
        let counts = dhondt_counts(&stakes, SpoolCount(SPOOL_COUNT as u64));

        let a = initial_assignment(SPOOL_GROUP_COUNT, &addrs, &counts).unwrap();
        let b = initial_assignment(SPOOL_GROUP_COUNT, &addrs, &counts).unwrap();
        assert_eq!(a, b);
    }

    #[test]
    fn deterministic_migrate() {
        let m1 = make_addresses(30);
        let c: Vec<SpoolCount> = [vec![SpoolCount(50); 10], vec![SpoolCount(25); 20]].concat();
        let r1 = fresh(&m1, &c);

        let mut m2 = m1.clone();
        for i in 25..30 {
            m2[i] = addr(300 + i as u64);
        }
        let a = mig(&r1, &m2, &c);
        let b = mig(&r1, &m2, &c);
        assert_eq!(a, b);
    }

    /// Cross-platform contract: identical input must produce a fixed output
    /// hash. If this digest changes, the off-chain spooler will diverge from
    /// every other operator and `vote_assignment` aggregate sigs will fail.
    /// Update PINNED_DIGEST ONLY in lockstep with a deliberate algorithm
    /// change, and re-run on every supported target.
    fn concat_addresses(slices: &[&[Address]]) -> Vec<u8> {
        let total: usize = slices.iter().map(|s| s.len() * 32).sum();
        let mut out = Vec::with_capacity(total);
        for slice in slices {
            for a in *slice {
                out.extend_from_slice(a.as_ref());
            }
        }
        out
    }

    #[test]
    fn cross_platform_digest() {
        let n = 50;
        let addrs = make_addresses(n);
        let stakes: Vec<TAPE> = (1..=n as u64).map(|i| TAPE(i * 1000)).collect();
        let counts = dhondt_counts(&stakes, SpoolCount(SPOOL_COUNT as u64));

        let mut seed_bytes = [0u8; 32];
        seed_bytes[..8].copy_from_slice(&0x1234_5678_9abc_def0u64.to_le_bytes());
        let seed = Hash::from(seed_bytes);

        // Exercise initial + a small grow chain so the digest covers
        // retention, eviction, and fill paths.
        let r0 = initial_assignment(SPOOL_GROUP_COUNT, &addrs, &counts).unwrap();

        let r1_input: Vec<Option<Address>> = r0.iter().copied().map(Some).collect();
        let r1 = migrate_spools(SPOOL_GROUP_COUNT, &r1_input, &addrs, &counts, &seed).unwrap();

        let grown_addrs: Vec<Address> = (1..=80u64).map(addr).collect();
        let grown_stakes: Vec<TAPE> = (1..=80u64).map(|i| TAPE(i * 500)).collect();
        let grown_counts = dhondt_counts(&grown_stakes, SpoolCount(SPOOL_COUNT as u64));
        let r2_input: Vec<Option<Address>> = r1.iter().copied().map(Some).collect();
        let r2 = migrate_spools(SPOOL_GROUP_COUNT, &r2_input, &grown_addrs, &grown_counts, &seed).unwrap();

        let bytes = concat_addresses(&[&r0, &r1, &r2]);
        let digest = hashv(&[&bytes]);

        // Pinned at the commit that landed this rewrite. Replace deliberately
        // if the algorithm changes; a CI failure on a new platform = nondeterminism.
        const PINNED_DIGEST: [u8; 32] = [
            212, 1, 131, 94, 115, 111, 87, 56, 246, 203, 187, 182, 68, 241, 80, 48,
            246, 210, 25, 211, 93, 201, 128, 137, 134, 247, 20, 182, 25, 148, 193, 40,
        ];

        assert_eq!(digest.to_bytes(), PINNED_DIGEST, "spooler digest drift");
    }

    // ----- Group constraint tests -----

    #[test]
    fn one_per_group() {
        let n = 50;
        let addrs = make_addresses(n);
        let counts: Vec<SpoolCount> = vec![SpoolCount(20); n];
        let r = fresh(&addrs, &counts);
        verify_group_constraints(&r, &addrs);
        verify_counts(&r, &addrs, &counts);

        for &a in addrs.iter() {
            let mut groups = HashSet::new();
            for spool_idx in 0..SPOOL_COUNT {
                if r[spool_idx] == a {
                    groups.insert(spool_idx / GROUP_SIZE);
                }
            }
            assert_eq!(groups.len(), 20);
        }
    }

    // ----- Group structure tests -----

    #[test]
    fn fresh_composition() {
        let m = make_addresses(20);
        let r = fresh(&m, &uniform(20, SpoolCount(50)));
        let mut expected = m.clone();
        expected.sort();
        for g in 0..SPOOL_GROUP_COUNT {
            assert_eq!(group_set(&r, g), expected, "group {g}");
        }
    }

    #[test]
    fn fresh_saturated() {
        for &n in &[20, 25, 50, 100] {
            let m = make_addresses(n);
            let stakes: Vec<TAPE> = (1..=n as u64).map(|i| TAPE(i * 100)).collect();
            let c = dhondt_counts(&stakes, SpoolCount(SPOOL_COUNT as u64));
            let r = fresh(&m, &c);
            for i in 0..n {
                assert_eq!(SpoolCount(group_count(&r, m[i]) as u64), c[i], "n={n} i={i}");
            }
        }
    }

    #[test]
    fn fresh_uniform() {
        let cases: &[(usize, SpoolCount)] = &[
            (20, SpoolCount(50)),
            (25, SpoolCount(40)),
            (50, SpoolCount(20)),
        ];
        for &(n, per) in cases {
            let m = make_addresses(n);
            let r = fresh(&m, &uniform(n, per));
            for &a in m.iter() {
                assert_eq!(group_count(&r, a), per.as_usize(), "n={n} per={per}");
            }
        }
    }

    #[test]
    fn fresh_tiers() {
        let m = make_addresses(25);
        let c: Vec<SpoolCount> = [vec![SpoolCount(50); 5], vec![SpoolCount(40); 10], vec![SpoolCount(35); 10]].concat();
        let r = fresh(&m, &c);
        verify_group_constraints(&r, &m);
        assert_eq!(tally(&r, &m), c);
        for i in 0..5 {
            assert_eq!(group_count(&r, m[i]), 50);
        }
        for i in 5..15 {
            assert_eq!(group_count(&r, m[i]), 40);
        }
        for i in 15..25 {
            assert_eq!(group_count(&r, m[i]), 35);
        }
    }

    #[test]
    fn fresh_minimal() {
        let m = make_addresses(21);
        let c: Vec<SpoolCount> = [vec![SpoolCount(50); 19], vec![SpoolCount(49)], vec![SpoolCount(1)]].concat();
        let r = fresh(&m, &c);
        verify_group_constraints(&r, &m);
        assert_eq!(group_count(&r, m[20]), 1);
        assert_eq!(group_count(&r, m[19]), 49);
        for i in 0..19 {
            assert_eq!(group_count(&r, m[i]), 50);
        }
    }

    #[test]
    fn fresh_zeroes() {
        let m = make_addresses(21);
        let c: Vec<SpoolCount> = [vec![SpoolCount(50); 20], vec![SpoolCount(0)]].concat();
        let r = fresh(&m, &c);
        assert_eq!(group_count(&r, m[20]), 0);
        assert_eq!(tally(&r, &m)[20], SpoolCount(0));
    }

    #[test]
    fn group_diversity() {
        for &n in &[20, 25, 30, 50, 80, 128] {
            let m = make_addresses(n);
            let stakes: Vec<TAPE> = (1..=n as u64).map(|i| TAPE(i * 100)).collect();
            let c = dhondt_counts(&stakes, SpoolCount(SPOOL_COUNT as u64));
            let r = fresh(&m, &c);
            for g in 0..SPOOL_GROUP_COUNT {
                let gs = group_set(&r, g);
                let mut deduped = gs.clone();
                deduped.dedup();
                assert_eq!(deduped.len(), GROUP_SIZE, "dup in n={n} g={g}");
            }
        }
    }

    // ----- Count accuracy -----

    #[test]
    fn tally_matches() {
        let m = make_addresses(25);
        let c: Vec<SpoolCount> = [vec![SpoolCount(50); 5], vec![SpoolCount(40); 10], vec![SpoolCount(35); 10]].concat();
        assert_eq!(tally(&fresh(&m, &c), &m), c);

        let m = make_addresses(30);
        let c: Vec<SpoolCount> = [vec![SpoolCount(50); 10], vec![SpoolCount(25); 20]].concat();
        assert_eq!(tally(&fresh(&m, &c), &m), c);
    }

    #[test]
    fn tally_survives() {
        let m = make_addresses(30);
        let s1: Vec<TAPE> = (1..=30u64).map(|i| TAPE(i * 500)).collect();
        let c1 = dhondt_counts(&s1, SpoolCount(SPOOL_COUNT as u64));
        let r1 = fresh(&m, &c1);

        let s2: Vec<TAPE> = (1..=30u64).map(|i| TAPE((31 - i) * 500)).collect();
        let c2 = dhondt_counts(&s2, SpoolCount(SPOOL_COUNT as u64));
        let r2 = mig(&r1, &m, &c2);
        assert_eq!(tally(&r2, &m), c2);
    }

    // ----- Retention & stability -----

    #[test]
    fn identity_sweep() {
        for &n in &[20, 30, 50, 100] {
            let m = make_addresses(n);
            let stakes: Vec<TAPE> = (1..=n as u64).map(|i| TAPE(i * 100)).collect();
            let c = dhondt_counts(&stakes, SpoolCount(SPOOL_COUNT as u64));
            let r1 = fresh(&m, &c);
            let r2 = mig(&r1, &m, &c);
            assert_eq!(count_changes(&r1, &r2), 0, "n={n}");
        }
    }

    #[test]
    fn single_swap() {
        let n = 25;
        let m1 = make_addresses(n);
        let c = uniform(n, SpoolCount(40));
        let r1 = fresh(&m1, &c);

        let mut m2 = m1.clone();
        m2[n - 1] = addr(999);
        let r2 = mig(&r1, &m2, &c);
        verify_group_constraints(&r2, &m2);
        assert_eq!(count_changes(&r1, &r2), 40);
    }

    #[test]
    fn double_swap() {
        let n = 25;
        let m1 = make_addresses(n);
        let c = uniform(n, SpoolCount(40));
        let r1 = fresh(&m1, &c);

        let mut m2 = m1.clone();
        m2[0] = addr(900);
        m2[n - 1] = addr(901);
        let r2 = mig(&r1, &m2, &c);
        verify_group_constraints(&r2, &m2);

        let m = count_changes(&r1, &r2);
        assert!((80..=85).contains(&m), "moves={m}");
    }

    #[test]
    fn survivor_stability() {
        let n = 30;
        let m1 = make_addresses(n);
        let c: Vec<SpoolCount> = [vec![SpoolCount(50); 10], vec![SpoolCount(25); 20]].concat();
        let r1 = fresh(&m1, &c);

        let mut m2 = m1.clone();
        for i in 25..30 {
            m2[i] = addr(200 + i as u64);
        }
        let r2 = mig(&r1, &m2, &c);

        let survivors: HashSet<Address> = m1[..25].iter().copied().collect();
        for i in 0..SPOOL_COUNT {
            if survivors.contains(&r1[i]) {
                assert_eq!(r1[i], r2[i], "spool {i} should be retained");
            }
        }
    }

    #[test]
    fn stake_stability() {
        let n = 40;
        let m = make_addresses(n);

        let stakes1: Vec<TAPE> = (1..=n as u64).map(|i| TAPE(i * 1000)).collect();
        let counts1 = dhondt_counts(&stakes1, SpoolCount(SPOOL_COUNT as u64));
        let r1 = fresh(&m, &counts1);

        let mut stakes2 = stakes1.clone();
        stakes2[n - 1] = stakes2[n - 1] - TAPE(500);
        stakes2[0] = stakes2[0] + TAPE(500);
        let counts2 = dhondt_counts(&stakes2, SpoolCount(SPOOL_COUNT as u64));

        let r2 = mig(&r1, &m, &counts2);
        verify_group_constraints(&r2, &m);
        verify_counts(&r2, &m, &counts2);

        let changes = count_changes(&r1, &r2);
        let count_diff: usize = counts1
            .iter()
            .zip(counts2.iter())
            .map(|(&a, &b)| (a.as_u64() as i64 - b.as_u64() as i64).unsigned_abs() as usize)
            .sum();
        assert!(
            changes <= count_diff + 10,
            "too many changes: {changes} (count diff: {count_diff})",
        );
    }

    // ----- Addition & removal -----

    #[test]
    fn add_nodes() {
        let m1 = make_addresses(25);
        let c1 = uniform(25, SpoolCount(40));
        let r1 = fresh(&m1, &c1);

        let m2 = make_addresses(30);
        let s2: Vec<TAPE> = vec![TAPE(1000); 30];
        let c2 = dhondt_counts(&s2, SpoolCount(SPOOL_COUNT as u64));
        let r2 = mig(&r1, &m2, &c2);
        verify_group_constraints(&r2, &m2);
        assert_eq!(tally(&r2, &m2), c2);

        let new_spools: usize = c2[25..].iter().map(|c| c.as_usize()).sum();
        let m = count_changes(&r1, &r2);
        assert!(m >= new_spools, "moves {m} < new spools {new_spools}");
    }

    #[test]
    fn remove_half() {
        let m1 = make_addresses(40);
        let s1: Vec<TAPE> = vec![TAPE(1000); 40];
        let c1 = dhondt_counts(&s1, SpoolCount(SPOOL_COUNT as u64));
        let r1 = fresh(&m1, &c1);

        let m2: Vec<Address> = m1[..20].to_vec();
        let c2 = uniform(20, SpoolCount(50));
        let r2 = mig(&r1, &m2, &c2);
        verify_group_constraints(&r2, &m2);
        assert_eq!(tally(&r2, &m2), c2);
    }

    #[test]
    fn complete_turnover() {
        let m1 = make_addresses(25);
        let m2: Vec<Address> = (100..125).map(addr).collect();
        let c = uniform(25, SpoolCount(40));
        let r1 = fresh(&m1, &c);
        let r2 = mig(&r1, &m2, &c);
        verify_group_constraints(&r2, &m2);
        assert_eq!(count_changes(&r1, &r2), SPOOL_COUNT);
    }

    // ----- Rebalance -----

    #[test]
    fn rebalance_reversed() {
        let n = 30;
        let m = make_addresses(n);
        let s1: Vec<TAPE> = (1..=n as u64).map(|i| TAPE(i * 1000)).collect();
        let c1 = dhondt_counts(&s1, SpoolCount(SPOOL_COUNT as u64));
        let r1 = fresh(&m, &c1);

        let s2: Vec<TAPE> = (1..=n as u64).rev().map(|i| TAPE(i * 1000)).collect();
        let c2 = dhondt_counts(&s2, SpoolCount(SPOOL_COUNT as u64));
        let r2 = mig(&r1, &m, &c2);
        verify_group_constraints(&r2, &m);
        assert_eq!(tally(&r2, &m), c2);

        let delta: usize = c1
            .iter()
            .zip(&c2)
            .map(|(&a, &b)| (a.as_u64() as i64 - b.as_u64() as i64).unsigned_abs() as usize)
            .sum();
        assert!(count_changes(&r1, &r2) <= delta, "moves exceed delta");
    }

    // ----- Address patterns -----

    #[test]
    fn nonsequential_fresh() {
        let m: Vec<Address> = [
            10u64, 99, 3, 500, 7, 42, 1000, 8, 2, 55,
            11, 88, 4, 501, 6, 43, 1001, 9, 1, 56,
        ]
        .iter()
        .copied()
        .map(addr)
        .collect();
        let r = fresh(&m, &uniform(20, SpoolCount(50)));
        verify_group_constraints(&r, &m);
        assert_eq!(tally(&r, &m), uniform(20, SpoolCount(50)));
    }

    #[test]
    fn nonsequential_migrate() {
        let m1: Vec<Address> = [
            10u64, 99, 3, 500, 7, 42, 1000, 8, 2, 55,
            11, 88, 4, 501, 6, 43, 1001, 9, 1, 56,
        ]
        .iter()
        .copied()
        .map(addr)
        .collect();
        let r1 = fresh(&m1, &uniform(20, SpoolCount(50)));

        let mut m2 = m1.clone();
        m2[0] = addr(777);
        m2[19] = addr(888);
        let r2 = mig(&r1, &m2, &uniform(20, SpoolCount(50)));
        verify_group_constraints(&r2, &m2);
        assert_eq!(count_changes(&r1, &r2), 100);
    }

    // ----- Epoch chains -----

    #[test]
    fn grow_shrink() {
        let m20 = make_addresses(20);
        let r1 = fresh(&m20, &uniform(20, SpoolCount(50)));

        let m50 = make_addresses(50);
        let r2 = mig(&r1, &m50, &uniform(50, SpoolCount(20)));
        verify_group_constraints(&r2, &m50);

        let r3 = mig(&r2, &m20, &uniform(20, SpoolCount(50)));
        verify_group_constraints(&r3, &m20);
        assert_eq!(tally(&r3, &m20), uniform(20, SpoolCount(50)));
    }

    #[test]
    fn rolling_replace() {
        let n = 25;
        let mut m = make_addresses(n);
        let c = uniform(n, SpoolCount(40));
        let mut r = fresh(&m, &c);

        for epoch in 1u64..=5 {
            let mut m_next = m.clone();
            for i in 0..5 {
                m_next[i] = addr(epoch * 100 + i as u64);
            }
            let r_next = mig(&r, &m_next, &c);
            verify_group_constraints(&r_next, &m_next);
            assert_eq!(tally(&r_next, &m_next), c);
            let mv = count_changes(&r, &r_next);
            assert!((200..=210).contains(&mv), "epoch {epoch} moves={mv}");

            m = m_next;
            r = r_next;
        }
    }

    // ----- Large-scale test -----

    #[test]
    fn epoch_large() {
        let n = 128;
        let addrs0 = make_addresses(n);
        let stakes: Vec<TAPE> = (1..=n as u64).map(|i| TAPE(i * 100)).collect();
        let counts = dhondt_counts(&stakes, SpoolCount(SPOOL_COUNT as u64));
        let mut current = initial_assignment(SPOOL_GROUP_COUNT, &addrs0, &counts).unwrap();
        verify_group_constraints(&current, &addrs0);
        verify_counts(&current, &addrs0, &counts);

        for epoch in 0..3u64 {
            let offset = (epoch + 1) * 10;
            let new_addrs: Vec<Address> =
                (offset + 1..=offset + n as u64).map(addr).collect();
            let new_stakes: Vec<TAPE> = (1..=n as u64).map(|i| TAPE(i * 100)).collect();
            let new_counts = dhondt_counts(&new_stakes, SpoolCount(SPOOL_COUNT as u64));

            current = mig(&current, &new_addrs, &new_counts);
            verify_group_constraints(&current, &new_addrs);
            verify_counts(&current, &new_addrs, &new_counts);
        }
    }

    // ----- Must-take / eviction integration tests -----

    #[test]
    fn forced_take() {
        let n = 20;
        let addrs = make_addresses(n);
        let counts: Vec<SpoolCount> = vec![SpoolCount(50); n];
        let r = initial_assignment(SPOOL_GROUP_COUNT, &addrs, &counts).unwrap();
        verify_group_constraints(&r, &addrs);
        verify_counts(&r, &addrs, &counts);

        for &a in addrs.iter() {
            for group in 0..SPOOL_GROUP_COUNT {
                let found = (0..GROUP_SIZE)
                    .any(|offset| r[group * GROUP_SIZE + offset] == a);
                assert!(found, "address missing from group {group}");
            }
        }
    }

    #[test]
    fn eviction() {
        let n = 25;
        let m = make_addresses(n);

        let mut stakes1: Vec<TAPE> = vec![TAPE(100); n];
        for i in 0..5 {
            stakes1[i] = TAPE(10_000);
        }
        let counts1 = dhondt_counts(&stakes1, SpoolCount(SPOOL_COUNT as u64));
        let r1 = initial_assignment(SPOOL_GROUP_COUNT, &m, &counts1).unwrap();
        verify_group_constraints(&r1, &m);

        let mut stakes2: Vec<TAPE> = vec![TAPE(100); n];
        for i in (n - 5)..n {
            stakes2[i] = TAPE(10_000);
        }
        let counts2 = dhondt_counts(&stakes2, SpoolCount(SPOOL_COUNT as u64));

        let r2 = mig(&r1, &m, &counts2);
        verify_group_constraints(&r2, &m);
        verify_counts(&r2, &m, &counts2);
    }

    // ----- Manual layouts -----

    #[test]
    fn manual_identity_20() {
        let m = make_addresses(20);
        let spools = round_robin(&m);
        let r = mig(&spools, &m, &uniform(20, SpoolCount(50)));
        assert_eq!(r, spools);
    }

    #[test]
    fn manual_identity_25() {
        let m = make_addresses(25);
        let spools = round_robin(&m);
        let r = mig(&spools, &m, &uniform(25, SpoolCount(40)));
        assert_eq!(r, spools);
    }

    #[test]
    fn manual_identity_50() {
        let m = make_addresses(50);
        let spools = round_robin(&m);
        let r = mig(&spools, &m, &uniform(50, SpoolCount(20)));
        assert_eq!(r, spools);
    }

    #[test]
    fn manual_interleaved() {
        let m = make_addresses(20);
        let spools: Vec<Address> = (0..SPOOL_COUNT)
            .map(|i| m[i % GROUP_SIZE])
            .collect();
        let r = mig(&spools, &m, &uniform(20, SpoolCount(50)));
        assert_eq!(r, spools);
    }

    #[test]
    fn manual_swap_one() {
        let m1 = make_addresses(20);
        let spools = round_robin(&m1);
        let mut m2 = m1.clone();
        m2[19] = addr(999);
        let r = mig(&spools, &m2, &uniform(20, SpoolCount(50)));

        for i in 0..SPOOL_COUNT {
            if spools[i] != m1[19] {
                assert_eq!(r[i], spools[i], "spool {i} retained");
            }
        }
    }

    #[test]
    fn manual_swap_survivors() {
        let m1 = make_addresses(25);
        let spools = round_robin(&m1);
        let mut m2 = m1.clone();
        m2[24] = addr(999);
        let r = mig(&spools, &m2, &uniform(25, SpoolCount(40)));
        verify_group_constraints(&r, &m2);

        for i in 0..SPOOL_COUNT {
            if spools[i] != m1[24] {
                assert_eq!(r[i], spools[i], "spool {i} retained");
            }
        }
        assert_eq!(tally(&r, &m2)[24], SpoolCount(40));
    }

    #[test]
    fn manual_swap_two() {
        let m1 = make_addresses(20);
        let spools = round_robin(&m1);
        let mut m2 = m1.clone();
        m2[0] = addr(900);
        m2[19] = addr(901);
        let r = mig(&spools, &m2, &uniform(20, SpoolCount(50)));
        verify_group_constraints(&r, &m2);

        let middle: HashSet<Address> = m1[1..19].iter().copied().collect();
        for i in 0..SPOOL_COUNT {
            if middle.contains(&spools[i]) {
                assert_eq!(r[i], spools[i], "spool {i} retained");
            }
        }
        assert_eq!(tally(&r, &m2), uniform(20, SpoolCount(50)));
    }

    #[test]
    fn manual_single_owner() {
        let prev_addr = addr(1);
        let prev: Vec<Option<Address>> = vec![Some(prev_addr); SPOOL_COUNT];
        let mut next: Vec<Address> = vec![prev_addr];
        next.extend((2..=20).map(addr));
        let r = migrate_spools(SPOOL_GROUP_COUNT, &prev, &next, &uniform(20, SpoolCount(50)), &Hash::default()).unwrap();
        verify_group_constraints(&r, &next);

        for g in 0..SPOOL_GROUP_COUNT {
            assert_eq!(
                r[g * GROUP_SIZE], prev_addr,
                "group {g} slot 0 retained by sole previous owner",
            );
        }
        assert_eq!(tally(&r, &next)[0], SpoolCount(50));
    }

    #[test]
    fn manual_block_retention() {
        let m = make_addresses(20);
        let prev = sequential_blocks(&m, 50);
        let r = mig(&prev, &m, &uniform(20, SpoolCount(50)));
        verify_group_constraints(&r, &m);
        assert_eq!(tally(&r, &m), uniform(20, SpoolCount(50)));

        for k in 0..20 {
            assert_eq!(r[k * 50], m[k], "node {k}'s first spool retained");
        }
    }

    #[test]
    fn manual_block_pairs() {
        let m = make_addresses(20);
        let prev: Vec<Address> = (0..SPOOL_COUNT)
            .map(|i| {
                let block = i / 100;
                let half = (i % 100) / 50;
                m[block * 2 + half]
            })
            .collect();
        let r = mig(&prev, &m, &uniform(20, SpoolCount(50)));
        verify_group_constraints(&r, &m);
        assert_eq!(tally(&r, &m), uniform(20, SpoolCount(50)));

        for k in 0..20 {
            let first_spool = if k % 2 == 0 { (k / 2) * 100 } else { (k / 2) * 100 + 50 };
            assert_eq!(
                r[first_spool], m[k],
                "node {k}'s first spool at {first_spool} retained",
            );
        }
    }

    #[test]
    fn manual_grow() {
        let m1 = make_addresses(20);
        let spools = round_robin(&m1);
        let m2 = make_addresses(25);
        let c2 = uniform(25, SpoolCount(40));
        let r = mig(&spools, &m2, &c2);
        verify_group_constraints(&r, &m2);
        assert_eq!(tally(&r, &m2), c2);

        let originals: HashSet<Address> = m1.iter().copied().collect();
        let retained: usize = (0..SPOOL_COUNT)
            .filter(|&i| originals.contains(&r[i]) && r[i] == spools[i])
            .count();
        assert!(
            (725..=800).contains(&retained),
            "expected high retention during committee growth, got {retained}",
        );
    }

    #[test]
    fn manual_shrink() {
        let m1 = make_addresses(20);
        let spools = round_robin(&m1);
        let m2: Vec<Address> = (101..=120).map(addr).collect();
        let r = mig(&spools, &m2, &uniform(20, SpoolCount(50)));
        verify_group_constraints(&r, &m2);
        assert_eq!(count_changes(&spools, &r), SPOOL_COUNT);
    }

    #[test]
    fn manual_group_slices() {
        let m = make_addresses(20);
        let spools = round_robin(&m);
        for g in 0..SPOOL_GROUP_COUNT {
            let base = g * GROUP_SIZE;
            let expected: Vec<Address> = (0..GROUP_SIZE)
                .map(|s| m[(g + s) % 20])
                .collect();
            let actual: Vec<Address> = (0..GROUP_SIZE)
                .map(|s| spools[base + s])
                .collect();
            assert_eq!(actual, expected, "group {g} raw content");
        }

        let r = mig(&spools, &m, &uniform(20, SpoolCount(50)));
        for g in 0..SPOOL_GROUP_COUNT {
            let base = g * GROUP_SIZE;
            for s in 0..GROUP_SIZE {
                assert_eq!(
                    r[base + s], spools[base + s],
                    "group {g} slot {s} mismatch",
                );
            }
        }
    }

    #[test]
    fn manual_partial_overlap() {
        let m1 = make_addresses(25);
        let spools = round_robin(&m1);
        let mut m2 = m1.clone();
        for i in 20..25 {
            m2[i] = addr(100 + i as u64);
        }
        let r = mig(&spools, &m2, &uniform(25, SpoolCount(40)));
        verify_group_constraints(&r, &m2);
        assert_eq!(tally(&r, &m2), uniform(25, SpoolCount(40)));

        let m = count_changes(&spools, &r);
        assert!(m >= 200, "too few moves: {m}");
        assert!(m <= 250, "too many moves: {m}");
    }

    #[test]
    fn manual_half_replaced() {
        let m1 = make_addresses(25);
        let spools = round_robin(&m1);
        let mut m2 = m1.clone();
        for i in 12..25 {
            m2[i] = addr(100 + i as u64);
        }
        let r = mig(&spools, &m2, &uniform(25, SpoolCount(40)));
        verify_group_constraints(&r, &m2);
        assert_eq!(tally(&r, &m2), uniform(25, SpoolCount(40)));

        let m = count_changes(&spools, &r);
        assert!(m >= 520, "too few moves: {m}");
        assert!(m <= 600, "too many moves: {m}");
    }

    // ----- Stress / edge-case tests -----

    #[test]
    fn stress_must_take() {
        let n = 20;
        let addrs = make_addresses(n);
        let counts = uniform(n, SpoolCount(50));
        let r = initial_assignment(SPOOL_GROUP_COUNT, &addrs, &counts).unwrap();
        verify_group_constraints(&r, &addrs);
        verify_counts(&r, &addrs, &counts);

        for &a in addrs.iter() {
            for g in 0..SPOOL_GROUP_COUNT {
                let base = g * GROUP_SIZE;
                let found = (0..GROUP_SIZE).any(|s| r[base + s] == a);
                assert!(found, "address missing from group {g}");
            }
        }
    }

    #[test]
    fn stress_grow() {
        let m1 = make_addresses(20);
        let c1 = uniform(20, SpoolCount(50));
        let r1 = fresh(&m1, &c1);

        let n2 = 128;
        let m2 = make_addresses(n2);
        let stakes2: Vec<TAPE> = (1..=n2 as u64).map(|i| TAPE(i * 100)).collect();
        let c2 = dhondt_counts(&stakes2, SpoolCount(SPOOL_COUNT as u64));

        let r2 = mig(&r1, &m2, &c2);
        verify_group_constraints(&r2, &m2);
        verify_counts(&r2, &m2, &c2);

        for g in 0..SPOOL_GROUP_COUNT {
            let mut seen = HashSet::new();
            let base = g * GROUP_SIZE;
            for s in 0..GROUP_SIZE {
                seen.insert(r2[base + s]);
            }
            assert_eq!(seen.len(), GROUP_SIZE, "group {g} has duplicates");
        }

        for i in 0..n2 {
            assert_eq!(SpoolCount(group_count(&r2, m2[i]) as u64), c2[i]);
        }

        assert_eq!(c2.iter().map(|c| c.as_usize()).sum::<usize>(), SPOOL_COUNT);
    }

    #[test]
    fn stress_full_churn() {
        let n = 128;
        let m1 = make_addresses(n);
        let stakes: Vec<TAPE> = (1..=n as u64).map(|i| TAPE(i * 100)).collect();
        let c = dhondt_counts(&stakes, SpoolCount(SPOOL_COUNT as u64));
        let r1 = fresh(&m1, &c);

        let m2: Vec<Address> = (1001..=1128).map(addr).collect();
        let r2 = mig(&r1, &m2, &c);
        verify_group_constraints(&r2, &m2);
        verify_counts(&r2, &m2, &c);
        assert_eq!(count_changes(&r1, &r2), SPOOL_COUNT);
    }

    #[test]
    fn stress_rebalance() {
        let n = 128;
        let m = make_addresses(n);
        let s1: Vec<TAPE> = (1..=n as u64).map(|i| TAPE(i * 1000)).collect();
        let c1 = dhondt_counts(&s1, SpoolCount(SPOOL_COUNT as u64));
        let r1 = fresh(&m, &c1);

        let s2: Vec<TAPE> = (1..=n as u64).rev().map(|i| TAPE(i * 1000)).collect();
        let c2 = dhondt_counts(&s2, SpoolCount(SPOOL_COUNT as u64));
        let r2 = mig(&r1, &m, &c2);
        verify_group_constraints(&r2, &m);
        verify_counts(&r2, &m, &c2);

        for g in 0..SPOOL_GROUP_COUNT {
            let mut seen = HashSet::new();
            let base = g * GROUP_SIZE;
            for s in 0..GROUP_SIZE {
                seen.insert(r2[base + s]);
            }
            assert_eq!(seen.len(), GROUP_SIZE, "group {g} has duplicates");
        }

        for i in 0..n {
            assert_eq!(SpoolCount(group_count(&r2, m[i]) as u64), c2[i]);
        }
    }

    #[test]
    fn stress_epoch_bucket() {
        let n = 50;
        let mut addrs = make_addresses(n);
        let stakes: Vec<TAPE> = (1..=n as u64).map(|i| TAPE(i * 500)).collect();
        let counts = dhondt_counts(&stakes, SpoolCount(SPOOL_COUNT as u64));
        let mut current = initial_assignment(SPOOL_GROUP_COUNT, &addrs, &counts).unwrap();
        verify_group_constraints(&current, &addrs);
        verify_counts(&current, &addrs, &counts);

        let churn_amounts = [5, 10, 15, 20, 25];
        for (epoch, &churn) in churn_amounts.iter().enumerate() {
            let mut new_addrs = addrs.clone();
            for i in 0..churn {
                new_addrs[i] = addr((epoch as u64 + 1) * 1000 + i as u64);
            }
            let new_stakes: Vec<TAPE> = (1..=n as u64).map(|i| TAPE(i * 500)).collect();
            let new_counts = dhondt_counts(&new_stakes, SpoolCount(SPOOL_COUNT as u64));

            current = mig(&current, &new_addrs, &new_counts);
            verify_group_constraints(&current, &new_addrs);
            verify_counts(&current, &new_addrs, &new_counts);

            for g in 0..SPOOL_GROUP_COUNT {
                let mut seen = HashSet::new();
                let base = g * GROUP_SIZE;
                for s in 0..GROUP_SIZE {
                    seen.insert(current[base + s]);
                }
                assert_eq!(
                    seen.len(), GROUP_SIZE,
                    "epoch {} group {g} has duplicates", epoch + 1,
                );
            }

            addrs = new_addrs;
        }
    }
}
