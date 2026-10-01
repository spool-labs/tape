//! Assigns every spool to a committee node by bytes, keeping holders in place.
//!
//! Each epoch runs four steps over the previous layout:
//!
//! 1. Keep: every spool stays with its holder while the holder is in the committee.
//! 2. Release: a node without room for what it holds gives up spools until it fits.
//! 3. Fill: open slots go to the node with the strongest claim, heaviest group first.
//! 4. Rebalance: a spool moves when the move cuts the byte objective by at least
//!    `MIN_MOVE_GAIN` per byte, until the epoch has moved `MOVE_BUDGET` of stored bytes.
//!
//! A node never holds two spools of one group, and never takes a spool it lacks
//! room for. Integer math and address-keyed tie-breaks make the result the same
//! on every node, whatever the committee order.

use std::collections::BTreeMap;

use tape_core::erasure::MAX_GROUP_SIZE;
use tape_core::spooler::{GroupIndex, SpoolerError};
use tape_core::types::{BasisPoints, Coin, StorageUnits, TAPE};
use tape_crypto::address::Address;
use tape_crypto::hash::{Hash, hashv};

use crate::score::Score;
use crate::target::byte_targets;
use crate::{GROWTH_MARGIN, MIN_MOVE_GAIN, MOVE_BUDGET};

/// A node of the next committee, as the assignment sees it
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Candidate {
    pub node: Address,
    pub stake: Coin<TAPE>,
    /// The most bytes the node holds; zero means no limit
    pub capacity: StorageUnits,
}

/// Assign every spool of `sizes.len()` groups to the candidates.
///
/// `width` is the network's spools per group. `sizes` holds each group's bytes
/// per spool, `current` the previous holder of every spool (`width` per group,
/// in group order, with no gaps between groups). Returns one address per spool
/// in the same order.
pub fn assign_spools(
    sizes: &[StorageUnits],
    current: &[Option<Address>],
    candidates: &[Candidate],
    nonce: &Hash,
    width: usize,
) -> Result<Vec<Address>, SpoolerError> {
    validate(sizes, current, candidates, width)?;

    let mut layout = Layout::new(sizes, candidates, *nonce, width)?;
    layout.keep(current);
    layout.release();
    layout.fill()?;
    layout.rebalance();
    layout.into_assignment()
}

fn validate(
    sizes: &[StorageUnits],
    current: &[Option<Address>],
    candidates: &[Candidate],
    width: usize,
) -> Result<(), SpoolerError> {
    if width == 0 || width > MAX_GROUP_SIZE {
        return Err(SpoolerError::LengthMismatch);
    }
    if current.len() != sizes.len() * width {
        return Err(SpoolerError::LengthMismatch);
    }
    // A node holds at most one spool per group, so a group needs `width` nodes
    if candidates.len() < width {
        return Err(SpoolerError::InsufficientNodes);
    }

    let mut nodes: Vec<Address> = candidates.iter().map(|candidate| candidate.node).collect();
    nodes.sort_unstable();
    for pair in nodes.windows(2) {
        if pair[0] == pair[1] {
            return Err(SpoolerError::DuplicateNode);
        }
    }
    Ok(())
}

struct NodeState {
    address: Address,
    capacity: StorageUnits,
    target: StorageUnits,
    load: StorageUnits,
}

impl NodeState {
    /// Whether the node has room for one more spool of `weight` bytes
    fn has_room(&self, weight: StorageUnits) -> bool {
        self.capacity.is_zero()
            || (self.load.as_u128() + weight.as_u128()) * GROWTH_MARGIN as u128
                <= self.capacity.as_u128()
    }

    /// Whether the node fits once it gives up a spool of `weight` bytes
    fn fits_without(&self, weight: StorageUnits) -> bool {
        self.capacity.is_zero()
            || (self.load.as_u128() - weight.as_u128()) * GROWTH_MARGIN as u128
                <= self.capacity.as_u128()
    }
}

struct Layout {
    nonce: Hash,
    width: usize,
    sizes: Vec<StorageUnits>,
    weights: Vec<StorageUnits>,
    stored: StorageUnits,
    nodes: Vec<NodeState>,
    holders: Vec<Option<usize>>,
    is_holding: Vec<bool>,
}

impl Layout {
    fn new(
        sizes: &[StorageUnits],
        candidates: &[Candidate],
        nonce: Hash,
        width: usize,
    ) -> Result<Self, SpoolerError> {
        // An empty group still weighs a byte per spool, so its spools spread by stake
        let mut weights = Vec::with_capacity(sizes.len());
        let mut per_spool_total = StorageUnits::zero();
        for size in sizes {
            let weight = size
                .checked_add(StorageUnits(1))
                .ok_or(SpoolerError::Overflow)?;
            per_spool_total = per_spool_total
                .checked_add(weight)
                .ok_or(SpoolerError::Overflow)?;
            weights.push(weight);
        }
        let stored = per_spool_total
            .checked_mul(StorageUnits(width as u64))
            .ok_or(SpoolerError::Overflow)?;

        let stakes: Vec<Coin<TAPE>> = candidates.iter().map(|candidate| candidate.stake).collect();
        let bounds: Vec<StorageUnits> = candidates
            .iter()
            .map(|candidate| node_bound(candidate.capacity, per_spool_total))
            .collect();
        let targets = byte_targets(&stakes, &bounds, stored);

        let nodes = candidates
            .iter()
            .zip(targets)
            .map(|(candidate, target)| NodeState {
                address: candidate.node,
                capacity: candidate.capacity,
                target,
                load: StorageUnits::zero(),
            })
            .collect();

        Ok(Self {
            nonce,
            width,
            sizes: sizes.to_vec(),
            weights,
            stored,
            nodes,
            holders: vec![None; sizes.len() * width],
            is_holding: vec![false; sizes.len() * candidates.len()],
        })
    }

    fn group_count(&self) -> usize {
        self.sizes.len()
    }

    fn holds(&self, group: usize, node: usize) -> bool {
        self.is_holding[group * self.nodes.len() + node]
    }

    fn place(&mut self, group: usize, offset: usize, node: usize) {
        self.holders[group * self.width + offset] = Some(node);
        self.is_holding[group * self.nodes.len() + node] = true;
        self.nodes[node].load = StorageUnits(self.nodes[node].load.0 + self.weights[group].0);
    }

    fn vacate(&mut self, group: usize, offset: usize) {
        let Some(node) = self.holders[group * self.width + offset].take() else {
            return;
        };
        self.is_holding[group * self.nodes.len() + node] = false;
        self.nodes[node].load = StorageUnits(self.nodes[node].load.0 - self.weights[group].0);
    }

    /// Hash-based order for equal scores, so ties don't depend on committee order
    fn tie_break(&self, group: usize, node: usize) -> Hash {
        let group = GroupIndex(group as u64);
        hashv(&[self.nonce.as_ref(), &group.pack(), self.nodes[node].address.as_ref()])
    }

    /// Keep every previous holder that is still in the committee, at its slot
    fn keep(&mut self, current: &[Option<Address>]) {
        let index: BTreeMap<Address, usize> = self
            .nodes
            .iter()
            .enumerate()
            .map(|(node, state)| (state.address, node))
            .collect();

        for (spool, holder) in current.iter().enumerate() {
            let Some(node) = holder.and_then(|address| index.get(&address).copied()) else {
                continue;
            };
            let group = spool / self.width;
            if !self.holds(group, node) {
                self.place(group, spool % self.width, node);
            }
        }
    }

    /// Shed spools from every node that no longer has room for its load
    fn release(&mut self) {
        for node in 0..self.nodes.len() {
            while !self.nodes[node].fits_without(StorageUnits::zero()) {
                let Some((group, offset)) = self.spool_to_release(node) else {
                    break;
                };
                self.vacate(group, offset);
            }
        }
    }

    /// The smallest held spool that makes the node fit, or the largest when none does
    fn spool_to_release(&self, node: usize) -> Option<(usize, usize)> {
        let mut smallest_clearing: Option<usize> = None;
        let mut largest: Option<usize> = None;

        for group in 0..self.group_count() {
            if !self.holds(group, node) {
                continue;
            }
            let weight = self.weights[group];
            let clears = self.nodes[node].fits_without(weight);
            if clears && smallest_clearing.is_none_or(|best| weight < self.weights[best]) {
                smallest_clearing = Some(group);
            }
            if largest.is_none_or(|best| weight > self.weights[best]) {
                largest = Some(group);
            }
        }

        let group = smallest_clearing.or(largest)?;
        let offset = self.offset_of(group, node)?;
        Some((group, offset))
    }

    fn offset_of(&self, group: usize, node: usize) -> Option<usize> {
        let start = group * self.width;
        self.holders[start..start + self.width]
            .iter()
            .position(|holder| *holder == Some(node))
    }

    /// Fill every open slot, heaviest group first
    fn fill(&mut self) -> Result<(), SpoolerError> {
        let mut order: Vec<usize> = (0..self.group_count()).collect();
        order.sort_by(|a, b| self.weights[*b].cmp(&self.weights[*a]).then(a.cmp(b)));

        for group in order {
            for offset in 0..self.width {
                if self.holders[group * self.width + offset].is_some() {
                    continue;
                }
                let (node, _) = self
                    .best_taker(group)
                    .ok_or(SpoolerError::Infeasible)?;
                self.place(group, offset, node);
            }
        }
        Ok(())
    }

    /// The node outside `group` with room and the strongest claim on one of its spools
    fn best_taker(&self, group: usize) -> Option<(usize, Score)> {
        let weight = self.weights[group];
        let mut best: Option<(usize, Score)> = None;

        for (node, state) in self.nodes.iter().enumerate() {
            if self.holds(group, node) || !state.has_room(weight) {
                continue;
            }
            let score = Score::claim(state.target, state.load, weight);
            if self.is_better(group, node, score, best) {
                best = Some((node, score));
            }
        }
        best
    }

    /// The holder in `group` that gains most by giving up its spool
    fn best_giver(&self, group: usize) -> Option<(usize, Score)> {
        let weight = self.weights[group];
        let mut best: Option<(usize, Score)> = None;

        for holder in &self.holders[group * self.width..(group + 1) * self.width] {
            let Some(node) = *holder else {
                continue;
            };
            let state = &self.nodes[node];
            let score = Score::release(state.target, state.load, weight);
            if self.is_better(group, node, score, best) {
                best = Some((node, score));
            }
        }
        best
    }

    fn is_better(&self, group: usize, node: usize, score: Score, best: Option<(usize, Score)>) -> bool {
        match best {
            None => true,
            Some((best_node, best_score)) => {
                score > best_score
                    || (score == best_score
                        && self.tie_break(group, node) > self.tie_break(group, best_node))
            }
        }
    }

    /// Move spools while a move clears `MIN_MOVE_GAIN` and the budget allows
    fn rebalance(&mut self) {
        let budget = self.stored.as_u128() * MOVE_BUDGET.as_u128() / BasisPoints::MAX as u128;
        let mut allowance = StorageUnits(budget as u64);

        // Every move cuts the objective, so this only guards the loop's length
        for _ in 0..self.holders.len() {
            let Some((group, giver, taker)) = self.best_move(allowance) else {
                return;
            };
            let Some(offset) = self.offset_of(group, giver) else {
                return;
            };
            self.vacate(group, offset);
            self.place(group, offset, taker);
            allowance = StorageUnits(allowance.0 - self.weights[group].0);
        }
    }

    /// The move with the largest objective cut per byte, among spools within `allowance`
    fn best_move(&self, allowance: StorageUnits) -> Option<(usize, usize, usize)> {
        let mut best: Option<(usize, usize, usize, Score)> = None;

        for group in 0..self.group_count() {
            // Empty spools move no bytes, and will fill evenly as new data lands
            if self.sizes[group].is_zero() || self.weights[group] > allowance {
                continue;
            }
            let (Some((giver, release)), Some((taker, claim))) =
                (self.best_giver(group), self.best_taker(group))
            else {
                continue;
            };
            let gain = release.saturating_add(claim);
            if gain >= MIN_MOVE_GAIN && best.is_none_or(|(_, _, _, best_gain)| gain > best_gain) {
                best = Some((group, giver, taker, gain));
            }
        }
        best.map(|(group, giver, taker, _)| (group, giver, taker))
    }

    fn into_assignment(self) -> Result<Vec<Address>, SpoolerError> {
        let mut assignment = Vec::with_capacity(self.holders.len());
        for holder in &self.holders {
            let node = holder.ok_or(SpoolerError::Infeasible)?;
            assignment.push(self.nodes[node].address);
        }
        Ok(assignment)
    }
}

/// A node holds at most one spool per group, and at most its capacity over the growth margin
fn node_bound(capacity: StorageUnits, per_spool_total: StorageUnits) -> StorageUnits {
    if capacity.is_zero() {
        return per_spool_total;
    }
    StorageUnits(capacity.0 / GROWTH_MARGIN).min(per_spool_total)
}
