//! Scenario tests for byte-aware spool assignment.

use std::collections::{BTreeMap, BTreeSet};

use tape_core::erasure::GROUP_SIZE;
use tape_core::spooler::SpoolerError;
use tape_core::types::{StorageUnits, TAPE};
use tape_crypto::address::Address;
use tape_crypto::hash::Hash;
use tape_spooler::assign::{Candidate, assign_spools};

const MB: u64 = StorageUnits::MB;

fn address(index: u64) -> Address {
    let mut bytes = [0u8; 32];
    bytes[..8].copy_from_slice(&index.to_le_bytes());
    bytes[31] = 0xAA;
    Address::new(bytes)
}

fn committee(stakes: &[u64]) -> Vec<Candidate> {
    stakes
        .iter()
        .enumerate()
        .map(|(index, stake)| Candidate {
            node: address(index as u64),
            stake: TAPE(*stake),
            capacity: StorageUnits::zero(),
        })
        .collect()
}

fn sizes(megabytes: &[u64]) -> Vec<StorageUnits> {
    megabytes.iter().map(|size| StorageUnits(size * MB)).collect()
}

fn empty(groups: usize) -> Vec<Option<Address>> {
    vec![None; groups * GROUP_SIZE]
}

fn previous(assignment: &[Address]) -> Vec<Option<Address>> {
    assignment.iter().copied().map(Some).collect()
}

fn nonce(seed: u8) -> Hash {
    Hash([seed; 32])
}

fn loads(assignment: &[Address], sizes: &[StorageUnits]) -> BTreeMap<Address, u64> {
    let mut loads = BTreeMap::new();
    for (spool, node) in assignment.iter().enumerate() {
        *loads.entry(*node).or_insert(0) += sizes[spool / GROUP_SIZE].0;
    }
    loads
}

fn targets(candidates: &[Candidate], sizes: &[StorageUnits]) -> BTreeMap<Address, f64> {
    let stored: u64 = sizes.iter().map(|size| size.0 * GROUP_SIZE as u64).sum();
    let total_stake: u64 = candidates.iter().map(|candidate| candidate.stake.0).sum();
    candidates
        .iter()
        .map(|candidate| {
            let share = candidate.stake.0 as f64 / total_stake as f64;
            (candidate.node, share * stored as f64)
        })
        .collect()
}

/// Share of stored bytes held beyond stake share
fn misplaced(assignment: &[Address], candidates: &[Candidate], sizes: &[StorageUnits]) -> f64 {
    let stored: u64 = sizes.iter().map(|size| size.0 * GROUP_SIZE as u64).sum();
    let loads = loads(assignment, sizes);
    let targets = targets(candidates, sizes);
    let mut total = 0.0;
    for (node, target) in &targets {
        let load = *loads.get(node).unwrap_or(&0) as f64;
        total += (load - target).abs();
    }
    total / 2.0 / stored as f64
}

fn moved_bytes(before: &[Address], after: &[Address], sizes: &[StorageUnits]) -> u64 {
    let mut moved = 0;
    for (spool, (old, new)) in before.iter().zip(after).enumerate() {
        if old != new {
            moved += sizes[spool / GROUP_SIZE].0;
        }
    }
    moved
}

fn assert_distinct(assignment: &[Address]) {
    for (group, holders) in assignment.chunks(GROUP_SIZE).enumerate() {
        let unique: BTreeSet<&Address> = holders.iter().collect();
        assert_eq!(unique.len(), GROUP_SIZE, "group {group} repeats a node");
    }
}

fn assert_within_capacity(assignment: &[Address], candidates: &[Candidate], sizes: &[StorageUnits]) {
    let loads = loads(assignment, sizes);
    for candidate in candidates {
        if candidate.capacity.is_zero() {
            continue;
        }
        let load = *loads.get(&candidate.node).unwrap_or(&0);
        assert!(
            2 * load <= candidate.capacity.0,
            "node {} holds {load} against capacity {}",
            candidate.node,
            candidate.capacity.0,
        );
    }
}

fn uneven_sizes(groups: usize) -> Vec<StorageUnits> {
    let megabytes: Vec<u64> = (0..groups as u64).map(|group| 100 + (group * 37) % 90).collect();
    sizes(&megabytes)
}

fn ramp(count: u64) -> Vec<u64> {
    (1..=count).map(|stake| stake * 1_000).collect()
}

#[test]
fn fills_every_group() {
    let candidates = committee(&ramp(30));
    let sizes = uneven_sizes(10);
    let assignment = assign_spools(&sizes, &empty(10), &candidates, &nonce(1)).expect("assign");

    assert_eq!(assignment.len(), 10 * GROUP_SIZE);
    assert_distinct(&assignment);
}

#[test]
fn bytes_follow_stake() {
    let candidates = committee(&ramp(40));
    let sizes = sizes(&[1_024; 50]);
    let assignment = assign_spools(&sizes, &empty(50), &candidates, &nonce(1)).expect("assign");

    let loads = loads(&assignment, &sizes);
    for (node, target) in targets(&candidates, &sizes) {
        let load = *loads.get(&node).unwrap_or(&0) as f64;
        assert!((load - target).abs() <= (1_024 * MB) as f64, "node {node} load {load} target {target}");
    }
}

#[test]
fn empty_groups_spread_by_stake() {
    let candidates = committee(&ramp(40));
    let sizes = sizes(&[0; 50]);
    let assignment = assign_spools(&sizes, &empty(50), &candidates, &nonce(1)).expect("assign");

    let total_stake: u64 = ramp(40).iter().sum();
    let mut counts: BTreeMap<Address, u64> = BTreeMap::new();
    for node in &assignment {
        *counts.entry(*node).or_insert(0) += 1;
    }
    for candidate in &candidates {
        let quota = candidate.stake.0 as f64 * assignment.len() as f64 / total_stake as f64;
        let count = *counts.get(&candidate.node).unwrap_or(&0) as f64;
        assert!((count - quota).abs() <= 1.0, "node {} holds {count} for quota {quota}", candidate.node);
    }
}

#[test]
fn settled_layout_stays_put() {
    let candidates = committee(&ramp(30));
    let sizes = uneven_sizes(10);
    let first = assign_spools(&sizes, &empty(10), &candidates, &nonce(1)).expect("first");
    let second = assign_spools(&sizes, &previous(&first), &candidates, &nonce(2)).expect("second");

    assert_eq!(first, second);
}

#[test]
fn stake_drift_moves_nothing() {
    let sizes = uneven_sizes(10);
    let settled = assign_spools(&sizes, &empty(10), &committee(&ramp(30)), &nonce(1)).expect("settled");

    // Every stake moves up to 3%, in both directions
    let drifted: Vec<u64> = ramp(30)
        .iter()
        .enumerate()
        .map(|(index, stake)| stake * (97 + (index as u64 * 5) % 7) / 100)
        .collect();
    let next = assign_spools(&sizes, &previous(&settled), &committee(&drifted), &nonce(2)).expect("next");

    assert_eq!(moved_bytes(&settled, &next, &sizes), 0);
}

#[test]
fn departed_spools_refill() {
    let candidates = committee(&ramp(30));
    let sizes = uneven_sizes(10);
    let settled = assign_spools(&sizes, &empty(10), &candidates, &nonce(1)).expect("settled");

    let departed = candidates[29].node;
    let remaining = &candidates[..29];
    let next = assign_spools(&sizes, &previous(&settled), remaining, &nonce(2)).expect("next");

    assert_distinct(&next);
    assert!(!next.contains(&departed));
    let departed_bytes = loads(&settled, &sizes)[&departed];
    let stored: u64 = sizes.iter().map(|size| size.0 * GROUP_SIZE as u64).sum();
    // The departed node's spools move, plus at most the rebalance budget
    assert!(moved_bytes(&settled, &next, &sizes) <= departed_bytes + stored / 20);
}

#[test]
fn capacity_holds() {
    let mut candidates = committee(&ramp(30));
    let sizes = uneven_sizes(10);
    for candidate in candidates.iter_mut().rev().take(10) {
        candidate.capacity = StorageUnits(800 * MB);
    }
    let assignment = assign_spools(&sizes, &empty(10), &candidates, &nonce(1)).expect("assign");

    assert_distinct(&assignment);
    assert_within_capacity(&assignment, &candidates, &sizes);
}

#[test]
fn no_room_means_no_spools() {
    let mut candidates = committee(&ramp(30));
    let sizes = uneven_sizes(10);
    let smallest = sizes.iter().min().expect("groups").0;
    candidates[29].capacity = StorageUnits(2 * smallest - 1);
    let assignment = assign_spools(&sizes, &empty(10), &candidates, &nonce(1)).expect("assign");

    assert!(!assignment.contains(&candidates[29].node));
}

#[test]
fn lowered_capacity_releases() {
    let mut candidates = committee(&ramp(30));
    let sizes = uneven_sizes(10);
    let settled = assign_spools(&sizes, &empty(10), &candidates, &nonce(1)).expect("settled");

    let node = candidates[29].node;
    let load = loads(&settled, &sizes)[&node];
    candidates[29].capacity = StorageUnits(load);
    let next = assign_spools(&sizes, &previous(&settled), &candidates, &nonce(2)).expect("next");

    assert_distinct(&next);
    assert_within_capacity(&next, &candidates, &sizes);
}

#[test]
fn expansion_keeps_old_spools() {
    let candidates = committee(&ramp(30));
    let old_sizes = uneven_sizes(5);
    let settled = assign_spools(&old_sizes, &empty(5), &candidates, &nonce(1)).expect("settled");

    let mut new_sizes = old_sizes.clone();
    new_sizes.extend(sizes(&[0; 5]));
    let mut current = previous(&settled);
    current.extend(empty(5));
    let next = assign_spools(&new_sizes, &current, &candidates, &nonce(2)).expect("next");

    assert_distinct(&next);
    assert_eq!(&next[..settled.len()], &settled[..]);
}

#[test]
fn skew_heals_within_budget() {
    let candidates = committee(&ramp(40));
    let sizes = uneven_sizes(10);
    let stored: u64 = sizes.iter().map(|size| size.0 * GROUP_SIZE as u64).sum();

    // The 20 largest stakers hold every group, as D'Hondt left devnet
    let top: Vec<Address> = candidates[20..].iter().map(|candidate| candidate.node).collect();
    let mut layout: Vec<Address> = (0..10).flat_map(|_| top.iter().copied()).collect();
    let start = misplaced(&layout, &candidates, &sizes);

    for epoch in 0..20u8 {
        let next = assign_spools(&sizes, &previous(&layout), &candidates, &nonce(epoch)).expect("next");
        assert!(moved_bytes(&layout, &next, &sizes) <= stored / 20);
        assert_distinct(&next);
        layout = next;
    }

    let end = misplaced(&layout, &candidates, &sizes);
    assert!(end < start / 4.0, "misplaced {start:.3} -> {end:.3}");
    let loads = loads(&layout, &sizes);
    let largest = sizes.iter().max().expect("groups").0 as f64;
    for (node, target) in targets(&candidates, &sizes) {
        let load = *loads.get(&node).unwrap_or(&0) as f64;
        assert!((load - target).abs() <= largest.max(target / 10.0), "node {node} load {load} target {target}");
    }
}

#[test]
fn order_free() {
    let mut candidates = committee(&ramp(30));
    for (index, candidate) in candidates.iter_mut().enumerate() {
        if index % 3 == 0 {
            candidate.capacity = StorageUnits(900 * MB);
        }
    }
    let sizes = uneven_sizes(10);
    let forward = assign_spools(&sizes, &empty(10), &candidates, &nonce(1)).expect("forward");

    candidates.reverse();
    let reversed = assign_spools(&sizes, &empty(10), &candidates, &nonce(1)).expect("reversed");

    assert_eq!(forward, reversed);
}

#[test]
fn too_few_nodes() {
    let result = assign_spools(&uneven_sizes(2), &empty(2), &committee(&ramp(19)), &nonce(1));
    assert_eq!(result, Err(SpoolerError::InsufficientNodes));
}

#[test]
fn no_room_anywhere() {
    let mut candidates = committee(&ramp(20));
    candidates[0].capacity = StorageUnits(MB);
    let result = assign_spools(&uneven_sizes(2), &empty(2), &candidates, &nonce(1));
    assert_eq!(result, Err(SpoolerError::Infeasible));
}

#[test]
fn length_mismatch() {
    let result = assign_spools(&uneven_sizes(2), &empty(1), &committee(&ramp(20)), &nonce(1));
    assert_eq!(result, Err(SpoolerError::LengthMismatch));
}

#[test]
fn duplicate_node() {
    let mut candidates = committee(&ramp(20));
    candidates[1].node = candidates[0].node;
    let result = assign_spools(&uneven_sizes(2), &empty(2), &candidates, &nonce(1));
    assert_eq!(result, Err(SpoolerError::DuplicateNode));
}
