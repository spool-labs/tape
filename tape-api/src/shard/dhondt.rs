use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::collections::BinaryHeap;

use super::{NodeId, StakeLeaderSet};

#[derive(Clone, Debug)]
struct Quotient {
    numer: u128,
    denom: u128,
}

impl Quotient {
    fn from_quot(numer: u128, denom: u128) -> Self {
        assert!(denom > 0, "Denominator must be > 0");
        Self { numer, denom }
    }
}

#[derive(Clone, Debug)]
struct Entry {
    quotient: Quotient,
    tie_breaker: u64,
    index: usize,
}

impl PartialEq for Entry {
    fn eq(&self, other: &Self) -> bool {
        self.quotient.numer * other.quotient.denom
            == other.quotient.numer * self.quotient.denom
            && self.tie_breaker == other.tie_breaker
            && self.index == other.index
    }
}

impl Eq for Entry {}

impl PartialOrd for Entry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

fn compare_quotients(a: &Quotient, b: &Quotient) -> Ordering {
    let left = a.numer.saturating_mul(b.denom);
    let right = b.numer.saturating_mul(a.denom);
    left.cmp(&right)
}

fn tie_break(t1: u64, i1: usize, t2: u64, i2: usize) -> Ordering {
    match t1.cmp(&t2) {
        Ordering::Greater => Ordering::Greater,
        Ordering::Less => Ordering::Less,
        Ordering::Equal => i2.cmp(&i1),
    }
}

impl Ord for Entry {
    fn cmp(&self, other: &Self) -> Ordering {
        match compare_quotients(&self.quotient, &other.quotient) {
            Ordering::Equal => tie_break(self.tie_breaker, self.index, other.tie_breaker, other.index),
            ord => ord,
        }
    }
}

pub fn max_shards_per_node(node_count: u64, shard_count: u64) -> u64 {
    const MIN_NODES: u64 = 20;
    const MAX_PER_NODE_SHARE: u64 = 10; // 10%

    if shard_count == 0 || node_count == 0 {
        return 0;
    }

    if node_count >= MIN_NODES {
        shard_count / MAX_PER_NODE_SHARE
    } else {
        // Scale linearly between 1 and MIN_NODES
        let num = shard_count.saturating_mul(MIN_NODES);
        let den = node_count.saturating_mul(MAX_PER_NODE_SHARE);
        num.saturating_add(den - 1) / den
    }
}

/// Allocate shards to nodes using the D'Hondt method with tie-breaking and max shard limits.
pub fn allocate_shards_dhondt(
    node_priorities: &[u64], 
    shard_count: u16, 
    stake: &[u64]
) -> Vec<u16> {

    let node_count = stake.len();
    if node_count == 0 {
        return Vec::new();
    }

    let total_stake: u128 = stake.iter().map(|&x| x as u128).sum();
    assert!(total_stake > 0, "Total stake must be > 0");

    let n_shards_u64 = shard_count as u64;
    let max_shards = max_shards_per_node(node_count as u64, n_shards_u64);
    let dist_number = (total_stake as u128 / (n_shards_u64 as u128 + 1)) + 1;

    let mut shards: Vec<u64> = stake
        .iter()
        .map(|&s| {
            let base = (s as u128) / dist_number;
            let v = base as u64;
            v.min(max_shards)
        })
        .collect();

    let mut heap = BinaryHeap::new();
    for (i, &s) in stake.iter().enumerate() {
        if shards[i] != max_shards {
            let denom = shards[i] + 1;
            let quotient = Quotient::from_quot(s as u128, denom as u128);
            heap.push(Entry {
                quotient,
                tie_breaker: node_priorities[i],
                index: i,
            });
        }
    }

    let mut distributed: u64 = shards.iter().sum();
    while distributed < n_shards_u64 {
        let Entry { quotient: _, tie_breaker, index } =
            heap.pop().expect("Heap empty while distributing shards");

        shards[index] += 1;
        distributed += 1;
        if shards[index] != max_shards {
            let denom = shards[index] + 1;
            let q = Quotient::from_quot(stake[index] as u128, denom as u128);
            heap.push(Entry { quotient: q, tie_breaker, index });
        }
    }

    shards.into_iter().map(|x| x as u16).collect()
}

pub fn stake_weighted_shard_counts<const N: usize>(
    leaders: &StakeLeaderSet<N>,
    shard_count: u16,
) -> BTreeMap<NodeId, u16> {
    let node_count = leaders.len as usize;
    if node_count == 0 || shard_count == 0 {
        return BTreeMap::new();
    }

    let node_ids: Vec<NodeId> = leaders.node_ids[0..node_count].to_vec();
    let stakes: Vec<u64> = leaders.stakes[0..node_count].to_vec();

    let node_priorities: Vec<u64> = (0..node_count).map(|i| (node_count - i) as u64).collect();

    let shards_vec = allocate_shards_dhondt(&node_priorities, shard_count, &stakes);
    let mut distribution = BTreeMap::new();
    for (i, &sh) in shards_vec.iter().enumerate() {
        if sh > 0 {
            distribution.insert(node_ids[i], sh);
        }
    }

    distribution
}

#[cfg(test)]
mod tests {
    use super::*;

    const MAX_NODES: usize = 256;

    type TestStakeLeaderSet = StakeLeaderSet<{ MAX_NODES }>;

    #[test]
    fn test_max_shards_per_node_threshold() {
        assert_eq!(max_shards_per_node(20, 1000), 100);
        assert_eq!(max_shards_per_node(25, 1000), 100);
        assert_eq!(max_shards_per_node(5, 1000), 400);
    }

    #[test]
    fn test_dhondt_basic_even() {
        let stake = vec![25_000_u64, 25_000, 25_000, 25_000];
        let priorities: Vec<u64> = (0..4).map(|i| 4 - i).collect();
        assert_eq!(allocate_shards_dhondt(&priorities, 4, &stake), vec![1, 1, 1, 1]);

        let res = allocate_shards_dhondt(&priorities, 1000, &stake);
        assert_eq!(res.iter().map(|&x| x as u64).sum::<u64>(), 1000);
        assert_eq!(res, vec![250, 250, 250, 250]);
    }

    #[test]
    fn test_dhondt_basic_uneven() {
        let stake = vec![50_000_u64, 30_000, 15_000, 5_000];
        let priorities: Vec<u64> = (0..4).map(|i| 4 - i).collect();
        assert_eq!(allocate_shards_dhondt(&priorities, 4, &stake), vec![2, 2, 0, 0]);

        let res = allocate_shards_dhondt(&priorities, 1000, &stake);
        assert_eq!(res.iter().map(|&x| x as u64).sum::<u64>(), 1000);
        assert_eq!(res, vec![500, 300, 150, 50]);
    }

    #[test]
    fn test_stake_weighted_equal_three_nodes() {
        // Expect a tie-broken split 4,3,3 for 10 shards
        let leaders = TestStakeLeaderSet::new(vec![(NodeId(1), 1000), (NodeId(2), 1000), (NodeId(3), 1000)]);
        let res = stake_weighted_shard_counts(&leaders, 10);
        assert_eq!(res.values().copied().sum::<u16>(), 10);
        let v: Vec<u16> = [NodeId(1), NodeId(2), NodeId(3)]
            .iter()
            .map(|nid| *res.get(nid).unwrap_or(&0))
            .collect();
        assert_eq!(v, vec![4, 3, 3]);
    }
}
