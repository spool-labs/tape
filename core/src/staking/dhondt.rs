use core::cmp::Ordering;
use std::collections::BinaryHeap;

use super::quotient::{
    Quotient, compare_quotients, tie_break
};

/// An entry in the priority queue for D'Hondt allocation, containing a quotient,
#[derive(Clone, Debug)]
pub struct Entry {
    pub quotient: Quotient,
    pub tie_breaker: u64,
    pub index: usize,
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

impl Ord for Entry {
    fn cmp(&self, other: &Self) -> Ordering {
        match compare_quotients(&self.quotient, &other.quotient) {
            Ordering::Equal => tie_break(self.tie_breaker, self.index, other.tie_breaker, other.index),
            ord => ord,
        }
    }
}

/// Allocate shards to nodes using the D'Hondt method with tie-breaking and max shard limits.
pub fn allocate_shards(
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_max_shards_per_node_threshold() {
        assert_eq!(max_shards_per_node(20, 1000), 100);
        assert_eq!(max_shards_per_node(25, 1000), 100);
        assert_eq!(max_shards_per_node(5, 1000), 400);
    }

    #[test]
    fn test_basic_even() {
        let stake = vec![25_000_u64, 25_000, 25_000, 25_000];
        let priorities: Vec<u64> = (0..4).map(|i| 4 - i).collect();
        assert_eq!(allocate_shards(&priorities, 4, &stake), vec![1, 1, 1, 1]);

        let res = allocate_shards(&priorities, 1000, &stake);
        assert_eq!(res.iter().map(|&x| x as u64).sum::<u64>(), 1000);
        assert_eq!(res, vec![250, 250, 250, 250]);
    }

    #[test]
    fn test_basic_uneven() {
        let stake = vec![50_000_u64, 30_000, 15_000, 5_000];
        let priorities: Vec<u64> = (0..4).map(|i| 4 - i).collect();
        assert_eq!(allocate_shards(&priorities, 4, &stake), vec![2, 2, 0, 0]);

        let res = allocate_shards(&priorities, 1000, &stake);
        assert_eq!(res.iter().map(|&x| x as u64).sum::<u64>(), 1000);
        assert_eq!(res, vec![500, 300, 150, 50]);
    }
}
