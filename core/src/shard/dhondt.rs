use std::collections::BinaryHeap;
use super::priority::{ ShardPriority, NodePriority };

/// Allocate shards to nodes using the D'Hondt method with tie-breaking and max shard limits.
pub fn allocate_shards(
    node_priorities: &[u64], 
    stake: &[u64],
    shard_count: u16, 
) -> Vec<u16> {

    let node_count = stake.len();
    if node_count == 0 {
        return Vec::new();
    }

    let total_stake: u128 = stake.iter().map(|&x| x as u128).sum();
    assert!(total_stake > 0, "Total stake must be > 0");

    let n_shards_u64 = shard_count as u64;
    let max_shards = cap_shards(node_count as u64, n_shards_u64);
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
            let d = shards[i] + 1;
            let priority = ShardPriority::from(s as u128, d as u128);
            heap.push(NodePriority {
                priority,
                tie_breaker: node_priorities[i],
                index: i,
            });
        }
    }

    let mut distributed: u64 = shards.iter().sum();
    while distributed < n_shards_u64 {
        let NodePriority {
            priority: _,
            tie_breaker,
            index,
        } = heap.pop().expect("Heap empty while distributing shards");

        shards[index] += 1;
        distributed += 1;
        if shards[index] != max_shards {
            let d = shards[index] + 1;
            let q = ShardPriority::from(stake[index] as u128, d as u128);
            heap.push(NodePriority {
                priority: q,
                tie_breaker,
                index,
            });
        }
    }

    shards.into_iter().map(|x| x as u16).collect()
}

/// Limit the maximum number of shards per node based on the total number of nodes.
/// - If there are at least 20 nodes, a node can have at most 10% of the shards.
/// - If there are fewer than 20 nodes, the limit scales linearly up to 10%.
pub fn cap_shards(node_count: u64, shard_count: u64) -> u64 {
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
    fn test_max_shards_per_node() {
        assert_eq!(cap_shards(20, 1000), 100);
        assert_eq!(cap_shards(25, 1000), 100);
        assert_eq!(cap_shards(5, 1000), 400);
    }

    #[test]
    fn test_basic_even() {
        let stake: Vec<u64> = vec![25_000, 25_000, 25_000, 25_000];
        let priorities: Vec<u64> = (0..4).map(|i| 4 - i as u64).collect();
        assert_eq!(allocate_shards(&priorities, &stake, 4), vec![1, 1, 1, 1]);

        let res = allocate_shards(&priorities, &stake, 1000);
        assert_eq!(res.iter().map(|&x| x as u64).sum::<u64>(), 1000);
        assert_eq!(res, vec![250, 250, 250, 250]);
    }

    #[test]
    fn test_basic_uneven() {
        let stake: Vec<u64> = vec![50_000, 30_000, 15_000, 5_000];
        let priorities: Vec<u64> = (0..4).map(|i| 4 - i as u64).collect();
        assert_eq!(allocate_shards(&priorities, &stake, 4), vec![2, 2, 0, 0]);

        let res = allocate_shards(&priorities, &stake, 1000);
        assert_eq!(res.iter().map(|&x| x as u64).sum::<u64>(), 1000);
        assert_eq!(res, vec![500, 300, 150, 50]);
    }
}
