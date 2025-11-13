use super::priority::{ SpoolPriority, NodePriority };
use super::spool::SpoolCount;
use crate::types::*;

const MAX_SPOOL_ALLOCATION: u64 = 20; // No committee member may have more than 20 spools (2% of the 1000)
const MIN_MEMBER_COUNT: u64 = 32;     // Minimum committee size needed to enforce max per-node share

/// Allocate spools to nodes using the D'Hondt method with tie-breaking and max spool limits.
/// Refer to https://en.wikipedia.org/wiki/D%27Hondt_method for details on the algorithm.
pub fn dhondt_allocate(
    stake_weight: &[Coin<TAPE>],
    spool_count: u16, 
) -> Vec<SpoolCount> {
    let node_count = stake_weight.len();
    if node_count == 0 {
        return Vec::new();
    }

    let total_stake: u128 = stake_weight.iter().map(|&x| x.as_u128()).sum();
    assert!(total_stake > 0, "Total stake_weight must be > 0");

    let n_spools_u64 = spool_count as u64;
    let max_spools = cap_spools(node_count as u64, n_spools_u64);
    let dist_number = (total_stake as u128 / (n_spools_u64 as u128 + 1)) + 1;

    let mut spools: Vec<u64> = stake_weight
        .iter()
        .map(|&s| {
            let base = (s.as_u128()) / dist_number;
            let v = base as u64;
            v.min(max_spools)
        })
        .collect();

    let mut heap = MaxHeap::with_capacity(node_count);
    for (i, &s) in stake_weight.iter().enumerate() {
        if spools[i] != max_spools {
            let d = spools[i] + 1;
            let priority = SpoolPriority::from(s.into(), d);
            heap.push(NodePriority {
                priority,
                tie_breaker: (node_count - i) as u64,
                index: i,
            });
        }
    }

    let mut distributed: u64 = spools.iter().sum();
    while distributed < n_spools_u64 {
        let NodePriority {
            priority: _,
            tie_breaker,
            index,
        } = heap.pop().expect("Heap empty while distributing spools");

        spools[index] += 1;
        distributed += 1;
        if spools[index] != max_spools {
            let d = spools[index] + 1;
            let q = SpoolPriority::from(stake_weight[index].into(), d);
            heap.push(NodePriority {
                priority: q,
                tie_breaker,
                index,
            });
        }
    }

    spools
        .into_iter()
        .map(|x| x as u16)
        .collect()
}

/// Limit the maximum number of spools per node based on the total number of nodes.
pub fn cap_spools(node_count: u64, spool_count: u64) -> u64 {
    if spool_count == 0 || node_count == 0 {
        return 0;
    }

    // If there are at least 32 nodes, a node can have at most MAX_SPOOL_ALLOCATION of the spools.
    if node_count >= MIN_MEMBER_COUNT {
        spool_count / MAX_SPOOL_ALLOCATION

    // Otherwise, if there are fewer than 32 nodes, the limit scales linearly up to 10%.
    } else {
        // Scale linearly between 1 and MIN_MEMBER_COUNT
        let num = spool_count.saturating_mul(MIN_MEMBER_COUNT);
        let den = node_count.saturating_mul(MAX_SPOOL_ALLOCATION);
        num.saturating_add(den - 1) / den
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn weights(v: &[u64]) -> Vec<TAPE> {
        v.iter().map(|&x| TAPE::new(x)).collect()
    }

    #[test]
    fn test_basic_even() {
        let stake = weights(&[25_000, 25_000, 25_000, 25_000]);
        assert_eq!(dhondt_allocate(&stake, 4), vec![1, 1, 1, 1]);

        let res = dhondt_allocate(&stake, 1000);
        assert_eq!(res.iter().map(|&x| x as u64).sum::<u64>(), 1000);
        assert_eq!(res, vec![250, 250, 250, 250]);
    }

    #[test]
    fn test_basic_uneven() {
        let stake = weights(&[50_000, 30_000, 15_000, 5_000]);

        // With MIN_MEMBER_COUNT=32 and 5% cap, per-node cap for 4 spools and 4 nodes is 2.
        assert_eq!(dhondt_allocate(&stake, 4), vec![2, 2, 0, 0]);

        let res = dhondt_allocate(&stake, 1000);

        // For 1000 spools and 4 nodes, cap is 400.
        assert_eq!(res.iter().map(|&x| x as u64).sum::<u64>(), 1000);
        assert_eq!(res, vec![400, 360, 180, 60]);
    }

    #[test]
    fn test_ties() {
        // Even stake with uneven spool distribution
        let stake = weights(&[25_000, 25_000, 25_000, 25_000]);
        assert_eq!(dhondt_allocate(&stake, 7), vec![2, 2, 2, 1]);
        assert_eq!(dhondt_allocate(&stake, 6), vec![2, 2, 1, 1]);

        // Small uneven stake
        let stake = weights(&[200, 200, 200, 100]);
        assert_eq!(dhondt_allocate(&stake, 7), vec![2, 2, 2, 1]);

        // Larger stake with ties (cap = ceil(18*32*5/(5*100)) = 6), unchanged
        let stake = weights(&[780_000, 650_000, 520_000, 390_000, 260_000]);
        assert_eq!(dhondt_allocate(&stake, 18), vec![6, 5, 4, 2, 1]);
    }

    #[test]
    fn test_edge_cases() {
        // No spools
        let stake = weights(&[100, 90, 80]);
        assert_eq!(dhondt_allocate(&stake, 0), vec![0, 0, 0]);

        // Low stake
        let stake = weights(&[1, 0, 0]);

        // With MIN_MEMBER_COUNT=32 and 5% cap, cap for 5 spools and 3 nodes is 3.
        assert_eq!(dhondt_allocate(&stake, 5), vec![3, 2, 0]);

        // Nearly identical stake
        let s = 1_000_000;
        let stake = weights(&[s, s - 1]);
        assert_eq!(dhondt_allocate(&stake, 3), vec![2, 1]);

        // Large stake (cap = ceil(500*32*5/(3*100)) = 267), unchanged
        let stake = weights(&[1_000_000_000_000, 900_000_000_000, 100_000_000_000]);
        assert_eq!(dhondt_allocate(&stake, 500), vec![250, 225, 25]);
    }
}
