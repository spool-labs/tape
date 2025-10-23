use std::collections::BinaryHeap;
use super::priority::{ SeatPriority, NodePriority };
use crate::types::*;

/// Allocate seats to nodes using the D'Hondt method with tie-breaking and max seat limits.
pub fn allocate_seats(
    stake_weight: &[Coin<TAPE>],
    seat_count: u16, 
) -> Vec<u16> {
    let node_count = stake_weight.len();
    if node_count == 0 {
        return Vec::new();
    }

    let total_stake: u128 = stake_weight.iter().map(|&x| x.as_u128()).sum();
    assert!(total_stake > 0, "Total stake_weight must be > 0");

    let n_seats_u64 = seat_count as u64;
    let max_seats = cap_seats(node_count as u64, n_seats_u64);
    let dist_number = (total_stake as u128 / (n_seats_u64 as u128 + 1)) + 1;

    let mut seats: Vec<u64> = stake_weight
        .iter()
        .map(|&s| {
            let base = (s.as_u128()) / dist_number;
            let v = base as u64;
            v.min(max_seats)
        })
        .collect();

    let mut heap = BinaryHeap::new();
    for (i, &s) in stake_weight.iter().enumerate() {
        if seats[i] != max_seats {
            let d = seats[i] + 1;
            let priority = SeatPriority::from(s.into(), d);
            heap.push(NodePriority {
                priority,
                tie_breaker: (node_count - i) as u64,
                index: i,
            });
        }
    }

    let mut distributed: u64 = seats.iter().sum();
    while distributed < n_seats_u64 {
        let NodePriority {
            priority: _,
            tie_breaker,
            index,
        } = heap.pop().expect("Heap empty while distributing seats");

        seats[index] += 1;
        distributed += 1;
        if seats[index] != max_seats {
            let d = seats[index] + 1;
            let q = SeatPriority::from(stake_weight[index].into(), d);
            heap.push(NodePriority {
                priority: q,
                tie_breaker,
                index,
            });
        }
    }

    seats
        .into_iter()
        .map(|x| x as u16)
        .collect()
}

/// Limit the maximum number of seats per node based on the total number of nodes.
/// - If there are at least 20 nodes, a node can have at most 10% of the seats.
/// - If there are fewer than 20 nodes, the limit scales linearly up to 10%.
pub fn cap_seats(node_count: u64, seat_count: u64) -> u64 {
    const MIN_NODES: u64 = 20;
    const MAX_PER_NODE_SHARE: u64 = 10; // 10%

    if seat_count == 0 || node_count == 0 {
        return 0;
    }

    if node_count >= MIN_NODES {
        seat_count / MAX_PER_NODE_SHARE
    } else {
        // Scale linearly between 1 and MIN_NODES
        let num = seat_count.saturating_mul(MIN_NODES);
        let den = node_count.saturating_mul(MAX_PER_NODE_SHARE);
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
        assert_eq!(allocate_seats(&stake, 4), vec![1, 1, 1, 1]);

        let res = allocate_seats(&stake, 1000);
        assert_eq!(res.iter().map(|&x| x as u64).sum::<u64>(), 1000);
        assert_eq!(res, vec![250, 250, 250, 250]);
    }

    #[test]
    fn test_basic_uneven() {
        let stake = weights(&[50_000, 30_000, 15_000, 5_000]);
        assert_eq!(allocate_seats(&stake, 4), vec![2, 2, 0, 0]);

        let res = allocate_seats(&stake, 1000);
        assert_eq!(res.iter().map(|&x| x as u64).sum::<u64>(), 1000);
        assert_eq!(res, vec![500, 300, 150, 50]);
    }

    #[test]
    fn test_ties() {
        // Even stake with uneven seat distribution
        let stake = weights(&[25_000, 25_000, 25_000, 25_000]);
        assert_eq!(allocate_seats(&stake, 7), vec![2, 2, 2, 1]);
        assert_eq!(allocate_seats(&stake, 6), vec![2, 2, 1, 1]);

        // Small uneven stake
        let stake = weights(&[200, 200, 200, 100]);
        assert_eq!(allocate_seats(&stake, 7), vec![2, 2, 2, 1]);

        // Larger stake with ties
        let stake = weights(&[780_000, 650_000, 520_000, 390_000, 260_000]);
        assert_eq!(allocate_seats(&stake, 18), vec![6, 5, 4, 2, 1]);
    }

    #[test]
    fn test_edge_cases() {
        // No seats
        let stake = weights(&[100, 90, 80]);
        assert_eq!(allocate_seats(&stake, 0), vec![0, 0, 0]);

        // Low stake
        let stake = weights(&[1, 0, 0]);
        assert_eq!(allocate_seats(&stake, 5), vec![4, 1, 0]);

        // Nearly identical stake
        let s = 1_000_000;
        let stake = weights(&[s, s - 1]);
        assert_eq!(allocate_seats(&stake, 3), vec![2, 1]);

        // Large stake
        let stake = weights(&[1_000_000_000_000, 900_000_000_000, 100_000_000_000]);
        assert_eq!(allocate_seats(&stake, 500), vec![250, 225, 25]);
    }
}
