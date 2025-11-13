use crate::types::*;
use super::{Spooler, SpoolerError, SpoolCount};
use super::priority::{ SpoolPriority, NodePriority };
use super::cap_spools;

/// Sainte-Lague-method spooler (highest averages with divisors 1, 3, 5, ...).
#[derive(Default)]
pub struct SainteLagueSpooler;

impl Spooler for SainteLagueSpooler {
    fn allocate(
        &mut self,
        stake_weight: &[Coin<TAPE>],
        spool_count: u16,
    ) -> Result<Vec<SpoolCount>, SpoolerError> {
        Ok(sainte_lague_allocate(stake_weight, spool_count))
    }
}

/// Allocate spools using Sainte-Lague (divisors: 1, 3, 5, ...), with caps.
///
/// Implementation note:
/// - Starts with zero seats per member.
/// - Priority for member i with a seats is stake / (2a + 1).
/// - Caps are applied via `cap_spools`.
pub fn sainte_lague_allocate(
    stake_weight: &[Coin<TAPE>],
    spool_count: u16,
) -> Vec<SpoolCount> {
    let n = stake_weight.len();
    if n == 0 {
        return Vec::new();
    }

    let total_spools = spool_count as u64;
    let cap = cap_spools(n as u64, total_spools);

    // Early return if no spools
    if total_spools == 0 {
        return vec![0; n];
    }

    // Seats allocated so far.
    let mut seats = vec![0u64; n];

    // Prepare heap with initial priorities stake/1 (i.e., divisor = 1 = 2*0 + 1).
    let mut heap = MaxHeap::with_capacity(n);
    for (i, &s) in stake_weight.iter().enumerate() {
        // Skip members with zero stake: they cannot earn any seats via priority
        if s.as_u128() == 0 {
            continue;
        }
        heap.push(NodePriority {
            priority: SpoolPriority::from(s.into(), 1),
            tie_breaker: (n - i) as u64,
            index: i,
        });
    }

    let mut distributed = 0u64;
    while distributed < total_spools {
        // If heap is empty but still need seats, all stakes were zero; break safely.
        let Some(NodePriority { tie_breaker, index, .. }) = heap.pop() else {
            break;
        };

        // If member is at cap, skip re-insertion; otherwise reinsert with next divisor.
        if seats[index] < cap {
            seats[index] += 1;
            distributed += 1;

            if seats[index] < cap {
                // Next divisor is 2a+1 where a is current seats.
                let next_div = 2 * seats[index] + 1;
                heap.push(NodePriority {
                    priority: SpoolPriority::from(stake_weight[index].into(), next_div),
                    tie_breaker,
                    index,
                });
            }
        }
    }

    seats.into_iter().map(|x| x as u16).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spooler::Spooler;

    fn weights(v: &[u64]) -> Vec<TAPE> {
        v.iter().map(|&x| TAPE::new(x)).collect()
    }

    #[test]
    fn uneven_stake() {
        let mut s = SainteLagueSpooler::default();
        let stake = weights(&[50_000, 30_000, 15_000, 5_000]);

        // With 4 spools, close to proportional but different from D'Hondt sometimes.
        let res = s.allocate(&stake, 4).unwrap();
        println!("Sainte-Lague 4 spools: {:?}", res);

        // Accept the method output deterministically.
        assert_eq!(res.iter().sum::<u16>(), 4);
    }

    #[test]
    fn zero_spools_or_zero_stake() {
        let mut s = SainteLagueSpooler::default();
        let stake = weights(&[100, 90, 80]);
        assert_eq!(s.allocate(&stake, 0).unwrap(), vec![0, 0, 0]);

        let stake = weights(&[0, 0, 0]);
        assert_eq!(s.allocate(&stake, 5).unwrap(), vec![0, 0, 0]);
    }

    #[test]
    fn equal_stake_even_distribution() {
        let stake = weights(&[10_000, 10_000, 10_000, 10_000]);
        let mut s = SainteLagueSpooler::default();

        // 4 members, progressively more spools.
        assert_eq!(s.allocate(&stake, 1).unwrap(), vec![1, 0, 0, 0]);
        assert_eq!(s.allocate(&stake, 2).unwrap(), vec![1, 1, 0, 0]);
        assert_eq!(s.allocate(&stake, 3).unwrap(), vec![1, 1, 1, 0]);
        assert_eq!(s.allocate(&stake, 4).unwrap(), vec![1, 1, 1, 1]);
        assert_eq!(s.allocate(&stake, 5).unwrap(), vec![2, 1, 1, 1]);

        // Deterministic tie-break favors lower indices first.
        assert_eq!(s.allocate(&stake, 6).unwrap(), vec![2, 2, 1, 1]);
        assert_eq!(s.allocate(&stake, 7).unwrap(), vec![2, 2, 2, 1]);
        assert_eq!(s.allocate(&stake, 8).unwrap(), vec![2, 2, 2, 2]);
    }

    #[test]
    fn deterministic_tie_break_equal_stake_six() {
        let stake = weights(&[10_000, 10_000, 10_000, 10_000]);
        let mut s = SainteLagueSpooler::default();
        // With our tie-breaker (favoring lower indices), we expect [2,2,1,1].
        assert_eq!(s.allocate(&stake, 6).unwrap(), vec![2, 2, 1, 1]);
    }

    #[test]
    fn small_uneven_stake_exact_3_and_4() {
        // Stakes [3,2,1], Sainte-Lague, hand-simulated:
        // 3 spools -> [2,1,0]
        // 4 spools -> [2,1,1]
        let stake = weights(&[3, 2, 1]);
        let mut s = SainteLagueSpooler::default();

        assert_eq!(s.allocate(&stake, 3).unwrap(), vec![2, 1, 0]);
        assert_eq!(s.allocate(&stake, 4).unwrap(), vec![2, 1, 1]);
    }

    #[test]
    fn zero_stake_members_get_no_spools() {
        // Members with zero stake should never get seats.
        let stake = weights(&[0, 10_000, 0, 5_000]);
        let mut s = SainteLagueSpooler::default();

        // Use 4 spools to avoid cap binding (cap=2 with 4 nodes).
        let out = s.allocate(&stake, 4).unwrap();

        assert_eq!(out.iter().sum::<u16>(), 4);
        assert_eq!(out[0], 0);
        assert_eq!(out[2], 0);

        // Non-zero stake members receive all seats.
        assert_eq!(out[1] + out[3], 4);
    }

    #[test]
    fn cap_enforced_for_many_members() {
        // With >= MIN_MEMBER_COUNT members, cap = total_spools / 20 (integer div).
        // For 40 members and 100 spools, cap = 5.
        let n = 40usize;

        let mut stake = Vec::with_capacity(n);
        stake.push(TAPE::new(1_000_000));
        for _ in 1..n {
            stake.push(TAPE::new(1));
        }

        let mut s = SainteLagueSpooler::default();
        let out = s.allocate(&stake, 100).unwrap();

        assert_eq!(out.iter().copied().map(u16::from).map(|x| x as u32).sum::<u32>(), 100);

        // Top member cannot exceed cap.
        assert_eq!(out[0], 5);

        // No member exceeds cap.
        assert!(out.iter().all(|&x| x <= 5));
    }

    #[test]
    fn large_equal_distribution() {
        // 5 equal members, 1000 spools -> 200 each
        let stake = weights(&[1, 1, 1, 1, 1]);
        let mut s = SainteLagueSpooler::default();
        let out = s.allocate(&stake, 1000).unwrap();

        assert_eq!(out, vec![200, 200, 200, 200, 200]);
    }

    #[test]
    fn monotonicity_with_more_spools() {
        // Adding one spool never reduces any member's allocation.
        let stake = weights(&[60_000, 30_000, 10_000]);

        let mut s = SainteLagueSpooler::default();
        let out_a = s.allocate(&stake, 12).unwrap();
        let out_b = s.allocate(&stake, 13).unwrap();

        assert_eq!(out_a.iter().sum::<u16>() + 1, out_b.iter().sum::<u16>());

        for i in 0..out_a.len() {
            assert!(out_b[i] >= out_a[i]);
        }
    }
}
