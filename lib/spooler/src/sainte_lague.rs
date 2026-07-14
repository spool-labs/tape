use crate::heap::MaxHeap;
use crate::cap_spools;
use crate::priority::{NodePriority, SpoolPriority};
use tape_core::spooler::SpoolerError;
use tape_core::types::SpoolCount;
use tape_core::types::TAPE;

/// Sainte-Lague-method spooler (highest averages with divisors 1, 3, 5, ...).
#[derive(Default)]
pub struct SainteLagueSpooler;

impl SainteLagueSpooler {
    pub fn allocate(
        &self,
        stake_weight: &[TAPE],
        spool_count: SpoolCount,
    ) -> Result<Vec<SpoolCount>, SpoolerError> {
        sainte_lague_allocate(stake_weight, spool_count)
    }
}

/// Allocate spools using Sainte-Lague (divisors: 1, 3, 5, ...), with caps.
pub fn sainte_lague_allocate(
    stake_weight: &[TAPE],
    spool_count: SpoolCount,
) -> Result<Vec<SpoolCount>, SpoolerError> {
    let n = stake_weight.len();
    if n == 0 {
        return Ok(Vec::new());
    }

    let stakes: Vec<u64> = stake_weight.iter().map(|s| s.as_u64()).collect();
    let total_spools = spool_count.as_u64();
    let cap = cap_spools(n as u64, total_spools);

    if total_spools == 0 {
        return Ok(vec![SpoolCount(0); n]);
    }

    let mut seats = vec![0u64; n];

    let mut heap = MaxHeap::with_capacity(n);
    for (i, &s) in stakes.iter().enumerate() {
        if s == 0 {
            continue;
        }
        heap.push(NodePriority {
            priority: SpoolPriority::new(s, 1),
            tie_breaker: (n - i) as u64,
            index: i,
        });
    }

    let mut distributed = 0u64;
    while distributed < total_spools {
        let NodePriority {
            tie_breaker, index, ..
        } = heap.pop().ok_or(SpoolerError::Infeasible)?;

        if seats[index] < cap {
            seats[index] += 1;
            distributed += 1;

            if seats[index] < cap {
                let next_div = 2 * seats[index] + 1;
                heap.push(NodePriority {
                    priority: SpoolPriority::new(stakes[index], next_div),
                    tie_breaker,
                    index,
                });
            }
        }
    }

    Ok(seats.into_iter().map(SpoolCount).collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn counts(values: &[u64]) -> Vec<SpoolCount> {
        values.iter().copied().map(SpoolCount).collect()
    }

    #[test]
    fn uneven_stake() {
        let s = SainteLagueSpooler::default();
        let stake = vec![TAPE(50_000), TAPE(30_000), TAPE(15_000), TAPE(5_000)];

        let res = s.allocate(&stake, SpoolCount(4)).unwrap();
        assert_eq!(res.iter().map(|c| c.as_u64()).sum::<u64>(), 4);
    }

    #[test]
    fn zero_spools_or_zero_stake() {
        let s = SainteLagueSpooler::default();
        let stake = vec![TAPE(100), TAPE(90), TAPE(80)];
        assert_eq!(s.allocate(&stake, SpoolCount(0)).unwrap(), counts(&[0, 0, 0]));

        let stake = vec![TAPE(0), TAPE(0), TAPE(0)];
        assert_eq!(s.allocate(&stake, SpoolCount(5)).unwrap_err(), SpoolerError::Infeasible);
    }

    #[test]
    fn equal_stake_even_distribution() {
        let stake = vec![TAPE(10_000), TAPE(10_000), TAPE(10_000), TAPE(10_000)];
        let s = SainteLagueSpooler::default();

        assert_eq!(s.allocate(&stake, SpoolCount(1)).unwrap(), counts(&[1, 0, 0, 0]));
        assert_eq!(s.allocate(&stake, SpoolCount(2)).unwrap(), counts(&[1, 1, 0, 0]));
        assert_eq!(s.allocate(&stake, SpoolCount(3)).unwrap(), counts(&[1, 1, 1, 0]));
        assert_eq!(s.allocate(&stake, SpoolCount(4)).unwrap(), counts(&[1, 1, 1, 1]));
        assert_eq!(s.allocate(&stake, SpoolCount(5)).unwrap(), counts(&[2, 1, 1, 1]));
        assert_eq!(s.allocate(&stake, SpoolCount(6)).unwrap(), counts(&[2, 2, 1, 1]));
        assert_eq!(s.allocate(&stake, SpoolCount(7)).unwrap(), counts(&[2, 2, 2, 1]));
        assert_eq!(s.allocate(&stake, SpoolCount(8)).unwrap(), counts(&[2, 2, 2, 2]));
    }

    #[test]
    fn large_equal_distribution() {
        let stake = vec![TAPE(1), TAPE(1), TAPE(1), TAPE(1), TAPE(1)];
        let s = SainteLagueSpooler::default();
        let out = s.allocate(&stake, SpoolCount(1000)).unwrap();
        assert_eq!(out, counts(&[200, 200, 200, 200, 200]));
    }

    #[test]
    fn cap_enforced_for_many_members() {
        let n = 40usize;
        let mut stake: Vec<TAPE> = Vec::with_capacity(n);
        stake.push(TAPE(1_000_000));
        for _ in 1..n {
            stake.push(TAPE(1));
        }

        let s = SainteLagueSpooler::default();
        let out = s.allocate(&stake, SpoolCount(100)).unwrap();
        assert_eq!(out.iter().map(|c| c.as_u64()).sum::<u64>(), 100);
        assert_eq!(out[0], SpoolCount(5));
        assert!(out.iter().all(|&x| x <= SpoolCount(5)));
    }
}
