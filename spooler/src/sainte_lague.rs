use crate::heap::MaxHeap;
use crate::{cap_spools, Stake};
use crate::priority::{NodePriority, SpoolPriority};
use tape_core::spooler::{SpoolCount, SpoolerError};

/// Sainte-Lague-method spooler (highest averages with divisors 1, 3, 5, ...).
#[derive(Default)]
pub struct SainteLagueSpooler;

impl SainteLagueSpooler {
    pub fn allocate(
        &mut self,
        stake_weight: &[Stake],
        spool_count: SpoolCount,
    ) -> Result<Vec<SpoolCount>, SpoolerError> {
        sainte_lague_allocate(stake_weight, spool_count)
    }
}

/// Allocate spools using Sainte-Lague (divisors: 1, 3, 5, ...), with caps.
pub fn sainte_lague_allocate(
    stake_weight: &[Stake],
    spool_count: SpoolCount,
) -> Result<Vec<SpoolCount>, SpoolerError> {
    let n = stake_weight.len();
    if n == 0 {
        return Ok(Vec::new());
    }

    let total_spools = spool_count as u64;
    let cap = cap_spools(n as u64, total_spools);

    if total_spools == 0 {
        return Ok(vec![0; n]);
    }

    let mut seats = vec![0u64; n];

    let mut heap = MaxHeap::with_capacity(n);
    for (i, &s) in stake_weight.iter().enumerate() {
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
                    priority: SpoolPriority::new(stake_weight[index], next_div),
                    tie_breaker,
                    index,
                });
            }
        }
    }

    Ok(seats.into_iter().map(|x| x as SpoolCount).collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uneven_stake() {
        let mut s = SainteLagueSpooler::default();
        let stake: Vec<Stake> = vec![50_000, 30_000, 15_000, 5_000];

        let res = s.allocate(&stake, 4).unwrap();
        assert_eq!(res.iter().sum::<SpoolCount>(), 4);
    }

    #[test]
    fn zero_spools_or_zero_stake() {
        let mut s = SainteLagueSpooler::default();
        let stake: Vec<Stake> = vec![100, 90, 80];
        assert_eq!(s.allocate(&stake, 0).unwrap(), vec![0, 0, 0]);

        let stake: Vec<Stake> = vec![0, 0, 0];
        assert_eq!(s.allocate(&stake, 5).unwrap_err(), SpoolerError::Infeasible);
    }

    #[test]
    fn equal_stake_even_distribution() {
        let stake: Vec<Stake> = vec![10_000, 10_000, 10_000, 10_000];
        let mut s = SainteLagueSpooler::default();

        assert_eq!(s.allocate(&stake, 1).unwrap(), vec![1, 0, 0, 0]);
        assert_eq!(s.allocate(&stake, 2).unwrap(), vec![1, 1, 0, 0]);
        assert_eq!(s.allocate(&stake, 3).unwrap(), vec![1, 1, 1, 0]);
        assert_eq!(s.allocate(&stake, 4).unwrap(), vec![1, 1, 1, 1]);
        assert_eq!(s.allocate(&stake, 5).unwrap(), vec![2, 1, 1, 1]);
        assert_eq!(s.allocate(&stake, 6).unwrap(), vec![2, 2, 1, 1]);
        assert_eq!(s.allocate(&stake, 7).unwrap(), vec![2, 2, 2, 1]);
        assert_eq!(s.allocate(&stake, 8).unwrap(), vec![2, 2, 2, 2]);
    }

    #[test]
    fn large_equal_distribution() {
        let stake: Vec<Stake> = vec![1, 1, 1, 1, 1];
        let mut s = SainteLagueSpooler::default();
        let out = s.allocate(&stake, 1000).unwrap();
        assert_eq!(out, vec![200, 200, 200, 200, 200]);
    }

    #[test]
    fn cap_enforced_for_many_members() {
        let n = 40usize;
        let mut stake: Vec<Stake> = Vec::with_capacity(n);
        stake.push(1_000_000);
        for _ in 1..n {
            stake.push(1);
        }

        let mut s = SainteLagueSpooler::default();
        let out = s.allocate(&stake, 100).unwrap();
        assert_eq!(
            out.iter().copied().map(SpoolCount::from).map(|x| x as u32).sum::<u32>(),
            100
        );
        assert_eq!(out[0], 5);
        assert!(out.iter().all(|&x| x <= 5));
    }
}
