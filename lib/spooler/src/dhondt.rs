use crate::heap::MaxHeap;
use crate::cap_spools;
use crate::priority::{NodePriority, SpoolPriority};
use tape_core::spooler::{SpoolCount, SpoolerError};
use tape_core::types::TAPE;

/// D'Hondt-method spooler.
#[derive(Default)]
pub struct DhondtSpooler;

impl DhondtSpooler {
    pub fn allocate(
        &self,
        stake_weight: &[TAPE],
        spool_count: SpoolCount,
    ) -> Result<Vec<SpoolCount>, SpoolerError> {
        dhondt_allocate(stake_weight, spool_count)
    }
}

/// Allocate spools to nodes using the D'Hondt method with tie-breaking and max spool limits.
pub fn dhondt_allocate(
    stake_weight: &[TAPE],
    spool_count: SpoolCount,
) -> Result<Vec<SpoolCount>, SpoolerError> {
    let node_count = stake_weight.len();
    if node_count == 0 {
        return Ok(Vec::new());
    }

    let stakes: Vec<u64> = stake_weight.iter().map(|s| s.as_u64()).collect();

    let total_stake: u128 = stakes.iter().map(|&x| x as u128).sum();
    if total_stake == 0 && spool_count > 0 {
        return Err(SpoolerError::Infeasible);
    }

    let n_spools_u64 = spool_count as u64;
    let max_spools = cap_spools(node_count as u64, n_spools_u64);
    let dist_number = (total_stake / (n_spools_u64 as u128 + 1)) + 1;

    let mut seats: Vec<u64> = stakes
        .iter()
        .map(|&s| {
            let base = (s as u128) / dist_number;
            (base as u64).min(max_spools)
        })
        .collect();

    let mut heap = MaxHeap::with_capacity(node_count);
    for (i, &s) in stakes.iter().enumerate() {
        if seats[i] != max_spools {
            let d = seats[i] + 1;
            heap.push(NodePriority {
                priority: SpoolPriority::new(s, d),
                tie_breaker: (node_count - i) as u64,
                index: i,
            });
        }
    }

    let mut distributed: u64 = seats.iter().sum();
    while distributed < n_spools_u64 {
        let NodePriority {
            tie_breaker, index, ..
        } = heap.pop().ok_or(SpoolerError::Infeasible)?;
        seats[index] += 1;
        distributed += 1;
        if seats[index] != max_spools {
            let d = seats[index] + 1;
            heap.push(NodePriority {
                priority: SpoolPriority::new(stakes[index], d),
                tie_breaker,
                index,
            });
        }
    }

    Ok(seats.into_iter().map(|x| x as SpoolCount).collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_even() {
        let stake = vec![TAPE(25_000), TAPE(25_000), TAPE(25_000), TAPE(25_000)];
        let s = DhondtSpooler::default();
        assert_eq!(s.allocate(&stake, 4).unwrap(), vec![1, 1, 1, 1]);

        let res = s.allocate(&stake, 1000).unwrap();
        assert_eq!(res.iter().map(|&x| x as u64).sum::<u64>(), 1000);
        assert_eq!(res, vec![250, 250, 250, 250]);
    }

    #[test]
    fn basic_uneven() {
        let stake = vec![TAPE(50_000), TAPE(30_000), TAPE(15_000), TAPE(5_000)];
        let s = DhondtSpooler::default();

        // With SPOOL_GROUP_SIZE=20, cap(4,4) = 1 -> each node gets at most 1.
        assert_eq!(s.allocate(&stake, 4).unwrap(), vec![1, 1, 1, 1]);

        // cap(4,1000) = 250 -> all nodes hit cap.
        let res = s.allocate(&stake, 1000).unwrap();
        assert_eq!(res.iter().map(|&x| x as u64).sum::<u64>(), 1000);
        assert_eq!(res, vec![250, 250, 250, 250]);
    }

    #[test]
    fn ties() {
        let s = DhondtSpooler::default();

        let stake = vec![TAPE(25_000), TAPE(25_000), TAPE(25_000), TAPE(25_000)];
        assert_eq!(s.allocate(&stake, 7).unwrap(), vec![2, 2, 2, 1]);
        assert_eq!(s.allocate(&stake, 6).unwrap(), vec![2, 2, 1, 1]);

        let stake = vec![TAPE(200), TAPE(200), TAPE(200), TAPE(100)];
        assert_eq!(s.allocate(&stake, 7).unwrap(), vec![2, 2, 2, 1]);

        // cap(5,18) = 4.
        let stake = vec![TAPE(780_000), TAPE(650_000), TAPE(520_000), TAPE(390_000), TAPE(260_000)];
        assert_eq!(s.allocate(&stake, 18).unwrap(), vec![4, 4, 4, 4, 2]);
    }

    #[test]
    fn zero_stake_infeasible() {
        let s = DhondtSpooler::default();
        let stake = vec![TAPE(0), TAPE(0), TAPE(0)];
        assert_eq!(s.allocate(&stake, 5).unwrap_err(), SpoolerError::Infeasible);
        assert_eq!(s.allocate(&stake, 0).unwrap(), vec![0, 0, 0]);
    }

    #[test]
    fn edge_cases() {
        let s = DhondtSpooler::default();

        let stake = vec![TAPE(100), TAPE(90), TAPE(80)];
        assert_eq!(s.allocate(&stake, 0).unwrap(), vec![0, 0, 0]);

        // cap(3,5) = 2.
        let stake = vec![TAPE(1), TAPE(0), TAPE(0)];
        assert_eq!(s.allocate(&stake, 5).unwrap(), vec![2, 2, 1]);

        let stake = vec![TAPE(1_000_000), TAPE(999_999)];
        assert_eq!(s.allocate(&stake, 3).unwrap(), vec![2, 1]);

        // cap(3,500) = 167.
        let stake = vec![TAPE(1_000_000_000_000), TAPE(900_000_000_000), TAPE(100_000_000_000)];
        assert_eq!(s.allocate(&stake, 500).unwrap(), vec![167, 167, 166]);
    }
}
