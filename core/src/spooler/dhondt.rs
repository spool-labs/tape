use crate::types::*;
use super::{Spooler, SpoolerError, SpoolCount};
use super::priority::{ SpoolPriority, NodePriority };
use super::cap_spools;

/// D'Hondt-method spooler.
#[derive(Default)]
pub struct DhondtSpooler;

impl Spooler for DhondtSpooler {
    fn allocate(
        &mut self,
        stake_weight: &[Coin<TAPE>],
        spool_count: u16,
    ) -> Result<Vec<SpoolCount>, SpoolerError> {
        Ok(dhondt_allocate(stake_weight, spool_count))
    }
}

/// Allocate spools to nodes using the D'Hondt method with tie-breaking and max spool limits.
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

    let mut seats: Vec<u64> = stake_weight
        .iter()
        .map(|&s| {
            let base = (s.as_u128()) / dist_number;
            (base as u64).min(max_spools)
        })
        .collect();

    let mut heap = MaxHeap::with_capacity(node_count);
    for (i, &s) in stake_weight.iter().enumerate() {
        if seats[i] != max_spools {
            let d = seats[i] + 1;
            heap.push(NodePriority {
                priority: SpoolPriority::from(s.into(), d),
                tie_breaker: (node_count - i) as u64,
                index: i,
            });
        }
    }

    let mut distributed: u64 = seats.iter().sum();
    while distributed < n_spools_u64 {
        let NodePriority { tie_breaker, index, .. } =
            heap.pop().expect("Heap empty while distributing spools");
        seats[index] += 1;
        distributed += 1;
        if seats[index] != max_spools {
            let d = seats[index] + 1;
            heap.push(NodePriority {
                priority: SpoolPriority::from(stake_weight[index].into(), d),
                tie_breaker,
                index,
            });
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
    fn basic_even() {
        let stake = weights(&[25_000, 25_000, 25_000, 25_000]);
        let mut s = DhondtSpooler::default();
        assert_eq!(s.allocate(&stake, 4).unwrap(), vec![1, 1, 1, 1]);

        let res = s.allocate(&stake, 1000).unwrap();
        assert_eq!(res.iter().map(|&x| x as u64).sum::<u64>(), 1000);
        assert_eq!(res, vec![250, 250, 250, 250]);
    }

    #[test]
    fn basic_uneven() {
        let stake = weights(&[50_000, 30_000, 15_000, 5_000]);
        let mut s = DhondtSpooler::default();

        assert_eq!(s.allocate(&stake, 4).unwrap(), vec![2, 2, 0, 0]);

        let res = s.allocate(&stake, 1000).unwrap();
        assert_eq!(res.iter().map(|&x| x as u64).sum::<u64>(), 1000);
        assert_eq!(res, vec![313, 313, 281, 93]);
    }

    #[test]
    fn ties() {
        let mut s = DhondtSpooler::default();

        let stake = weights(&[25_000, 25_000, 25_000, 25_000]);
        assert_eq!(s.allocate(&stake, 7).unwrap(), vec![2, 2, 2, 1]);
        assert_eq!(s.allocate(&stake, 6).unwrap(), vec![2, 2, 1, 1]);

        let stake = weights(&[200, 200, 200, 100]);
        assert_eq!(s.allocate(&stake, 7).unwrap(), vec![2, 2, 2, 1]);

        let stake = weights(&[780_000, 650_000, 520_000, 390_000, 260_000]);
        assert_eq!(s.allocate(&stake, 18).unwrap(), vec![5, 5, 4, 3, 1]);
    }

    #[test]
    fn edge_cases() {
        let mut s = DhondtSpooler::default();

        let stake = weights(&[100, 90, 80]);
        assert_eq!(s.allocate(&stake, 0).unwrap(), vec![0, 0, 0]);

        let stake = weights(&[1, 0, 0]);
        assert_eq!(s.allocate(&stake, 5).unwrap(), vec![3, 2, 0]);

        let s1 = 1_000_000;
        let stake = weights(&[s1, s1 - 1]);
        assert_eq!(s.allocate(&stake, 3).unwrap(), vec![2, 1]);

        let stake = weights(&[1_000_000_000_000, 900_000_000_000, 100_000_000_000]);
        assert_eq!(s.allocate(&stake, 500).unwrap(), vec![209, 209, 82]);
    }
}

