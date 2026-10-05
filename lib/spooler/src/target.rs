//! Byte targets: each node's stake share of stored bytes, within its bound.

use tape_core::types::{Coin, StorageUnits, TAPE};

/// Split `stored` bytes by stake, capping each node at its bound.
///
/// Bytes cut off by a bound go to the other nodes by stake, until no node is
/// over its bound. Nodes without stake get nothing.
pub fn byte_targets(
    stakes: &[Coin<TAPE>],
    bounds: &[StorageUnits],
    stored: StorageUnits,
) -> Vec<StorageUnits> {
    let mut targets = vec![StorageUnits::zero(); stakes.len()];
    let mut is_open = vec![true; stakes.len()];
    let mut remaining = stored.as_u128();

    loop {
        let open_stake: u128 = stakes
            .iter()
            .zip(&is_open)
            .filter(|(_, is_open)| **is_open)
            .map(|(stake, _)| stake.as_u128())
            .sum();
        if open_stake == 0 {
            return targets;
        }

        // Clamp every node over its bound at this level; clamping only raises the others'
        let level = remaining;
        let mut has_clamped = false;
        for (index, stake) in stakes.iter().enumerate() {
            if !is_open[index] {
                continue;
            }
            let share = stake.as_u128() * level / open_stake;
            targets[index] = StorageUnits(share as u64);
            if share > bounds[index].as_u128() {
                targets[index] = bounds[index];
                remaining -= bounds[index].as_u128();
                is_open[index] = false;
                has_clamped = true;
            }
        }

        if !has_clamped {
            return targets;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stakes(values: &[u64]) -> Vec<Coin<TAPE>> {
        values.iter().map(|value| TAPE(*value)).collect()
    }

    fn units(values: &[u64]) -> Vec<StorageUnits> {
        values.iter().map(|value| StorageUnits(*value)).collect()
    }

    #[test]
    fn proportional() {
        let targets = byte_targets(&stakes(&[3, 1]), &units(&[1_000, 1_000]), StorageUnits(400));
        assert_eq!(targets, units(&[300, 100]));
    }

    #[test]
    fn bound_spills_to_others_by_stake() {
        let targets = byte_targets(
            &stakes(&[2, 1, 1]),
            &units(&[100, 1_000, 1_000]),
            StorageUnits(400),
        );
        assert_eq!(targets, units(&[100, 150, 150]));
    }

    #[test]
    fn spill_can_push_others_over() {
        let targets = byte_targets(
            &stakes(&[4, 3, 1]),
            &units(&[100, 160, 1_000]),
            StorageUnits(400),
        );
        assert_eq!(targets, units(&[100, 160, 140]));
    }

    #[test]
    fn no_stake_gets_nothing() {
        let targets = byte_targets(&stakes(&[0, 1]), &units(&[1_000, 1_000]), StorageUnits(400));
        assert_eq!(targets, units(&[0, 400]));
    }

    #[test]
    fn all_bounded() {
        let targets = byte_targets(&stakes(&[1, 1]), &units(&[10, 20]), StorageUnits(400));
        assert_eq!(targets, units(&[10, 20]));
    }

    #[test]
    fn extreme_values_fit() {
        let targets = byte_targets(
            &stakes(&[u64::MAX, u64::MAX]),
            &units(&[u64::MAX, u64::MAX]),
            StorageUnits(u64::MAX),
        );
        assert_eq!(targets, units(&[u64::MAX / 2, u64::MAX / 2]));
    }
}
