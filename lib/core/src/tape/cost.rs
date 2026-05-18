use crate::types::coin::{Coin, TAPE};
use crate::types::{EpochNumber, StorageUnits};

/// Compute the token cost for reserving `capacity` for `epochs`.
pub fn tape_reservation_cost(
    price_per_unit: Coin<TAPE>,
    capacity: StorageUnits,
    epochs: u64,
) -> Option<Coin<TAPE>> {
    price_per_unit
        .checked_mul(TAPE::new(capacity.to_mb()))?
        .checked_mul(TAPE::new(epochs))
}

/// Compute how many epochs remain on a tape from the later of `current` and `active`.
pub fn remaining_tape_epochs(
    current: EpochNumber,
    active: EpochNumber,
    expiry: EpochNumber,
) -> Option<u64> {
    let activation = current.max(active);
    if activation >= expiry {
        None
    } else {
        Some(expiry.as_u64() - activation.as_u64())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reservation_cost_rounds_capacity_to_mb() {
        let price = TAPE::new(100);
        let cost = tape_reservation_cost(
            price,
            StorageUnits::from_bytes(StorageUnits::MB + 1),
            3,
        )
        .unwrap();

        assert_eq!(cost.as_u64(), 600);
    }

    #[test]
    fn reservation_cost_overflow_returns_none() {
        let price = TAPE::new(u64::MAX);
        assert!(tape_reservation_cost(price, StorageUnits::mb(2), 2).is_none());
    }

    #[test]
    fn remaining_epochs_uses_later_activation() {
        let remaining = remaining_tape_epochs(EpochNumber(5), EpochNumber(3), EpochNumber(10));
        assert_eq!(remaining, Some(5));
    }

    #[test]
    fn remaining_epochs_before_activation_uses_tape_start() {
        let remaining = remaining_tape_epochs(EpochNumber(2), EpochNumber(4), EpochNumber(10));
        assert_eq!(remaining, Some(6));
    }

    #[test]
    fn remaining_epochs_returns_none_for_expired_tape() {
        let remaining = remaining_tape_epochs(EpochNumber(10), EpochNumber(4), EpochNumber(10));
        assert_eq!(remaining, None);
    }
}
