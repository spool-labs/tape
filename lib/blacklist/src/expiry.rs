//! When a followed tape is close enough to expiry to warn about

use tape_core::types::EpochNumber;

/// Epochs before a followed tape expires at which consumers start warning
pub const EXPIRY_WARNING_EPOCHS: EpochNumber = EpochNumber(2);

/// Whether a tape ending at `end_epoch` is inside the warning window at `epoch`
pub fn is_near_expiry(end_epoch: EpochNumber, epoch: EpochNumber) -> bool {
    end_epoch.0 <= epoch.0.saturating_add(EXPIRY_WARNING_EPOCHS.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    // a tape warns from the window's first epoch on, and not before
    #[test]
    fn near_expiry() {
        assert!(!is_near_expiry(EpochNumber(10), EpochNumber(7)));
        assert!(is_near_expiry(EpochNumber(10), EpochNumber(8)));
        assert!(is_near_expiry(EpochNumber(10), EpochNumber(11)));
    }
}
