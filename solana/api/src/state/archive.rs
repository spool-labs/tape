use tape_solana::*;
use tape_core::system::{EpochSchedule, EpochScheduleError};
use tape_core::types::{BasisPoints, EpochNumber, StorageUnits};
use tape_core::types::coin::{Coin, TAPE};
use super::AccountType;

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct Archive {
    /// The total storage capacity of the archive.
    pub storage_capacity: StorageUnits,

    /// The price per unit of storage in TAPE.
    pub storage_price: Coin<TAPE>,

    /// The storage payment burn rate in basis points.
    pub burn_fee_bps: BasisPoints,

    /// The subsidy vault decay rate in basis points per epoch.
    pub subsidy_decay_bps: BasisPoints,

    /// The longest reservation the archive takes, in epochs from the current one.
    pub max_reservation_epochs: u64,

    /// The archive schedule for future epochs.
    pub schedule: EpochSchedule,

    /// Monotonic tape ID allocator. This is not decremented when tapes close.
    pub tape_count: u64,

    /// The capacity reserved in the last epoch.
    pub recent_usage: StorageUnits,

    /// The fees collected in the last epoch including carry over.
    pub rewards_pool: Coin<TAPE>,

    /// The rewards paid out so far in this epoch.
    pub rewards_paid: Coin<TAPE>,
}

impl Archive {
    /// The latest expiry a reservation made this epoch can have: the last
    /// schedule row boundary within the voted maximum.
    pub fn latest_expiry(&self) -> Result<EpochNumber, EpochScheduleError> {
        let layout = self.schedule.layout()?;
        Ok(layout.latest_expiry(self.max_reservation_epochs))
    }

    /// The expiry a reservation asking for `requested` gets: the next
    /// schedule row boundary, stopping at the latest expiry.
    pub fn expiry_for(&self, requested: EpochNumber) -> Result<EpochNumber, EpochScheduleError> {
        let layout = self.schedule.layout()?;
        Ok(layout.expiry_for(requested, self.max_reservation_epochs))
    }
}

tape_solana::state!(AccountType, Archive);

#[cfg(test)]
mod tests {
    use solana_program::entrypoint::MAX_PERMITTED_DATA_INCREASE;

    use super::*;

    const ARCHIVE_SIZE: usize = 8_280;

    // the program creates the archive in one instruction, which caps its size
    #[test]
    fn fits_creation() {
        assert_eq!(Archive::get_size(), ARCHIVE_SIZE);
        assert!(Archive::get_size() <= MAX_PERMITTED_DATA_INCREASE);
    }
}
