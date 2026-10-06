use bytemuck::{Pod, Zeroable};

use super::{ScheduleLayout, SCHEDULE_ROWS};
use crate::types::{Coin, EpochNumber, StorageUnits, TAPE};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EpochScheduleError {
    EpochInPast,
    EpochTooFar,
    EndNotAfterStart,
    MidRow,
    Overflow,
    Underflow,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct EpochUsage {
    reserved: StorageUnits,
    paid: Coin<TAPE>,
}

impl EpochUsage {
    pub fn new(reserved: StorageUnits, paid: Coin<TAPE>) -> Self {
        Self { reserved, paid }
    }

    #[inline]
    pub fn reserved(&self) -> StorageUnits {
        self.reserved
    }

    #[inline]
    pub fn paid(&self) -> Coin<TAPE> {
        self.paid
    }
}

unsafe impl Zeroable for EpochUsage {}
unsafe impl Pod for EpochUsage {}

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct EpochSchedule {
    /// The current epoch, where the first row starts.
    pub now: EpochNumber,

    /// The usage of each epoch a row covers. `ScheduleLayout` gives the
    /// epochs and the slot of every row.
    pub rows: [EpochUsage; SCHEDULE_ROWS],
}

unsafe impl Zeroable for EpochSchedule {}
unsafe impl Pod for EpochSchedule {}

impl EpochSchedule {
    pub fn new() -> Self {
        Self::new_at(EpochNumber(0))
    }

    pub fn new_at(start_epoch: EpochNumber) -> Self {
        Self {
            now: start_epoch,
            rows: [EpochUsage::zeroed(); SCHEDULE_ROWS],
        }
    }

    /// Empty the schedule and start it at an epoch, without building a copy
    /// of it on the stack.
    pub fn reset(&mut self, start_epoch: EpochNumber) {
        self.now = start_epoch;
        self.rows.fill(EpochUsage::zeroed());
    }

    #[inline]
    pub fn current_epoch(&self) -> EpochNumber {
        self.now
    }

    /// The epochs and slot of every row at the current epoch.
    pub fn layout(&self) -> Result<ScheduleLayout, EpochScheduleError> {
        ScheduleLayout::try_new(self.now)
    }

    /// Advance to the next epoch, returning the EpochUsage of the current epoch.
    pub fn advance_epoch(&mut self) -> Result<EpochUsage, EpochScheduleError> {
        let next = self.now.checked_next().ok_or(EpochScheduleError::Overflow)?;
        let old = self.layout()?;
        let new = ScheduleLayout::try_new(next)?;

        let current = core::mem::take(&mut self.rows[old.row_at(self.now)?.slot()]);

        // The parent is read before its tier takes its own two rows, which
        // can land in the slot the parent leaves.
        for split in old.splits(&new) {
            let parent = core::mem::take(&mut self.rows[split.parent.slot()]);
            for child in split.children {
                self.rows[child.slot()] = parent;
            }
        }

        // The last tier's new row lands in the slot its first row left, which
        // is zeroed above.
        self.now = next;

        Ok(current)
    }

    /// Get the per-epoch scheduled usage/fees for the provided epoch.
    #[inline]
    pub fn get(&self, epoch: EpochNumber) -> Result<EpochUsage, EpochScheduleError> {
        let row = self.layout()?.row_at(epoch)?;
        Ok(self.rows[row.slot()])
    }

    /// Reserve capacity and fees per-epoch in [start_epoch, end_epoch) (end exclusive).
    /// Adds `units` to reserved and `fee` to paid each epoch in the range.
    /// Both epochs must be row boundaries.
    pub fn reserve_capacity(
        &mut self,
        units: StorageUnits,
        fee: Coin<TAPE>,
        start_epoch: EpochNumber,
        end_epoch: EpochNumber,
    ) -> Result<(), EpochScheduleError> {
        for row in self.layout()?.rows(start_epoch, end_epoch)? {
            let entry = &mut self.rows[row.slot()];

            entry.reserved = entry.reserved
                .checked_add(units)
                .ok_or(EpochScheduleError::Overflow)?;

            entry.paid = entry.paid
                .checked_add(fee)
                .ok_or(EpochScheduleError::Overflow)?;
        }
        Ok(())
    }

    /// Cancel previously reserved capacity and fees per-epoch in [start_epoch, end_epoch).
    /// Subtracts `units` from reserved and `fee` from paid each epoch in the range.
    pub fn cancel_capacity(
        &mut self,
        units: StorageUnits,
        fee: Coin<TAPE>,
        start_epoch: EpochNumber,
        end_epoch: EpochNumber,
    ) -> Result<(), EpochScheduleError> {
        for row in self.layout()?.rows(start_epoch, end_epoch)? {
            let entry = &mut self.rows[row.slot()];

            entry.reserved = entry.reserved
                .checked_sub(units)
                .ok_or(EpochScheduleError::Underflow)?;

            entry.paid = entry.paid
                .checked_sub(fee)
                .ok_or(EpochScheduleError::Underflow)?;
        }
        Ok(())
    }

    /// Check if there is capacity for additional reserved units in [start_epoch, end_epoch).
    /// Fees are not considered for capacity.
    pub fn has_capacity_for(
        &self,
        additional_units: StorageUnits,
        max_capacity: StorageUnits,
        start_epoch: EpochNumber,
        end_epoch: EpochNumber,
    ) -> Result<bool, EpochScheduleError> {
        for row in self.layout()?.rows(start_epoch, end_epoch)? {
            let Some(new_used) = self.rows[row.slot()].reserved
                .checked_add(additional_units) else {
                return Ok(false);
            };

            if new_used > max_capacity {
                return Ok(false);
            }
        }

        Ok(true)
    }
}

impl Default for EpochSchedule {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::system::SCHEDULE_MIN_REACH;

    const RUN_EPOCHS: u64 = 45_000;
    const RESERVE_EVERY: u64 = 16;
    const COMPARE_EVERY: u64 = 64;

    fn epoch(n: u64) -> EpochNumber { EpochNumber(n) }
    fn storage(n: u64) -> StorageUnits { StorageUnits::mb(n) }
    fn tape(n: u64) -> Coin<TAPE> { TAPE(n) }

    // One entry per epoch, from epoch zero
    struct Dense {
        usage: Vec<EpochUsage>,
    }

    impl Dense {
        fn new(epochs: u64) -> Self {
            Self { usage: vec![EpochUsage::zeroed(); epochs as usize] }
        }

        fn reserve(&mut self, units: StorageUnits, fee: Coin<TAPE>, start: EpochNumber, end: EpochNumber) {
            for entry in &mut self.usage[start.as_usize()..end.as_usize()] {
                entry.reserved = entry.reserved + units;
                entry.paid = entry.paid + fee;
            }
        }

        fn rewards_from(&self, start: EpochNumber) -> u128 {
            let mut total = 0;
            for entry in &self.usage[start.as_usize()..] {
                total += entry.paid.as_u128();
            }
            total
        }
    }

    // xorshift, so a run is the same every time
    struct Sequence(u64);

    impl Sequence {
        fn next(&mut self, below: u64) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0 % below
        }
    }

    fn assert_matches(schedule: &EpochSchedule, dense: &Dense) {
        let layout = schedule.layout().expect("layout");
        let mut rewards = 0;

        for row in layout.all_rows() {
            let usage = schedule.rows[row.slot()];
            for entry in &dense.usage[row.start.as_usize()..row.end.as_usize()] {
                assert_eq!(*entry, usage);
            }
            rewards += usage.paid().as_u128() * row.width() as u128;
        }

        assert_eq!(rewards, dense.rewards_from(layout.start()));
    }

    // a long run of reservations and advances agrees with one entry per epoch
    #[test]
    fn matches_dense() {
        for start in [0, 1, 311] {
            let mut schedule = EpochSchedule::new_at(epoch(start));
            let mut dense = Dense::new(start + RUN_EPOCHS + 2 * SCHEDULE_MIN_REACH);
            let mut sequence = Sequence(0x9E37_79B9_7F4A_7C15 + start);

            for step in 0..RUN_EPOCHS {
                if step % RESERVE_EVERY == 0 {
                    let layout = schedule.layout().expect("layout");
                    let reach = (layout.end() - layout.start()).as_u64();
                    let first = layout.start().as_u64() + sequence.next(reach);
                    let second = layout.start().as_u64() + sequence.next(reach + 1);

                    // one in four starts now, as the SDK's do
                    let from = match sequence.next(4) {
                        0 => layout.start(),
                        _ => layout.boundary_at_or_before(epoch(first.min(second))),
                    };
                    let to = layout.expiry_for(epoch(first.max(second)), u64::MAX);

                    if to > from {
                        let units = storage(1 + sequence.next(50));
                        let fee = tape(sequence.next(1_000));
                        schedule.reserve_capacity(units, fee, from, to).expect("reserve");
                        dense.reserve(units, fee, from, to);
                    }
                }

                if step % COMPARE_EVERY == 0 {
                    assert_matches(&schedule, &dense);
                }

                let closing = schedule.advance_epoch().expect("advance");
                assert_eq!(closing, dense.usage[(start + step) as usize]);
            }

            assert_matches(&schedule, &dense);
        }
    }

    // a slot no row uses holds zero
    #[test]
    fn free_slots_zeroed() {
        let mut schedule = EpochSchedule::new();
        let layout = schedule.layout().expect("layout");
        schedule.reserve_capacity(storage(1), tape(1), layout.start(), layout.end()).expect("reserve");

        for _ in 0..3_000 {
            schedule.advance_epoch().expect("advance");

            let mut is_used = [false; SCHEDULE_ROWS];
            for row in schedule.layout().expect("layout").all_rows() {
                is_used[row.slot()] = true;
            }
            for (slot, usage) in schedule.rows.iter().enumerate() {
                assert!(is_used[slot] || *usage == EpochUsage::zeroed());
            }
        }
    }

    // a new schedule is empty from its first epoch to its last
    #[test]
    fn starts_empty() {
        let schedule = EpochSchedule::new_at(epoch(7));
        let layout = schedule.layout().expect("layout");

        assert_eq!(schedule.current_epoch(), epoch(7));
        assert_eq!(schedule.get(epoch(7)), Ok(EpochUsage::zeroed()));
        assert_eq!(schedule.get(layout.end().prev()), Ok(EpochUsage::zeroed()));
        assert_eq!(schedule.get(epoch(6)), Err(EpochScheduleError::EpochInPast));
        assert_eq!(schedule.get(layout.end()), Err(EpochScheduleError::EpochTooFar));
        assert_eq!(schedule.get(epoch(u64::MAX)), Err(EpochScheduleError::EpochTooFar));
    }

    // reset empties a used schedule in place
    #[test]
    fn reset() {
        let mut schedule = EpochSchedule::new();
        schedule.reserve_capacity(storage(5), tape(5), epoch(0), epoch(200)).expect("reserve");

        schedule.reset(epoch(40));

        assert_eq!(schedule, EpochSchedule::new_at(epoch(40)));
    }

    // reserving adds to every epoch of the range and cancelling takes it back
    #[test]
    fn reserve_and_cancel() {
        let mut schedule = EpochSchedule::new();
        let units = storage(100);
        let fee = tape(1000);

        schedule.reserve_capacity(units, fee, epoch(1), epoch(300)).expect("reserve");

        assert_eq!(schedule.get(epoch(0)), Ok(EpochUsage::zeroed()));
        assert_eq!(schedule.get(epoch(1)), Ok(EpochUsage::new(units, fee)));
        assert_eq!(schedule.get(epoch(299)), Ok(EpochUsage::new(units, fee)));
        assert_eq!(schedule.get(epoch(300)), Ok(EpochUsage::zeroed()));

        schedule.cancel_capacity(units, fee, epoch(1), epoch(300)).expect("cancel");

        assert_eq!(schedule, EpochSchedule::new());
    }

    // a range off the row boundaries changes nothing
    #[test]
    fn mid_row() {
        let mut schedule = EpochSchedule::new();

        // one-epoch rows end at 128, and 129 falls inside a two-epoch row
        assert_eq!(
            schedule.reserve_capacity(storage(1), tape(1), epoch(0), epoch(129)),
            Err(EpochScheduleError::MidRow),
        );
        assert_eq!(
            schedule.has_capacity_for(storage(1), storage(10), epoch(129), epoch(256)),
            Err(EpochScheduleError::MidRow),
        );
        assert_eq!(
            schedule.reserve_capacity(storage(1), tape(1), epoch(4), epoch(4)),
            Err(EpochScheduleError::EndNotAfterStart),
        );
        assert_eq!(schedule, EpochSchedule::new());
    }

    // the reserved and paid amounts stop at the largest number
    #[test]
    fn overflow() {
        let mut schedule = EpochSchedule::new();
        schedule.reserve_capacity(StorageUnits(u64::MAX), tape(0), epoch(0), epoch(1)).expect("reserve");
        assert_eq!(
            schedule.reserve_capacity(StorageUnits(1), tape(0), epoch(0), epoch(1)),
            Err(EpochScheduleError::Overflow),
        );

        let mut schedule = EpochSchedule::new();
        schedule.reserve_capacity(storage(0), tape(u64::MAX), epoch(0), epoch(1)).expect("reserve");
        assert_eq!(
            schedule.reserve_capacity(storage(0), tape(1), epoch(0), epoch(1)),
            Err(EpochScheduleError::Overflow),
        );
    }

    // cancelling more than was reserved fails
    #[test]
    fn underflow() {
        let mut schedule = EpochSchedule::new();

        assert_eq!(
            schedule.cancel_capacity(storage(1), tape(0), epoch(0), epoch(1)),
            Err(EpochScheduleError::Underflow),
        );
        assert_eq!(
            schedule.cancel_capacity(storage(0), tape(1), epoch(0), epoch(1)),
            Err(EpochScheduleError::Underflow),
        );
    }

    // capacity is checked against the fullest epoch of the range
    #[test]
    fn capacity() {
        let mut schedule = EpochSchedule::new();
        let max = storage(200);

        assert_eq!(schedule.has_capacity_for(storage(100), max, epoch(0), epoch(512)), Ok(true));

        schedule.reserve_capacity(storage(100), tape(0), epoch(0), epoch(512)).expect("reserve");
        schedule.reserve_capacity(storage(60), tape(0), epoch(256), epoch(264)).expect("reserve");

        assert_eq!(schedule.has_capacity_for(storage(100), max, epoch(0), epoch(256)), Ok(true));
        assert_eq!(schedule.has_capacity_for(storage(41), max, epoch(0), epoch(512)), Ok(false));
        assert_eq!(schedule.has_capacity_for(storage(40), max, epoch(0), epoch(512)), Ok(true));
        assert_eq!(schedule.has_capacity_for(StorageUnits(u64::MAX), max, epoch(0), epoch(1)), Ok(false));
    }

    // advancing returns the closing epoch's usage, whatever tier it was booked in
    #[test]
    fn advance() {
        let mut schedule = EpochSchedule::new();
        let units = storage(100);
        let fee = tape(100);

        schedule.reserve_capacity(units, fee, epoch(1_024), epoch(1_040)).expect("reserve");

        for now in 0..1_050 {
            let closing = schedule.advance_epoch().expect("advance");
            let expected = match now {
                1_024..1_040 => EpochUsage::new(units, fee),
                _ => EpochUsage::zeroed(),
            };
            assert_eq!(closing, expected);
        }

        assert_eq!(schedule.current_epoch(), epoch(1_050));
    }
}
