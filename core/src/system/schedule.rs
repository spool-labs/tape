use bytemuck::{Pod, Zeroable};
use crate::types::*;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EpochScheduleError {
    EpochInPast,
    EpochTooFar,
    IndexOutOfBounds,
    StartNotAfterBase,
    EndNotAfterStart,
    RangeTooLarge,
    ExceedsFutureEpochs,
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
pub struct EpochSchedule<const N: usize> {
    /// The current epoch number for index 0 in the buffer.
    pub now: EpochNumber,

    /// The per-epoch usage as a contiguous ring buffer of size N.
    pub values: RingBuffer<EpochUsage, N>,
}

unsafe impl<const N: usize> Zeroable for EpochSchedule<N> {}
unsafe impl<const N: usize> Pod for EpochSchedule<N> {}

impl<const N: usize> EpochSchedule<N> {
    pub fn new() -> Self {
        Self::new_at(EpochNumber(0))
    }

    pub fn new_at(start_epoch: EpochNumber) -> Self {
        let front = (start_epoch.as_u64() as usize) % N;
        Self {
            values: RingBuffer::filled_zero_at(front),
            now: start_epoch,
        }
    }

    #[inline]
    pub fn current_epoch(&self) -> EpochNumber {
        self.now
    }

    /// Advance to the next epoch, returning the EpochUsage of the current epoch.
    pub fn advance_epoch(&mut self) -> EpochUsage {
        debug_assert!(self.values.len() == N);

        let current = self.values.front().copied().unwrap_or(EpochUsage::zeroed());

        // Push zeroed entry for the new future epoch
        self.values.push(EpochUsage::zeroed());

        // Advance the epoch number
        self.now.increment();

        current
    }

    /// Get the per-epoch scheduled usage/fees for the provided epoch.
    #[inline]
    pub fn get(&self, epoch: EpochNumber) -> Result<EpochUsage, EpochScheduleError> {
        debug_assert!(self.values.len() == N);

        if epoch < self.now {
            return Err(EpochScheduleError::EpochInPast);
        }
        if epoch >= EpochNumber(self.now.as_u64() + N as u64) {
            return Err(EpochScheduleError::EpochTooFar);
        }

        let index = (epoch - self.now).as_u64() as usize;
        self.values.get(index).copied().ok_or(EpochScheduleError::IndexOutOfBounds)
    }

    /// Reserve capacity and fees per-epoch in [start_epoch, end_epoch) (end exclusive).
    /// Adds `units` to reserved and `fee` to paid each epoch in the range.
    pub fn reserve_capacity(
        &mut self,
        units: StorageUnits,
        fee: Coin<TAPE>,
        start_epoch: EpochNumber,
        end_epoch: EpochNumber,
    ) -> Result<(), EpochScheduleError> {
        debug_assert!(self.values.len() == N);

        let (start_offset, end_offset) = get_offsets::<N>(self.now, start_epoch, end_epoch)?;

        for i in start_offset..end_offset {
            let entry = self.values
                .get_mut(i)
                .ok_or(EpochScheduleError::IndexOutOfBounds)?;

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
        debug_assert!(self.values.len() == N);

        let (start_offset, mut end_offset) = get_offsets::<N>(self.now, start_epoch, end_epoch)?;

        end_offset = end_offset.min(self.values.len());

        for i in start_offset..end_offset {
            let entry = self.values
                .get_mut(i)
                .ok_or(EpochScheduleError::IndexOutOfBounds)?;

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
    ) -> bool {
        debug_assert!(self.values.len() == N);

        let start = start_epoch.as_u64();
        let end = end_epoch.as_u64();

        for i in start..end {
            let epoch = EpochNumber(i);
            match self.get(epoch) {
                Ok(entry) => {
                    let Some(new_used) = entry.reserved
                        .checked_add(additional_units) else {
                        return false;
                    };

                    if new_used > max_capacity {
                        return false;
                    }

                }
                Err(_) => return false,
            }
        }

        true
    }
}

impl<const N: usize> Default for EpochSchedule<N> {
    fn default() -> Self {
        Self::new()
    }
}

/// Get the start and end offsets in the usage buffer for the specified epoch range.
pub fn get_offsets<const N: usize>(
    base_epoch: EpochNumber,
    start_epoch: EpochNumber,
    end_epoch: EpochNumber,
) -> Result<(usize, usize), EpochScheduleError> {

    if start_epoch < base_epoch {
        return Err(EpochScheduleError::StartNotAfterBase);
    }

    if end_epoch <= start_epoch {
        return Err(EpochScheduleError::EndNotAfterStart);
    }

    // Window and horizon (end is exclusive)
    let epoch_count = (end_epoch - start_epoch).as_u64();
    let horizon = (end_epoch - base_epoch).as_u64();

    // Must be addressable within the ring capacity
    if epoch_count > N as u64 {
        return Err(EpochScheduleError::RangeTooLarge);
    }
    if horizon > N as u64 {
        return Err(EpochScheduleError::ExceedsFutureEpochs);
    }

    assert!(epoch_count >= 1);
    assert!(horizon >= epoch_count);

    // [start, end), end exclusive
    let start_offset = (start_epoch - base_epoch).as_u64() as usize;
    let end_offset = start_offset + epoch_count as usize;

    Ok((start_offset, end_offset))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{EpochNumber, StorageUnits};

    const N: usize = 5;

    // Helpers
    fn epoch(n: u64) -> EpochNumber { EpochNumber(n) }
    fn storage(n: u64) -> StorageUnits { StorageUnits(n) }
    fn tape(n: u64) -> Coin<TAPE> { TAPE(n) }

    #[test]
    fn test_get_offsets_success() {
        let base = EpochNumber(0);
        let res = get_offsets::<N>(base, EpochNumber(0), EpochNumber(1));
        assert_eq!(res, Ok((0, 1)));

        let res = get_offsets::<N>(base, EpochNumber(2), EpochNumber(4));
        assert_eq!(res, Ok((2, 4)));

        // Max range
        let res = get_offsets::<N>(base, EpochNumber(0), EpochNumber(N as u64));
        assert_eq!(res, Ok((0, N)));

        // Max horizon with small range
        let res = get_offsets::<N>(base, EpochNumber(N as u64 - 1), EpochNumber(N as u64));
        assert_eq!(res, Ok((N - 1, N)));
    }

    #[test]
    fn test_get_offsets_errors() {
        let base = EpochNumber(10);

        // Start before base
        assert_eq!(
            get_offsets::<N>(base, EpochNumber(9), EpochNumber(11)),
            Err(EpochScheduleError::StartNotAfterBase)
        );

        // End not after start
        assert_eq!(
            get_offsets::<N>(base, EpochNumber(10), EpochNumber(10)),
            Err(EpochScheduleError::EndNotAfterStart)
        );
        assert_eq!(
            get_offsets::<N>(base, EpochNumber(11), EpochNumber(10)),
            Err(EpochScheduleError::EndNotAfterStart)
        );

        // Range too large
        assert_eq!(
            get_offsets::<N>(base, EpochNumber(10), EpochNumber(10 + N as u64 + 1)),
            Err(EpochScheduleError::RangeTooLarge)
        );
    }

    #[test]
    fn accounting_new() {
        let db: EpochSchedule<N> = EpochSchedule::new();
        assert_eq!(db.now, epoch(0));
        assert_eq!(db.values.len(), N);

        for i in 0..N {
            assert_eq!(
                db.get(epoch(i as u64)).unwrap(),
                EpochUsage::new(storage(0), tape(0))
            );
        }
    }

    #[test]
    fn accounting_get() {
        let db: EpochSchedule<N> = EpochSchedule::new();

        // Valid
        for i in 0..N as u64 {
            assert_eq!(
                db.get(epoch(i)).unwrap(),
                EpochUsage::new(storage(0), tape(0))
            );
        }

        // Errors
        assert_eq!(
            db.get(epoch(u64::MAX)),
            Err(EpochScheduleError::EpochTooFar)
        );
    }

    #[test]
    fn accounting_advance() {
        let mut db: EpochSchedule<N> = EpochSchedule::new();

        for _ in 0..10 {
            let current = db.advance_epoch();
            assert_eq!(current, EpochUsage::new(storage(0), tape(0)));
            assert_eq!(db.values.len(), N);
        }

        assert_eq!(db.now, epoch(10));
        for i in 0..N as u64 {
            assert_eq!(
                db.get(epoch(10 + i)).unwrap(),
                EpochUsage::new(storage(0), tape(0))
            );
        }
    }

    #[test]
    fn accounting_reserve_and_cancel() {
        let mut db: EpochSchedule<N> = EpochSchedule::new();
        let units = storage(100);
        let fee = tape(1000);

        // Reserve in epochs 1..3 (1,2)
        db.reserve_capacity(units, fee, epoch(1), epoch(3)).unwrap();

        let e0 = db.get(epoch(0)).unwrap();
        assert_eq!(e0.reserved(), storage(0));
        assert_eq!(e0.paid(), tape(0));

        let e1 = db.get(epoch(1)).unwrap();
        assert_eq!(e1.reserved(), units);
        assert_eq!(e1.paid(), fee);

        let e2 = db.get(epoch(2)).unwrap();
        assert_eq!(e2.reserved(), units);
        assert_eq!(e2.paid(), fee);

        let e3 = db.get(epoch(3)).unwrap();
        assert_eq!(e3.reserved(), storage(0));
        assert_eq!(e3.paid(), tape(0));

        // Cancel the same range
        db.cancel_capacity(units, fee, epoch(1), epoch(3)).unwrap();

        for i in 0..N as u64 {
            let e = db.get(epoch(i)).unwrap();
            assert_eq!(e.reserved(), storage(0));
            assert_eq!(e.paid(), tape(0));
        }
    }

    #[test]
    fn accounting_reserve_error_ranges() {
        let mut db: EpochSchedule<N> = EpochSchedule::new();
        let units = storage(100);
        let fee = tape(1000);

        // Invalid ranges
        assert_eq!(
            db.reserve_capacity(units, fee, epoch(0), epoch(0)),
            Err(EpochScheduleError::EndNotAfterStart)
        );
        assert_eq!(
            db.reserve_capacity(units, fee, epoch(0), epoch(N as u64 + 1)),
            Err(EpochScheduleError::RangeTooLarge)
        );
        assert_eq!(
            db.reserve_capacity(units, fee, epoch(N as u64), epoch(N as u64 + 1)),
            Err(EpochScheduleError::ExceedsFutureEpochs)
        );
    }

    #[test]
    fn accounting_reserve_overflow() {
        let mut db: EpochSchedule<N> = EpochSchedule::new();

        // Overflow via reserved
        let max_units = storage(u64::MAX);
        db.reserve_capacity(max_units, tape(0), epoch(0), epoch(1)).unwrap();
        assert_eq!(
            db.reserve_capacity(storage(1), tape(0), epoch(0), epoch(1)),
            Err(EpochScheduleError::Overflow)
        );

        // Overflow via paid
        let mut db2: EpochSchedule<N> = EpochSchedule::new();
        let max_fee = tape(u64::MAX);
        db2.reserve_capacity(storage(0), max_fee, epoch(0), epoch(1)).unwrap();
        assert_eq!(
            db2.reserve_capacity(storage(0), tape(1), epoch(0), epoch(1)),
            Err(EpochScheduleError::Overflow)
        );
    }

    #[test]
    fn accounting_cancel_underflow() {
        let mut db: EpochSchedule<N> = EpochSchedule::new();

        // Underflow via reserved
        assert_eq!(
            db.cancel_capacity(storage(1), tape(0), epoch(0), epoch(1)),
            Err(EpochScheduleError::Underflow)
        );

        // Underflow via paid
        assert_eq!(
            db.cancel_capacity(storage(0), tape(1), epoch(0), epoch(1)),
            Err(EpochScheduleError::Underflow)
        );

        // Invalid range
        assert_eq!(
            db.cancel_capacity(storage(1), tape(1), epoch(0), epoch(0)),
            Err(EpochScheduleError::EndNotAfterStart)
        );
    }

    #[test]
    fn accounting_capacity() {
        let mut db: EpochSchedule<N> = EpochSchedule::new();
        let max_cap = storage(200);
        let add_units = storage(100);

        // Initially all zero, should have capacity
        assert!(db.has_capacity_for(add_units, max_cap, epoch(0), epoch(N as u64)));

        // Reserve units without adding any fee
        db.reserve_capacity(add_units, tape(0), epoch(0), epoch(N as u64)).unwrap();

        // Now used=100, add 50 <= 200 ok; add 101 > 200 no
        assert!(db.has_capacity_for(storage(50), max_cap, epoch(0), epoch(N as u64)));
        assert!(!db.has_capacity_for(storage(101), max_cap, epoch(0), epoch(N as u64)));

        // Out of range
        assert!(!db.has_capacity_for(add_units, max_cap, epoch(0), epoch(N as u64 + 1)));
        assert!(!db.has_capacity_for(add_units, max_cap, epoch(u64::MAX - 1), epoch(u64::MAX)));

        // Overflow case
        assert!(!db.has_capacity_for(storage(u64::MAX), max_cap, epoch(0), epoch(1)));
    }

    #[test]
    fn accounting_advance_schedules() {
        let mut db: EpochSchedule<N> = EpochSchedule::new();
        let units = storage(100);
        let fee = tape(100);

        // Schedule both usage and fee on epochs 2..4
        db.reserve_capacity(units, fee, epoch(2), epoch(4)).unwrap();

        assert_eq!(db.get(epoch(0)).unwrap(), EpochUsage::new(storage(0), tape(0)));
        assert_eq!(db.get(epoch(1)).unwrap(), EpochUsage::new(storage(0), tape(0)));
        assert_eq!(db.get(epoch(2)).unwrap(), EpochUsage::new(units, fee));
        assert_eq!(db.get(epoch(3)).unwrap(), EpochUsage::new(units, fee));
        assert_eq!(db.get(epoch(4)).unwrap(), EpochUsage::new(storage(0), tape(0)));

        // Advance once: return zeros, now=1, new for 5 is zero
        let ret = db.advance_epoch();
        assert_eq!(ret, EpochUsage::new(storage(0), tape(0)));
        assert_eq!(db.now, epoch(1));
        assert_eq!(db.get(epoch(5)).unwrap(), EpochUsage::new(storage(0), tape(0)));

        // Advance again: return zeros, now=2
        let ret = db.advance_epoch();
        assert_eq!(ret, EpochUsage::new(storage(0), tape(0)));
        assert_eq!(db.now, epoch(2));

        // Advance again: return scheduled (old epoch 2), now=3
        let ret = db.advance_epoch();
        assert_eq!(ret, EpochUsage::new(units, fee));
        assert_eq!(db.now, epoch(3));
    }
}
