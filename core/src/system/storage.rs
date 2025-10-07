use crate::types::*;
use bytemuck::{Pod, Zeroable};
use super::SystemError;
use super::utils::get_offsets;

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct StorageAccounting<const N: usize> {
    /// The storage usage for future epochs.
    usage: RingBuffer<StorageUnits, N>,

    /// The current epoch number for index 0 in the usage buffer.
    now: EpochNumber,
}

unsafe impl<const N: usize> Zeroable for StorageAccounting<N> {}
unsafe impl<const N: usize> Pod for StorageAccounting<N> {}

impl<const N: usize> StorageAccounting<N> {
    pub fn new() -> Self {
        let now = EpochNumber(0);
        let mut usage = RingBuffer::new();

        while usage.len() < N {
            usage.push(StorageUnits::zero());
        }

        Self { usage, now }
    }

    /// Get the current epoch number.
    pub fn current_epoch(&self) -> EpochNumber {
        self.now
    }

    /// Advance to the next epoch, returning the usage of the current epoch.
    pub fn advance_epoch(&mut self) -> StorageUnits {
        let current_usage = *self.usage
            .front()
            .unwrap_or(&StorageUnits::zero());

        // Push a new zeroed entry for the new future epoch
        self.usage.push(StorageUnits::zero());

        // Advance the epoch number
        self.now.increment();

        current_usage
    }

    /// Get the allocated capacity for the provided epoch.
    #[inline]
    pub fn get(&self, epoch: EpochNumber) -> Result<StorageUnits, SystemError> {
        if epoch < self.now {
            return Err(SystemError::EpochInPast);
        }

        if epoch >= EpochNumber(self.now.as_u64() + N as u64) {
            return Err(SystemError::EpochTooFar);
        }

        let index = (epoch - self.now).as_u64() as usize;
        self.usage.get(index).copied().ok_or(SystemError::IndexOutOfBounds)
    }

    /// Check if there is capacity for the additional units in the specified epoch range.
    pub fn has_capacity_for(
        &self,
        additional_units: StorageUnits,
        max_capacity: StorageUnits,
        start_epoch: EpochNumber,
        end_epoch: EpochNumber,
    ) -> bool {

        let start = start_epoch.as_u64();
        let end = end_epoch.as_u64();

        for i in start..end {
            let epoch = EpochNumber(i);

            match self.get(epoch) {
                Ok(capacity_used) => {
                    if capacity_used
                        .checked_add(additional_units)
                        .map_or(true, |new_used| new_used > max_capacity) {
                        return false;
                    }
                }
                Err(_) => return false,
            }
        }

        true
    }

    /// Reserve capacity in the specified epoch range.
    pub fn reserve_capacity(
        &mut self,
        units: StorageUnits,
        start_epoch: EpochNumber,
        end_epoch: EpochNumber,
    ) -> Result<(), SystemError> {
        let (start_offset, end_offset) = get_offsets::<N>(self.now, start_epoch, end_epoch)?;

        for i in start_offset..end_offset {
            let entry = self.usage
                .get_mut(i)
                .ok_or(SystemError::IndexOutOfBounds)?;

            *entry = entry
                .checked_add(units)
                .ok_or(SystemError::Overflow)?;
        }

        Ok(())
    }

    /// Cancel previously reserved capacity in the specified epoch range.
    pub fn cancel_capacity(
        &mut self,
        start_epoch: EpochNumber,
        end_epoch: EpochNumber,
        units: StorageUnits,
    ) -> Result<(), SystemError> {
        let (start_offset, mut end_offset) = get_offsets::<N>(self.now, start_epoch, end_epoch)?;

        // Clamp to current length
        end_offset = end_offset.min(self.usage.len());

        for i in start_offset..end_offset {
            let entry = self.usage
                .get_mut(i)
                .ok_or(SystemError::IndexOutOfBounds)?;

            *entry = entry
                .checked_sub(units)
                .ok_or(SystemError::Underflow)?;
        }

        Ok(())
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{EpochNumber, StorageUnits};

    const N: usize = 5;

    #[test]
    fn test_future_usage_new() {
        let db: StorageAccounting<N> = StorageAccounting::new();
        assert_eq!(db.now, EpochNumber(0));
        assert_eq!(db.usage.len(), N);

        for i in 0..N {
            assert_eq!(
                db.get(EpochNumber(i as u64)).unwrap(),
                StorageUnits::zero()
            );
        }
    }

    #[test]
    fn test_future_usage_get_usage_at() {
        let db: StorageAccounting<N> = StorageAccounting::new();

        // Valid ranges
        for i in 0..N as u64 {
            assert_eq!(db.get(EpochNumber(i)).unwrap(), StorageUnits::zero());
        }

        // Errors
        assert_eq!(
            db.get(EpochNumber(u64::MAX)),
            Err(SystemError::EpochTooFar)
        );
    }

    #[test]
    fn test_future_usage_advance_epoch() {
        let mut db: StorageAccounting<N> = StorageAccounting::new();

        for _ in 0..10 {
            let current = db.advance_epoch();
            assert_eq!(current, StorageUnits::zero());
            assert_eq!(db.usage.len(), N);
        }

        // After advances, now is updated, usages still zero
        assert_eq!(db.now, EpochNumber(10));
        for i in 0..N as u64 {
            assert_eq!(
                db.get(EpochNumber(10 + i)).unwrap(),
                StorageUnits::zero()
            );
        }
    }

    #[test]
    fn test_future_usage_reserve_and_cancel_capacity() {
        let mut db: StorageAccounting<N> = StorageAccounting::new();
        let units = StorageUnits(100);

        // Reserve in epochs 1 to 3 (exclusive, so 1,2)
        db.reserve_capacity(units, EpochNumber(1), EpochNumber(3))
            .unwrap();

        assert_eq!(db.get(EpochNumber(0)).unwrap(), StorageUnits::zero());
        assert_eq!(db.get(EpochNumber(1)).unwrap(), units);
        assert_eq!(db.get(EpochNumber(2)).unwrap(), units);
        assert_eq!(db.get(EpochNumber(3)).unwrap(), StorageUnits::zero());
        assert_eq!(db.get(EpochNumber(4)).unwrap(), StorageUnits::zero());

        // Cancel
        db.cancel_capacity(EpochNumber(1), EpochNumber(3), units)
            .unwrap();

        for i in 0..N as u64 {
            assert_eq!(db.get(EpochNumber(i)).unwrap(), StorageUnits::zero());
        }
    }

    #[test]
    fn test_future_usage_reserve_errors() {
        let mut db: StorageAccounting<N> = StorageAccounting::new();
        let units = StorageUnits(100);

        // Invalid ranges
        assert_eq!(
            db.reserve_capacity(units, EpochNumber(0), EpochNumber(0)),
            Err(SystemError::EndNotAfterStart)
        );
        assert_eq!(
            db.reserve_capacity(units, EpochNumber(0), EpochNumber(N as u64 + 1)),
            Err(SystemError::RangeTooLarge)
        );
        assert_eq!(
            db.reserve_capacity(units, EpochNumber(N as u64), EpochNumber(N as u64 + 1)),
            Err(SystemError::ExceedsFutureEpochs)
        );

        // Overflow: assume max u64 -1, then add 2
        let max_units = StorageUnits(u64::MAX);
        db.reserve_capacity(max_units, EpochNumber(0), EpochNumber(1))
            .unwrap();
        assert_eq!(
            db.reserve_capacity(StorageUnits(1), EpochNumber(0), EpochNumber(1)),
            Err(SystemError::Overflow)
        );
    }

    #[test]
    fn test_future_usage_cancel_errors() {
        let mut db: StorageAccounting<N> = StorageAccounting::new();
        let units = StorageUnits(100);

        // Underflow
        assert_eq!(
            db.cancel_capacity(EpochNumber(0), EpochNumber(1), units),
            Err(SystemError::Underflow)
        );

        // Invalid ranges (same as reserve)
        assert_eq!(
            db.cancel_capacity(EpochNumber(0), EpochNumber(0), units),
            Err(SystemError::EndNotAfterStart)
        );
    }

    #[test]
    fn test_future_usage_has_capacity_for() {
        let mut db: StorageAccounting<N> = StorageAccounting::new();
        let max_cap = StorageUnits(200);
        let add_units = StorageUnits(100);

        // Initially all zero, should have capacity
        assert!(db.has_capacity_for(add_units, max_cap, EpochNumber(0), EpochNumber(N as u64)));

        db.reserve_capacity(add_units, EpochNumber(0), EpochNumber(N as u64))
            .unwrap();

        // Now used=100, add 50 <=200 ok, add 101 >200 no
        assert!(db.has_capacity_for(StorageUnits(50), max_cap, EpochNumber(0), EpochNumber(N as u64)));
        assert!(!db.has_capacity_for(StorageUnits(101), max_cap, EpochNumber(0), EpochNumber(N as u64)));

        // Out of range
        assert!(!db.has_capacity_for(add_units, max_cap, EpochNumber(0), EpochNumber(N as u64 + 1)));
        assert!(!db.has_capacity_for(add_units, max_cap, EpochNumber(u64::MAX - 1), EpochNumber(u64::MAX)));

        // Overflow case
        assert!(!db.has_capacity_for(StorageUnits(u64::MAX), max_cap, EpochNumber(0), EpochNumber(1)));
    }

    #[test]
    fn test_future_usage_advance_with_reservations() {
        let mut db: StorageAccounting<N> = StorageAccounting::new();
        let units = StorageUnits(100);

        // Reserve in future epochs 2-4
        db.reserve_capacity(units, EpochNumber(2), EpochNumber(4))
            .unwrap();

        assert_eq!(db.get(EpochNumber(0)).unwrap(), StorageUnits::zero());
        assert_eq!(db.get(EpochNumber(1)).unwrap(), StorageUnits::zero());
        assert_eq!(db.get(EpochNumber(2)).unwrap(), units);
        assert_eq!(db.get(EpochNumber(3)).unwrap(), units);
        assert_eq!(db.get(EpochNumber(4)).unwrap(), StorageUnits::zero());

        // Advance once: return 0, now=1, buffer shifts, new for 5=0
        let ret = db.advance_epoch();
        assert_eq!(ret, StorageUnits::zero());
        assert_eq!(db.now, EpochNumber(1));
        assert_eq!(db.get(EpochNumber(1)).unwrap(), StorageUnits::zero());
        assert_eq!(db.get(EpochNumber(2)).unwrap(), units);
        assert_eq!(db.get(EpochNumber(3)).unwrap(), units);
        assert_eq!(db.get(EpochNumber(4)).unwrap(), StorageUnits::zero());
        assert_eq!(db.get(EpochNumber(5)).unwrap(), StorageUnits::zero());

        // Advance again: return 0 (old 1), now=2, new for 6=0
        let ret = db.advance_epoch();
        assert_eq!(ret, StorageUnits::zero());
        assert_eq!(db.now, EpochNumber(2));
        assert_eq!(db.get(EpochNumber(2)).unwrap(), units);
        assert_eq!(db.get(EpochNumber(3)).unwrap(), units);
        assert_eq!(db.get(EpochNumber(4)).unwrap(), StorageUnits::zero());
        assert_eq!(db.get(EpochNumber(5)).unwrap(), StorageUnits::zero());
        assert_eq!(db.get(EpochNumber(6)).unwrap(), StorageUnits::zero());

        // Advance again: return units (old 2), now=3
        let ret = db.advance_epoch();
        assert_eq!(ret, units);
        assert_eq!(db.now, EpochNumber(3));
    }
}
