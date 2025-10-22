use bytemuck::{Pod, Zeroable};
use crate::types::*;
use super::SystemError;
use super::utils::get_offsets;

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct FutureUsage<const N: usize> {
    /// The storage usage for future epochs.
    usage: RingBuffer<StorageUnits, N>,

    /// The current epoch number for index 0 in the usage buffer.
    now: EpochNumber,
}

unsafe impl<const N: usize> Zeroable for FutureUsage<N> {}
unsafe impl<const N: usize> Pod for FutureUsage<N> {}

impl<const N: usize> FutureUsage<N> {
    pub fn new() -> Self {
        Self::new_at(EpochNumber(0))
    }

    /// Create a new FutureUsage starting at the specified epoch.
    pub fn new_at(start_epoch: EpochNumber) -> Self {
        let front = (start_epoch.as_u64() as usize) % N;
        Self {
            usage: RingBuffer::filled_zero_at(front),
            now: start_epoch,
        }
    }

    /// Fast forward to a specific epoch, useful for initializing from state.
    #[cfg(not(target_os = "solana"))]
    pub fn fast_forward_to(&mut self, target_epoch: EpochNumber) {
        while self.now < target_epoch {
            self.advance_epoch();
        }
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

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct FutureRewards<const N: usize> {
    /// The rewards to be distributed in future epochs.
    rewards: RingBuffer<Coin::<TAPE>, N>,

    /// The current epoch number for index 0 in the usage buffer.
    now: EpochNumber,
}

unsafe impl<const N: usize> Zeroable for FutureRewards<N> {}
unsafe impl<const N: usize> Pod for FutureRewards<N> {}

impl<const N: usize> FutureRewards<N> {
    pub fn new() -> Self {
        Self::new_at(EpochNumber(0))
    }

    /// Create a new FutureRewards starting at the specified epoch.
    pub fn new_at(start_epoch: EpochNumber) -> Self {
        let front = (start_epoch.as_u64() as usize) % N;
        Self {
            rewards: RingBuffer::filled_zero_at(front),
            now: start_epoch,
        }
    }

    /// Fast forward to a specific epoch, useful for initializing from state.
    #[cfg(not(target_os = "solana"))]
    pub fn fast_forward_to(&mut self, target_epoch: EpochNumber) {
        while self.now < target_epoch {
            self.advance_epoch();
        }
    }

    /// Get the current epoch number.
    pub fn current_epoch(&self) -> EpochNumber {
        self.now
    }

    /// Advance to the next epoch, returning the rewards of the current epoch.
    pub fn advance_epoch(&mut self) -> Coin::<TAPE> {
        let current_rewards = *self.rewards
            .front()
            .unwrap_or(&Coin::<TAPE>::zero());

        // Push a new zeroed entry for the new future epoch
        self.rewards.push(Coin::<TAPE>::zero());

        // Advance the epoch number
        self.now.increment();

        current_rewards
    }

    /// Get the rewards for the provided epoch.
    #[inline]
    pub fn get(&self, epoch: EpochNumber) -> Result<Coin::<TAPE>, SystemError> {
        if epoch < self.now {
            return Err(SystemError::EpochInPast);
        }

        if epoch >= EpochNumber(self.now.as_u64() + N as u64) {
            return Err(SystemError::EpochTooFar);
        }

        let index = (epoch - self.now).as_u64() as usize;
        self.rewards.get(index).copied().ok_or(SystemError::IndexOutOfBounds)
    }

    /// Add rewards in the specified epoch range.
    pub fn add_rewards(
        &mut self,
        amount: Coin::<TAPE>,
        start_epoch: EpochNumber,
        end_epoch: EpochNumber,
    ) -> Result<(), SystemError> {
        let (start_offset, end_offset) = get_offsets::<N>(self.now, start_epoch, end_epoch)?;

        for i in start_offset..end_offset {
            let entry = self.rewards
                .get_mut(i)
                .ok_or(SystemError::IndexOutOfBounds)?;

            *entry = entry
                .checked_add(amount)
                .ok_or(SystemError::Overflow)?;
        }

        Ok(())
    }

    /// Slash rewards in the specified epoch range.
    pub fn slash_rewards(
        &mut self,
        start_epoch: EpochNumber,
        end_epoch: EpochNumber,
        amount: Coin::<TAPE>,
    ) -> Result<(), SystemError> {
        let (start_offset, mut end_offset) = get_offsets::<N>(self.now, start_epoch, end_epoch)?;

        // Clamp to current length
        end_offset = end_offset.min(self.rewards.len());

        for i in start_offset..end_offset {
            let entry = self.rewards
                .get_mut(i)
                .ok_or(SystemError::IndexOutOfBounds)?;

            *entry = entry
                .checked_sub(amount)
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
    fn test_future_rewards_new() {
        let db: FutureRewards<N> = FutureRewards::new();
        assert_eq!(db.now, EpochNumber(0));
        assert_eq!(db.rewards.len(), N);

        for i in 0..N {
            assert_eq!(
                db.get(EpochNumber(i as u64)).unwrap(),
                Coin::<TAPE>::zero()
            );
        }
    }

    #[test]
    fn test_future_rewards_get_rewards_at() {
        let db: FutureRewards<N> = FutureRewards::new();

        // Valid ranges
        for i in 0..N as u64 {
            assert_eq!(db.get(EpochNumber(i)).unwrap(), Coin::<TAPE>::zero());
        }

        // Errors
        assert_eq!(
            db.get(EpochNumber(u64::MAX)),
            Err(SystemError::EpochTooFar)
        );
    }

    #[test]
    fn test_future_rewards_advance_epoch() {
        let mut db: FutureRewards<N> = FutureRewards::new();

        for _ in 0..10 {
            let current = db.advance_epoch();
            assert_eq!(current, Coin::<TAPE>::zero());
            assert_eq!(db.rewards.len(), N);
        }

        assert_eq!(db.now, EpochNumber(10));
        for i in 0..N as u64 {
            assert_eq!(
                db.get(EpochNumber(10 + i)).unwrap(),
                Coin::<TAPE>::zero()
            );
        }
    }

    #[test]
    fn test_future_rewards_add_and_slash_rewards() {
        let mut db: FutureRewards<N> = FutureRewards::new();
        let amount = TAPE::new(100);

        // Add in epochs 1 to 3
        db.add_rewards(amount, EpochNumber(1), EpochNumber(3))
            .unwrap();

        assert_eq!(db.get(EpochNumber(0)).unwrap(), Coin::<TAPE>::zero());
        assert_eq!(db.get(EpochNumber(1)).unwrap(), amount);
        assert_eq!(db.get(EpochNumber(2)).unwrap(), amount);
        assert_eq!(db.get(EpochNumber(3)).unwrap(), Coin::<TAPE>::zero());
        assert_eq!(db.get(EpochNumber(4)).unwrap(), Coin::<TAPE>::zero());

        // Slash
        db.slash_rewards(EpochNumber(1), EpochNumber(3), amount)
            .unwrap();

        for i in 0..N as u64 {
            assert_eq!(db.get(EpochNumber(i)).unwrap(), Coin::<TAPE>::zero());
        }
    }

    #[test]
    fn test_future_rewards_add_errors() {
        let mut db: FutureRewards<N> = FutureRewards::new();
        let amount = TAPE::new(100);

        // Invalid ranges
        assert_eq!(
            db.add_rewards(amount, EpochNumber(0), EpochNumber(0)),
            Err(SystemError::EndNotAfterStart)
        );
        assert_eq!(
            db.add_rewards(amount, EpochNumber(0), EpochNumber(N as u64 + 1)),
            Err(SystemError::RangeTooLarge)
        );
        assert_eq!(
            db.add_rewards(amount, EpochNumber(N as u64), EpochNumber(N as u64 + 1)),
            Err(SystemError::ExceedsFutureEpochs)
        );

        // Overflow: assume max, add 1
        let max_amount = TAPE(u64::MAX);
        db.add_rewards(max_amount, EpochNumber(0), EpochNumber(1))
            .unwrap();
        assert_eq!(
            db.add_rewards(TAPE::new(1), EpochNumber(0), EpochNumber(1)),
            Err(SystemError::Overflow)
        );
    }

    #[test]
    fn test_future_rewards_slash_errors() {
        let mut db: FutureRewards<N> = FutureRewards::new();
        let amount = TAPE::new(100);

        // Underflow
        assert_eq!(
            db.slash_rewards(EpochNumber(0), EpochNumber(1), amount),
            Err(SystemError::Underflow)
        );

        // Invalid ranges
        assert_eq!(
            db.slash_rewards(EpochNumber(0), EpochNumber(0), amount),
            Err(SystemError::EndNotAfterStart)
        );
    }

    #[test]
    fn test_future_rewards_advance_with_additions() {
        let mut db: FutureRewards<N> = FutureRewards::new();
        let amount = TAPE::new(100);

        // Add in future epochs 2-4
        db.add_rewards(amount, EpochNumber(2), EpochNumber(4))
            .unwrap();

        assert_eq!(db.get(EpochNumber(0)).unwrap(), Coin::<TAPE>::zero());
        assert_eq!(db.get(EpochNumber(1)).unwrap(), Coin::<TAPE>::zero());
        assert_eq!(db.get(EpochNumber(2)).unwrap(), amount);
        assert_eq!(db.get(EpochNumber(3)).unwrap(), amount);
        assert_eq!(db.get(EpochNumber(4)).unwrap(), Coin::<TAPE>::zero());

        // Advance once: return 0, now=1, new for 5=0
        let ret = db.advance_epoch();
        assert_eq!(ret, Coin::<TAPE>::zero());
        assert_eq!(db.now, EpochNumber(1));
        assert_eq!(db.get(EpochNumber(1)).unwrap(), Coin::<TAPE>::zero());
        assert_eq!(db.get(EpochNumber(2)).unwrap(), amount);
        assert_eq!(db.get(EpochNumber(3)).unwrap(), amount);
        assert_eq!(db.get(EpochNumber(4)).unwrap(), Coin::<TAPE>::zero());
        assert_eq!(db.get(EpochNumber(5)).unwrap(), Coin::<TAPE>::zero());

        // Advance again: return 0, now=2
        let ret = db.advance_epoch();
        assert_eq!(ret, Coin::<TAPE>::zero());
        assert_eq!(db.now, EpochNumber(2));

        // Advance again: return amount, now=3
        let ret = db.advance_epoch();
        assert_eq!(ret, amount);
        assert_eq!(db.now, EpochNumber(3));
    }


    #[test]
    fn test_future_usage_new() {
        let db: FutureUsage<N> = FutureUsage::new();
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
        let db: FutureUsage<N> = FutureUsage::new();

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
        let mut db: FutureUsage<N> = FutureUsage::new();

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
        let mut db: FutureUsage<N> = FutureUsage::new();
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
        let mut db: FutureUsage<N> = FutureUsage::new();
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
        let mut db: FutureUsage<N> = FutureUsage::new();
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
        let mut db: FutureUsage<N> = FutureUsage::new();
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
        let mut db: FutureUsage<N> = FutureUsage::new();
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
