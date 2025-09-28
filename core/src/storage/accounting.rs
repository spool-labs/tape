use crate::{
    types::*,
    ring::*,
    coin::*,
};
use bytemuck::{Pod, Zeroable};

const FUTURE_EPOCHS: usize = 256;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AccountingError {
    EpochInPast,
    EpochTooFar,
    StartNotAfterBase,
    EndNotAfterStart,
    RangeTooLarge,
    ExceedsFutureEpochs,
    IndexOutOfBounds,
    StorageOverflow,
    StorageUnderflow,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct FutureUsage {
    /// The storage usage for future epochs.
    usage: RingBuffer<StorageUnits, FUTURE_EPOCHS>,

    /// The current epoch number for index 0 in the usage buffer.
    base: EpochNumber,
}

impl FutureUsage {
    pub fn new() -> Self {
        Self {
            usage: RingBuffer::new(),
            base: EpochNumber(0),
        }
    }

    /// Get the current epoch number that index 0 in the usage buffer corresponds to.
    #[inline]
    pub fn get_base_epoch(&self) -> EpochNumber {
        self.base
    }

    /// Get the allocated capacity for the provided epoch.
    #[inline]
    pub fn get_usage_at(&self, epoch: EpochNumber) -> Result<StorageUnits, AccountingError> {
        let base_epoch = self.get_base_epoch();
        if epoch < base_epoch {
            return Err(AccountingError::EpochInPast);
        }

        if epoch >= EpochNumber(base_epoch.as_u64() + FUTURE_EPOCHS as u64) {
            return Err(AccountingError::EpochTooFar);
        }

        let index = (epoch - base_epoch).as_u64() as usize;
        self.usage.get(index).copied().ok_or(AccountingError::IndexOutOfBounds)
    }

    /// Iterate over the storage usage in the specified epoch range.
    pub fn iter_epochs(
        &self,
        start_epoch: EpochNumber,
        end_epoch: EpochNumber,
    ) -> Result<impl Iterator<Item = StorageUnits> + '_, AccountingError> {
        let base_epoch = self.get_base_epoch();
        let (start_offset, end_offset) = get_offsets(base_epoch, start_epoch, end_epoch)?;

        Ok((start_offset..end_offset).map(move |i| {
            if i < self.usage.len() {
                *self.usage.get(i).unwrap_or(&StorageUnits::zero())
            } else {
                StorageUnits::zero()
            }
        }))
    }

    pub fn has_capacity_for(
        &self,
        additional_units: StorageUnits,
        max_capacity: StorageUnits,
        start_epoch: EpochNumber,
        end_epoch: EpochNumber,
    ) -> bool {
        match self.check_capacity_for(
            additional_units,
            max_capacity,
            start_epoch,
            end_epoch,
        ) {
            Ok(result) => result,
            Err(_) => false,
        }
    }

    /// Check if there is capacity for the additional units in the specified epoch range.
    pub fn check_capacity_for(
        &self,
        additional_units: StorageUnits,
        max_capacity: StorageUnits,
        start_epoch: EpochNumber,
        end_epoch: EpochNumber,
    ) -> Result<bool, AccountingError> {
        self.iter_epochs(start_epoch, end_epoch)?
            .try_fold(true, |acc, used| {
                let new_usage = used.checked_add(additional_units)
                    .ok_or(AccountingError::StorageOverflow)?;
                Ok(acc && (new_usage <= max_capacity))
            })
    }

    /// Reserve capacity in the specified epoch range.
    pub fn reserve_capacity(
        &mut self,
        units: StorageUnits,
        start_epoch: EpochNumber,
        end_epoch: EpochNumber,
    ) -> Result<(), AccountingError> {
        let base_epoch = self.get_base_epoch();
        let (start_offset, end_offset) = get_offsets(base_epoch, start_epoch, end_epoch)?;

        // Extend the usage buffer if necessary
        self.extend(end_offset)?;

        for i in start_offset..end_offset {
            let entry = self.usage.get_mut(i).ok_or(AccountingError::IndexOutOfBounds)?;
            *entry = entry.checked_add(units).ok_or(AccountingError::StorageOverflow)?;
        }

        Ok(())
    }

    /// Cancel previously reserved capacity in the specified epoch range.
    pub fn cancel_capacity(
        &mut self,
        start_epoch: EpochNumber,
        end_epoch: EpochNumber,
        units: StorageUnits,
    ) -> Result<(), AccountingError> {
        let base_epoch = self.get_base_epoch();
        let (start_offset, mut end_offset) = get_offsets(base_epoch, start_epoch, end_epoch)?;

        // Clamp to current length
        end_offset = end_offset.min(self.usage.len());

        for i in start_offset..end_offset {
            let entry = self.usage.get_mut(i).ok_or(AccountingError::IndexOutOfBounds)?;
            *entry = entry.checked_sub(units).ok_or(AccountingError::StorageUnderflow)?;
        }

        Ok(())
    }

    /// Extend the usage buffer to the target length, filling new entries with zero.
    /// The target length must not exceed FUTURE_EPOCHS.
    pub fn extend(
        &mut self,
        target_len: usize,
    ) -> Result<(), AccountingError> {
        if target_len > FUTURE_EPOCHS {
            return Err(AccountingError::RangeTooLarge);
        }

        while self.usage.len() < target_len {
            self.usage.push(StorageUnits::zero());
        }

        Ok(())
    }
}

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct FutureRewards {
    /// The rewards to be distributed in future epochs.
    rewards: RingBuffer<Coin::<TAPE>, FUTURE_EPOCHS>,

    /// The current epoch number for index 0 in the usage buffer.
    base: EpochNumber,
}

impl FutureRewards {
    pub fn new() -> Self {
        Self {
            rewards: RingBuffer::new(),
            base: EpochNumber(0),
        }
    }

    /// Get the current epoch number that index 0 in the rewards buffer corresponds to.
    #[inline]
    pub fn get_base_epoch(&self) -> EpochNumber {
        self.base
    }

    /// Get the rewards for the provided epoch.
    #[inline]
    pub fn get_rewards_at(&self, epoch: EpochNumber) -> Result<Coin::<TAPE>, AccountingError> {
        let base_epoch = self.get_base_epoch();
        if epoch < base_epoch {
            return Err(AccountingError::EpochInPast);
        }

        if epoch >= EpochNumber(base_epoch.as_u64() + FUTURE_EPOCHS as u64) {
            return Err(AccountingError::EpochTooFar);
        }

        let index = (epoch - base_epoch).as_u64() as usize;
        self.rewards.get(index).copied().ok_or(AccountingError::IndexOutOfBounds)
    }

    /// Iterate over the rewards in the specified epoch range.
    pub fn iter_epochs(
        &self,
        start_epoch: EpochNumber,
        end_epoch: EpochNumber,
    ) -> Result<impl Iterator<Item = Coin::<TAPE>> + '_, AccountingError> {
        let base_epoch = self.get_base_epoch();
        let (start_offset, end_offset) = get_offsets(base_epoch, start_epoch, end_epoch)?;

        Ok((start_offset..end_offset).map(move |i| {
            if i < self.rewards.len() {
                *self.rewards.get(i).unwrap_or(&Coin::<TAPE>::zero())
            } else {
                Coin::<TAPE>::zero()
            }
        }))
    }

    /// Add rewards in the specified epoch range.
    pub fn add_rewards(
        &mut self,
        amount: Coin::<TAPE>,
        start_epoch: EpochNumber,
        end_epoch: EpochNumber,
    ) -> Result<(), AccountingError> {
        let base_epoch = self.get_base_epoch();
        let (start_offset, end_offset) = get_offsets(base_epoch, start_epoch, end_epoch)?;

        // Extend the rewards buffer if necessary
        self.extend(end_offset)?;

        for i in start_offset..end_offset {
            let entry = self.rewards.get_mut(i).ok_or(AccountingError::IndexOutOfBounds)?;
            *entry = entry.checked_add(amount).ok_or(AccountingError::StorageOverflow)?;
        }

        Ok(())
    }

    /// Slash rewards in the specified epoch range.
    pub fn slash_rewards(
        &mut self,
        start_epoch: EpochNumber,
        end_epoch: EpochNumber,
        amount: Coin::<TAPE>,
    ) -> Result<(), AccountingError> {
        let base_epoch = self.get_base_epoch();
        let (start_offset, mut end_offset) = get_offsets(base_epoch, start_epoch, end_epoch)?;

        // Clamp to current length
        end_offset = end_offset.min(self.rewards.len());

        for i in start_offset..end_offset {
            let entry = self.rewards.get_mut(i).ok_or(AccountingError::IndexOutOfBounds)?;
            *entry = entry.checked_sub(amount).ok_or(AccountingError::StorageUnderflow)?;
        }

        Ok(())
    }

    /// Extend the rewards buffer to the target length, filling new entries with zero.
    /// The target length must not exceed FUTURE_EPOCHS.
    pub fn extend(
        &mut self,
        target_len: usize,
    ) -> Result<(), AccountingError> {
        if target_len > FUTURE_EPOCHS {
            return Err(AccountingError::RangeTooLarge);
        }

        while self.rewards.len() < target_len {
            self.rewards.push(Coin::<TAPE>::zero());
        }

        Ok(())
    }
}

/// Get the start and end offsets in the usage buffer for the specified epoch range.
fn get_offsets(
    base_epoch: EpochNumber,
    start_epoch: EpochNumber,
    end_epoch: EpochNumber,
) -> Result<(usize, usize), AccountingError> {
    if start_epoch < base_epoch {
        return Err(AccountingError::StartNotAfterBase);
    }
    if end_epoch <= start_epoch {
        return Err(AccountingError::EndNotAfterStart);
    }

    // Window and horizon (end is exclusive)
    let epoch_count = (end_epoch - start_epoch).as_u64();
    let horizon = (end_epoch - base_epoch).as_u64();

    // Must be addressable within the ring capacity
    if epoch_count > FUTURE_EPOCHS as u64 {
        return Err(AccountingError::RangeTooLarge);
    }
    if horizon > FUTURE_EPOCHS as u64 {
        return Err(AccountingError::ExceedsFutureEpochs);
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

    // Helpers
    fn make_usage(base: u64, storage_used: &[u64]) -> FutureUsage {
        let mut rb = RingBuffer::<StorageUnits, FUTURE_EPOCHS>::new();
        for &u in storage_used {
            rb.push(StorageUnits::new(u));
        }
        FutureUsage {
            usage: rb,
            base: EpochNumber(base),
        }
    }

    fn make_rewards(base: u64, rewards: &[u64]) -> FutureRewards {
        let mut rb = RingBuffer::<Coin<TAPE>, FUTURE_EPOCHS>::new();
        for &f in rewards {
            rb.push(Coin::<TAPE>::new(f));
        }
        FutureRewards {
            rewards: rb,
            base: EpochNumber(base),
        }
    }

    fn usage_vec(u: &FutureUsage) -> Vec<u64> {
        u.usage.iter().map(|x| x.0).collect()
    }

    fn rewards_vec(r: &FutureRewards) -> Vec<u64> {
        r.rewards.iter().map(|x| x.0).collect()
    }

    fn test_has_capacity(
        additional_units: u64,
        base_epoch: u64,
        start_epoch: u64,
        end_epoch: u64,
        max_capacity: u64,
        storage_used: &[u64],
        expected: Result<bool, AccountingError>,
    ) {
        let usage = make_usage(base_epoch, storage_used);
        let res = usage.check_capacity_for(
            StorageUnits(additional_units),
            StorageUnits(max_capacity),
            EpochNumber(start_epoch),
            EpochNumber(end_epoch),
        );
        assert_eq!(res, expected);
    }

    // Capacity checks
    #[test]
    fn capacity_mixed() {
        let base = 100;
        let start = 102;
        let end = 105;
        let req = 300;
        let cap = 1000;

        // No existing usage
        test_has_capacity(req, base, start, end, cap, &[], Ok(true));

        // Basic fits
        test_has_capacity(req, base, start, end, cap, &[100, 200], Ok(true));

        // Many epochs, all fit
        test_has_capacity(req, base, start, end, cap, &[200; 10], Ok(true));

        // Some epochs exceed
        test_has_capacity(req, base, start, end, cap, &[701; 10], Ok(false));

        // Exactly at cap
        test_has_capacity(req, base, start, end, cap, &[700; 10], Ok(true));

        // One epoch, fits exactly
        test_has_capacity(req, base, start, start + 1, cap, &[700], Ok(true));
    }

    // Reservation + rewards
    #[test]
    fn reserve_rewards() {
        let base = 100;
        let start = 102;
        let end = 105;
        let req = 300;
        let fee = 50;

        // No existing usage, extends and fits
        let mut usage = make_usage(base, &[]);
        let mut rewards = make_rewards(base, &[]);

        assert_eq!(
            usage.reserve_capacity(StorageUnits(req), EpochNumber(start), EpochNumber(end)),
            Ok(())
        );
        assert_eq!(
            rewards.add_rewards(TAPE(fee), EpochNumber(start), EpochNumber(end)),
            Ok(())
        );
        assert_eq!(usage_vec(&usage), vec![0, 0, 300, 300, 300]);
        assert_eq!(rewards_vec(&rewards), vec![0, 0, 50, 50, 50]);

        // Existing usage, extends and fits
        let mut usage = make_usage(base, &[100, 200]);
        let mut rewards = make_rewards(base, &[10, 20]);
        assert_eq!(
            usage.reserve_capacity(StorageUnits(req), EpochNumber(start), EpochNumber(end)),
            Ok(())
        );
        assert_eq!(
            rewards.add_rewards(TAPE(fee), EpochNumber(start), EpochNumber(end)),
            Ok(())
        );
        assert_eq!(usage_vec(&usage), vec![100, 200, 300, 300, 300]);
        assert_eq!(rewards_vec(&rewards), vec![10, 20, 50, 50, 50]);

        // Existing usage, overlap and fits
        let mut usage = make_usage(base, &[100, 200, 700, 100]);
        let mut rewards = make_rewards(base, &[10, 20, 70, 10]);
        assert_eq!(
            usage.reserve_capacity(StorageUnits(req), EpochNumber(start), EpochNumber(end)),
            Ok(())
        );
        assert_eq!(
            rewards.add_rewards(TAPE(fee), EpochNumber(start), EpochNumber(end)),
            Ok(())
        );
        assert_eq!(usage_vec(&usage), vec![100, 200, 1000, 400, 300]);
        assert_eq!(rewards_vec(&rewards), vec![10, 20, 120, 60, 50]);
    }

    // Strict ordering / boundary errors
    #[test]
    fn ordering_errors() {
        let base = 100;
        let usage = make_usage(base, &[]);

        // end == start -> EndNotAfterStart
        assert_eq!(
            usage.check_capacity_for(
                StorageUnits(1),
                StorageUnits(1000),
                EpochNumber(105),
                EpochNumber(105)
            ),
            Err(AccountingError::EndNotAfterStart)
        );

        // end < start -> EndNotAfterStart
        assert_eq!(
            usage.check_capacity_for(
                StorageUnits(1),
                StorageUnits(1000),
                EpochNumber(106),
                EpochNumber(105)
            ),
            Err(AccountingError::EndNotAfterStart)
        );

        // start < base -> StartNotAfterBase (matches get_offsets)
        assert_eq!(
            usage.check_capacity_for(
                StorageUnits(1),
                StorageUnits(1000),
                EpochNumber(base - 1),
                EpochNumber(base + 1)
            ),
            Err(AccountingError::StartNotAfterBase)
        );
    }

    #[test]
    fn range_limits() {
        let base = 100;

        // Horizon exceeds FUTURE_EPOCHS => ExceedsFutureEpochs
        let usage = make_usage(base, &[]);
        assert_eq!(
            usage.check_capacity_for(
                StorageUnits(1),
                StorageUnits(1000),
                EpochNumber(base + 1),
                EpochNumber(base + FUTURE_EPOCHS as u64 + 1),
            ),
            Err(AccountingError::ExceedsFutureEpochs)
        );

        // Range too large (window > FUTURE_EPOCHS) => RangeTooLarge
        let usage = make_usage(base, &[]);
        let start = base + 1;
        let end = start + FUTURE_EPOCHS as u64 + 1;
        assert_eq!(
            usage.check_capacity_for(
                StorageUnits(1),
                StorageUnits(1000),
                EpochNumber(start),
                EpochNumber(end),
            ),
            Err(AccountingError::RangeTooLarge)
        );
    }

    #[test]
    fn single_epoch() {
        let base = 100;
        let start = 103;
        let end = 104;

        let mut usage = make_usage(base, &[]);
        assert_eq!(
            usage.reserve_capacity(StorageUnits(250), EpochNumber(start), EpochNumber(end)),
            Ok(())
        );
        assert_eq!(usage_vec(&usage), vec![0, 0, 0, 250]);
    }

    #[test]
    fn zero_fill() {
        // start_offset=3, epoch_count=3, horizon=end - base = 6
        let base = 200;
        let start = 203;
        let end = 206;
        let req = 100;
        let fee = 2;

        let mut usage = make_usage(base, &[5, 10]); // len = 2
        let mut rewards = make_rewards(base, &[1, 1]);

        assert_eq!(
            usage.reserve_capacity(StorageUnits(req), EpochNumber(start), EpochNumber(end)),
            Ok(())
        );
        assert_eq!(
            rewards.add_rewards(TAPE(fee), EpochNumber(start), EpochNumber(end)),
            Ok(())
        );

        assert_eq!(usage_vec(&usage), vec![5, 10, 0, 100, 100, 100]);
        assert_eq!(rewards_vec(&rewards), vec![1, 1, 0, 2, 2, 2]);
    }

    #[test]
    fn capacity_edges() {
        let base = 100;
        let start = 102;
        let end = 106;
        let cap = 1_000;

        // One epoch would exceed cap (800 + 300 > 1000), others fit.
        test_has_capacity(300, base, start, end, cap, &[400, 500, 800, 100, 100], Ok(false));

        // Exactly at cap across the window
        test_has_capacity(300, base, start, end, cap, &[700; 10], Ok(true));
    }

    #[test]
    fn capacity_overflow() {
        // Prepare a slot with u64::MAX usage; adding 1 overflows -> StorageOverflow.
        let cap = u64::MAX; // irrelevant; overflow happens first
        let base = 100;
        let start = 102;
        let end = 103; // one epoch window at index 2

        let usage = make_usage(base, &[0, 0, u64::MAX]);
        assert_eq!(
            usage.check_capacity_for(
                StorageUnits(1),
                StorageUnits(cap),
                EpochNumber(start),
                EpochNumber(end)
            ),
            Err(AccountingError::StorageOverflow)
        );
    }

    #[test]
    fn reserve_overflow() {
        // Expect error and buffer extended but unchanged at slot.
        let base = 100;
        let start = 102;
        let end = 103; // index 2

        let mut usage = make_usage(base, &[1, 2, u64::MAX]); // len already covers the slot
        let res = usage.reserve_capacity(StorageUnits(1), EpochNumber(start), EpochNumber(end));

        assert_eq!(res, Err(AccountingError::StorageOverflow));
        // Horizon is end - base = 3 -> len must be >= 3 (it already is); content unchanged.
        assert_eq!(usage_vec(&usage), vec![1, 2, u64::MAX]);
    }

    #[test]
    fn overlap_accumulates() {
        let base = 1_000;

        let mut usage = make_usage(base, &[]);
        let mut rewards = make_rewards(base, &[]);
        let fee = TAPE(7);

        // Reserve [base+2, base+5) → indices 2,3,4
        assert_eq!(
            usage.reserve_capacity(StorageUnits(100), EpochNumber(base + 2), EpochNumber(base + 5)),
            Ok(())
        );
        assert_eq!(
            rewards.add_rewards(fee, EpochNumber(base + 2), EpochNumber(base + 5)),
            Ok(())
        );

        // Reserve overlapping [base+4, base+7) → indices 4,5,6
        assert_eq!(
            usage.reserve_capacity(StorageUnits(50), EpochNumber(base + 4), EpochNumber(base + 7)),
            Ok(())
        );
        assert_eq!(
            rewards.add_rewards(fee, EpochNumber(base + 4), EpochNumber(base + 7)),
            Ok(())
        );

        assert_eq!(usage_vec(&usage), vec![0, 0, 100, 100, 150, 50, 50]);
        assert_eq!(rewards_vec(&rewards), vec![0, 0, 7, 7, 14, 7, 7]);
    }

    #[test]
    fn extend_no_rotate() {
        // Prove we can extend len to FUTURE_EPOCHS without overwriting earliest entries.
        let base = 10_000;
        let req = StorageUnits(1);
        let fee = TAPE(1);
        let end_full = base + FUTURE_EPOCHS as u64; // horizon == N

        let mut usage = make_usage(base, &[]);
        let mut rewards = make_rewards(base, &[]);

        // Pre-fill first 3 reserved epochs: [base+1, base+4) -> indices 1,2,3
        assert_eq!(
            usage.reserve_capacity(req, EpochNumber(base + 1), EpochNumber(base + 4)),
            Ok(())
        );
        assert_eq!(
            rewards.add_rewards(fee, EpochNumber(base + 1), EpochNumber(base + 4)),
            Ok(())
        );
        assert_eq!(usage_vec(&usage), vec![0, 1, 1, 1]);
        assert_eq!(rewards_vec(&rewards), vec![0, 1, 1, 1]);

        // Now extend all the way to FUTURE_EPOCHS with a later reservation; original 1..3 must remain.
        assert_eq!(
            usage.reserve_capacity(req, EpochNumber(base + 5), EpochNumber(end_full)),
            Ok(())
        );
        assert_eq!(
            rewards.add_rewards(fee, EpochNumber(base + 5), EpochNumber(end_full)),
            Ok(())
        );

        // Build expected
        let mut expected = vec![0u64; FUTURE_EPOCHS];
        expected[1] = 1;
        expected[2] = 1;
        expected[3] = 1;
        for i in 5..FUTURE_EPOCHS {
            expected[i] = 1;
        }

        assert_eq!(usage_vec(&usage), expected);

        let mut expected_fees = vec![0u64; FUTURE_EPOCHS];
        expected_fees[1] = 1;
        expected_fees[2] = 1;
        expected_fees[3] = 1;
        for i in 5..FUTURE_EPOCHS {
            expected_fees[i] = 1;
        }
        assert_eq!(rewards_vec(&rewards), expected_fees);
    }

    #[test]
    fn cancel_slash() {
        let base = 500;
        // Prepare: indices 0..4 = [10,10,10,10,10]
        let mut usage = make_usage(base, &[10, 10, 10, 10, 10]);
        let mut rewards = make_rewards(base, &[5, 5, 5, 5, 5]);

        // Cancel [base+1, base+4) by 3 => affects indices 1,2,3
        assert_eq!(
            usage.cancel_capacity(EpochNumber(base + 1), EpochNumber(base + 4), StorageUnits(3)),
            Ok(())
        );
        assert_eq!(usage_vec(&usage), vec![10, 7, 7, 7, 10]);

        // Slash [base+2, base+6) by 2 => affects indices 2,3,4 (5 is out of len)
        assert_eq!(
            rewards.slash_rewards(EpochNumber(base + 2), EpochNumber(base + 6), TAPE(2)),
            Ok(())
        );
        assert_eq!(rewards_vec(&rewards), vec![5, 5, 3, 3, 3]);
    }

    #[test]
    fn bounds_checks() {
        let base = 42;
        let usage = make_usage(base, &[1, 2, 3]);
        let rewards = make_rewards(base, &[7, 8, 9]);

        // In-bounds
        assert_eq!(usage.get_usage_at(EpochNumber(base + 1)), Ok(StorageUnits(2)));
        assert_eq!(rewards.get_rewards_at(EpochNumber(base + 2)), Ok(TAPE(9)));

        // Past
        assert_eq!(
            usage.get_usage_at(EpochNumber(base - 1)),
            Err(AccountingError::EpochInPast)
        );

        // Too far
        assert_eq!(
            usage.get_usage_at(EpochNumber(base + FUTURE_EPOCHS as u64)),
            Err(AccountingError::EpochTooFar)
        );
    }
}
