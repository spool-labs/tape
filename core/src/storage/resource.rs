use crate::{
    types::*,
    ring::*,
    coin::*,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResourceError {
    EndNotAfterStart,
    EndNotAfterCurrent,
    StartNotFuture,
    RangeTooLarge,
    ExceedsFutureEpochs,
    IndexOutOfBounds,
    StorageOverflow,
}

pub fn has_capacity_for<const N: usize>(
    additional_units: StorageUnits,
    start_epoch: EpochNumber,
    end_epoch: EpochNumber,
    current_capacity: StorageUnits,
    current_epoch: EpochNumber,
    future_usage: &RingBuffer<StorageUnits, N>,
) -> bool {
    match check_capacity(
        additional_units,
        start_epoch,
        end_epoch,
        current_capacity,
        current_epoch,
        future_usage,
    ) {
        Ok(result) => result,
        Err(_) => false,
    }
}

pub fn check_capacity<const N: usize>(
    additional_units: StorageUnits,
    start_epoch: EpochNumber,
    end_epoch: EpochNumber,
    current_capacity: StorageUnits,
    current_epoch: EpochNumber,
    future_usage: &RingBuffer<StorageUnits, N>,
) -> Result<bool, ResourceError> {

    // Basic range checks
    range_check(start_epoch, end_epoch, current_epoch)?;

    // Window and horizon (end is exclusive)
    let epoch_count = get_epoch_count(start_epoch, end_epoch)?;
    let horizon = get_horizon(current_epoch, end_epoch)?;

    // Must be addressable within the ring capacity
    if epoch_count > N as u64 {
        return Err(ResourceError::RangeTooLarge);
    }
    if horizon > N as u64 {
        return Err(ResourceError::ExceedsFutureEpochs);
    }

    assert!(epoch_count >= 1);
    assert!(horizon >= epoch_count);

    // [start, end), end exclusive
    let start_offset = (start_epoch - current_epoch).as_u64() as usize;
    let end_offset = start_offset + epoch_count as usize;

    // Check each epoch in the range
    for i in start_offset..end_offset {
        // By construction: i < horizon <= N, so never beyond capacity.

        // If beyond current length, treat as zero usage.
        let used = if i < future_usage.len() {
            // Safe due to bound above
            *future_usage.get(i).ok_or(ResourceError::IndexOutOfBounds)?
        } else {
            StorageUnits::zero()
        };

        let new_usage = used
            .checked_add(additional_units)
            .ok_or(ResourceError::StorageOverflow)?;

        if new_usage > current_capacity {
            return Ok(false);
        }
    }

    Ok(true)
}

pub fn reserve_capacity<const N: usize>(
    additional_units: StorageUnits,
    start_epoch: EpochNumber,
    end_epoch: EpochNumber,
    current_epoch: EpochNumber,
    fee_per_epoch: Coin<TAPE>,
    future_usage: &mut RingBuffer<StorageUnits, N>,
    future_rewards: &mut RingBuffer<Coin<TAPE>, N>,
) -> Result<(), ResourceError> {

    // Basic range checks
    range_check(start_epoch, end_epoch, current_epoch)?;

    // Window and horizon (end is exclusive)
    let epoch_count = get_epoch_count(start_epoch, end_epoch)?;
    let horizon = get_horizon(current_epoch, end_epoch)?;

    // Must be addressable within the ring capacity
    if epoch_count > N as u64 {
        return Err(ResourceError::RangeTooLarge);
    }
    if horizon > N as u64 {
        return Err(ResourceError::ExceedsFutureEpochs);
    }

    assert!(epoch_count >= 1);
    assert!(horizon >= epoch_count);

    // Extend both buffers to cover [current_epoch, end_epoch)
    let target_len = horizon as usize;
    while future_usage.len() < target_len {
        future_usage.push(StorageUnits::zero());
    }
    while future_rewards.len() < target_len {
        future_rewards.push(Coin::<TAPE>::zero());
    }

    let start_offset = (start_epoch - current_epoch).as_u64() as usize;
    let end_offset = start_offset + epoch_count as usize;

    for i in start_offset..end_offset {
        let entry = future_usage
            .get_mut(i)
            .ok_or(ResourceError::IndexOutOfBounds)?;

        *entry = entry
            .checked_add(additional_units)
            .ok_or(ResourceError::StorageOverflow)?;

        let fees = future_rewards
            .get_mut(i)
            .ok_or(ResourceError::IndexOutOfBounds)?;

        *fees = fees
            .checked_add(fee_per_epoch)
            .ok_or(ResourceError::StorageOverflow)?;
    }

    Ok(())
}

pub fn range_check(
    start_epoch: EpochNumber,
    end_epoch: EpochNumber,
    current_epoch: EpochNumber,
) -> Result<(), ResourceError> {
    if end_epoch <= start_epoch {
        return Err(ResourceError::EndNotAfterStart);
    }
    if start_epoch <= current_epoch {
        return Err(ResourceError::StartNotFuture);
    }
    Ok(())
}

pub fn get_horizon(
    current_epoch: EpochNumber,
    end_epoch: EpochNumber,
) -> Result<u64, ResourceError> {
    if end_epoch <= current_epoch {
        return Err(ResourceError::EndNotAfterCurrent);
    }

    let horizon = (end_epoch - current_epoch).as_u64();
    Ok(horizon)
}

pub fn get_epoch_count(
    start_epoch: EpochNumber,
    end_epoch: EpochNumber,
) -> Result<u64, ResourceError> {
    if end_epoch <= start_epoch {
        return Err(ResourceError::EndNotAfterStart);
    }

    let epoch_count = (end_epoch - start_epoch).as_u64();
    Ok(epoch_count)
}

#[cfg(test)]
mod tests {
    use super::*;

    const FUTURE_EPOCHS: usize = 10;

    type StorageBuffer = RingBuffer::<StorageUnits, FUTURE_EPOCHS>;
    type FeesBuffer = RingBuffer::<Coin<TAPE>, FUTURE_EPOCHS>;

    fn test_has_capacity(
        additional_units: u64,
        start_epoch: u64,
        end_epoch: u64,
        current_capacity: u64,
        current_epoch: u64,
        storage_used: &[u64],
        result: Result<bool, ResourceError>,
    ) {
        let mut buffer = StorageBuffer::new();
        for &u in storage_used {
            buffer.push(StorageUnits::new(u));
        }

        let res = check_capacity(
            StorageUnits(additional_units),
            EpochNumber(start_epoch),
            EpochNumber(end_epoch),
            StorageUnits(current_capacity),
            EpochNumber(current_epoch),
            &buffer,
        );

        assert_eq!(res, result);
    }

    fn test_reserve_capacity(
        additional_units: u64,
        start_epoch: u64,
        end_epoch: u64,
        current_epoch: u64,
        fee_per_epoch: u64,
        storage_used: &[u64],
        fees_collected: &[u64],
        result: Result<(), ResourceError>,
        expected_storage: &[u64],
        expected_fees: &[u64],
    ) {
        let mut storage_buffer = StorageBuffer::new();
        for &u in storage_used {
            storage_buffer.push(StorageUnits::new(u));
        }

        let mut fees_buffer = FeesBuffer::new();
        for &f in fees_collected {
            fees_buffer.push(Coin::<TAPE>::new(f));
        }

        let res = reserve_capacity(
            StorageUnits(additional_units),
            EpochNumber(start_epoch),
            EpochNumber(end_epoch),
            EpochNumber(current_epoch),
            TAPE(fee_per_epoch),
            &mut storage_buffer,
            &mut fees_buffer,
        );

        assert_eq!(res, result);

        let actual_storage: Vec<u64> = storage_buffer.iter().map(|u| u.0).collect();
        let actual_fees: Vec<u64> = fees_buffer.iter().map(|f| f.0).collect();

        assert_eq!(actual_storage, expected_storage);
        assert_eq!(actual_fees, expected_fees);
    }

    #[test]
    fn test_capacity() {
        let req = 300;
        let cap = 1000;
        let now = 100;
        let start = 102;
        let end = 105;

        // No existing usage
        test_has_capacity(req, start, end, cap, now, &[], Ok(true));

        // Basic fits
        test_has_capacity(req, start, end, cap, now, &[100, 200], Ok(true));

        // Many epochs, all fit
        test_has_capacity(req, start, end, cap, now, &[200; 10], Ok(true));

        // Some epochs exceed
        test_has_capacity(req, start, end, cap, now, &[701; 10], Ok(false));

        // Many epochs, fit exactly on 3
        test_has_capacity(req, start, end, cap, now, &[700; 10], Ok(true));

        // One epochs, fit exactly on first
        test_has_capacity(req, start, end, cap, now, &[700], Ok(true));
    }

    #[test]
    fn test_reserve() {
        let req = 300;
        let now = 100;
        let start = 102;
        let end = 105;
        let fee = 50;

        // No existing usage, extends and fits
        test_reserve_capacity(
            req, start, end, now, fee,
            &[],
            &[],
            Ok(()),
            &[0, 0, 300, 300, 300],
            &[0, 0, 50, 50, 50],
        );

        // Existing usage, extends and fits
        test_reserve_capacity(
            req, start, end, now, fee,
            &[100, 200],
            &[10, 20],
            Ok(()),
            &[100, 200, 300, 300, 300],
            &[10, 20, 50, 50, 50],
        );

        // Existing usage, overlap and fits
        test_reserve_capacity(
            req, start, end, now, fee,
            &[100, 200, 700, 100],
            &[10, 20, 70, 10],
            Ok(()),
            &[100, 200, 1000, 400, 300],
            &[10, 20, 120, 60, 50],
        );
    }

    #[test]
    fn has_capacity_strict_ordering_errors() {
        let cap = 1_000;
        let now = 100;

        // end == start
        test_has_capacity(1, 105, 105, cap, now, &[], Err(ResourceError::EndNotAfterStart));

        // end < start
        test_has_capacity(1, 106, 105, cap, now, &[], Err(ResourceError::EndNotAfterStart));

        // start == current
        test_has_capacity(1, now, 101, cap, now, &[], Err(ResourceError::StartNotFuture));

        // start < current
        test_has_capacity(1, 99, 101, cap, now, &[], Err(ResourceError::StartNotFuture));
    }

    #[test]
    fn reserve_strict_ordering_errors() {
        let now = 100;
        let fee = 1;

        // end == start
        test_reserve_capacity(
            1, 105, 105, now, fee,
            &[], &[],
            Err(ResourceError::EndNotAfterStart),
            &[], &[],
        );

        // start == current
        test_reserve_capacity(
            1, now, now + 1, now, fee,
            &[], &[],
            Err(ResourceError::StartNotFuture),
            &[], &[],
        );
    }

    #[test]
    fn test_extend_to_horizon() {
        // Here we test the case where the reservation extends the buffer to exactly N entries.

        let req = 7;
        let now = 100;
        let start = now + 5; // reserve last 5 slots
        let end = now + FUTURE_EPOCHS as u64; // horizon == N
        let fee = 3;

        test_reserve_capacity(
            req, start, end, now, fee,
            &[], &[],
            Ok(()),
            // len becomes horizon (10). indices [0..5) = 0, [5..10) += req
            &[0, 0, 0, 0, 0, 7, 7, 7, 7, 7],
            &[0, 0, 0, 0, 0, 3, 3, 3, 3, 3],
        );
    }

    #[test]
    fn horizon_exceeds_n_fails() {
        // Here we test the case where the reservation would extend the buffer beyond N entries.

        let req = 1;
        let now = 100;
        let start = now + 1;
        let end = now + FUTURE_EPOCHS as u64 + 1; // horizon = N + 1
        let fee = 1;

        test_reserve_capacity(
            req, start, end, now, fee,
            &[], &[],
            Err(ResourceError::ExceedsFutureEpochs),
            &[], &[],
        );

        test_has_capacity(
            req, start, end, 1_000, now, &[],
            Err(ResourceError::ExceedsFutureEpochs),
        );
    }

    #[test]
    fn range_too_large_fails() {
        // Here we test the case where the reservation range itself exceeds N epochs.

        let cap = 1_000;
        let now = 100;
        let start = now + 1;
        let end = start + (FUTURE_EPOCHS as u64) + 1; // epoch_count = N + 1

        test_has_capacity(
            1, start, end, cap, now, &[],
            Err(ResourceError::RangeTooLarge),
        );

        test_reserve_capacity(
            1, start, end, now, 1,
            &[], &[],
            Err(ResourceError::RangeTooLarge),
            &[], &[],
        );
    }

    #[test]
    fn single_epoch_window_works() {
        // Reserve exactly one epoch [start, start+1)

        let req = 250;
        let cap = 1_000;
        let now = 100;
        let start = 103;
        let end = 104;

        test_has_capacity(req, start, end, cap, now, &[], Ok(true));
        test_reserve_capacity(
            req, start, end, now, 5,
            &[], &[],
            Ok(()),
            &[0, 0, 0, 250],
            &[0, 0, 0, 5],
        );
    }

    #[test]
    fn zero_fill_when_range_extends_beyond_current_len() {
        // Here we test the case where the reservation range extends beyond the current length,
        // requiring zero-fill of the gap.

        let req = 100;
        let fee = 2;
        let now = 200;
        let start = 203;
        let end = 206; // start_offset=3, epoch_count=3, horizon=6

        // Existing usage covers only 2 epochs.
        test_reserve_capacity(
            req, start, end, now, fee,
            &[5, 10],        // len = 2
            &[1, 1],         // fees len = 2
            Ok(()),
            // After: horizon = 6 entries. indices 0..2 keep existing, index 2 remains 0,
            // indices 3..5 += req.
            &[5, 10, 0, 100, 100, 100],
            &[1, 1, 0, 2, 2, 2],
        );
    }

    #[test]
    fn has_capacity_exact_edge_and_failure() {
        // Here we test edge cases around capacity limits.

        let cap = 1_000;
        let now = 100;
        let start = 102;
        let end = 106; // indices 2..5

        // One epoch would exceed cap (800 + 300 > 1000), others fit.
        test_has_capacity(300, start, end, cap, now, &[400, 500, 800, 100, 100], Ok(false));

        // Exactly at cap (700 + 300 == 1000) across the window → Ok(true)
        test_has_capacity(300, start, end, cap, now, &[700; 10], Ok(true));
    }

    #[test]
    fn overflow_in_has_capacity_on_usage_add() {
        // Prepare a slot with u64::MAX usage; adding 1 overflows -> StorageOverflow.
        let cap = u64::MAX; // irrelevant; overflow happens first
        let now = 100;
        let start = 102;
        let end = 103; // one epoch window at index 2

        let used = vec![0, 0, u64::MAX];
        test_has_capacity(1, start, end, cap, now, &used, Err(ResourceError::StorageOverflow));
    }

    #[test]
    fn overflow_in_reserve_on_usage_add() {
        // same as above, but in reserve(); expect error and buffers extended but unchanged at slot.
        let now = 100;
        let start = 102;
        let end = 103; // index 2
        let fee = 1;

        test_reserve_capacity(
            1, start, end, now, fee,
            &[1, 2, u64::MAX], // len already covers the slot
            &[],               // fees start empty
            Err(ResourceError::StorageOverflow),
            // After failure: we extended fees to horizon (3) with zeros,
            // storage remains unchanged.
            &[1, 2, u64::MAX],
            &[0, 0, 0],
        );
    }

    #[test]
    fn overflow_in_reserve_on_fee_add_partial_apply() {
        // This demonstrates that storage is updated before fee overflow occurs.
        let now = 100;
        let start = 101; // index 1
        let end = 103;   // indices 1..2 (two epochs)
        let req = 10;
        let fee = 1;

        // Fees at index 1 are already max -> adding fee will overflow on the first iter.
        test_reserve_capacity(
            req, start, end, now, fee,
            &[5, 6],                  // storage existing
            &[0, u64::MAX],           // fees existing (overflow at i=1)
            Err(ResourceError::StorageOverflow),
            // storage at index 1 is incremented (partial apply), index 2 untouched.
            // buffers extended to horizon = end - now = 3.
            &[5, 16, 0],
            &[0, u64::MAX, 0],
        );
    }

    #[test]
    fn multiple_overlapping_reservations_accumulate() {
        let now = 1_000;
        let fee = 7;

        let mut storage = StorageBuffer::new();
        let mut fees = FeesBuffer::new();

        // Reserve [now+2, now+5) → indices 2,3,4
        assert_eq!(
            reserve_capacity(
                StorageUnits(100),
                EpochNumber(now + 2),
                EpochNumber(now + 5),
                EpochNumber(now),
                TAPE(fee),
                &mut storage,
                &mut fees
            ),
            Ok(())
        );

        // Reserve overlapping [now+4, now+7) → indices 4,5,6
        assert_eq!(
            reserve_capacity(
                StorageUnits(50),
                EpochNumber(now + 4),
                EpochNumber(now + 7),
                EpochNumber(now),
                TAPE(fee),
                &mut storage,
                &mut fees
            ),
            Ok(())
        );

        let s: Vec<u64> = storage.iter().map(|u| u.0).collect();
        let f: Vec<u64> = fees.iter().map(|c| c.0).collect();

        // horizon after second call: end-now = 7 → len = 7
        assert_eq!(s, vec![0, 0, 100, 100, 150, 50, 50]);
        assert_eq!(f, vec![0, 0, 7, 7, 14, 7, 7]);
    }

    #[test]
    fn extend_to_full_capacity_never_rotates() {
        // Prove we can extend len to N without overwriting earliest entries.
        let req = 1;
        let fee = 1;
        let now = 10_000;
        let start = now + 1;
        let end = now + FUTURE_EPOCHS as u64; // horizon == N

        // Pre-fill first 3 reserved epochs: [now+1, now+4) -> indices 1,2,3
        test_reserve_capacity(
            req, start, start + 3, now, fee,
            &[], &[],
            Ok(()),
            // horizon after this call = (start+3) - now = 4 → len == 4
            // indices: 0=0, 1..3=1
            &[0, 1, 1, 1],
            &[0, 1, 1, 1],
        );

        // Now extend all the way to N with a later reservation; original 1..3 must remain.
        test_reserve_capacity(
            req, now + 5, end, now, fee,
            // carry forward the actual buffers from the previous step (including the leading 0)
            &[0, 1, 1, 1],
            &[0, 1, 1, 1],
            Ok(()),
            // horizon = end - now = N → len == N
            // indices: 0=0, 1..3=1 (from first call), 4=0 (untouched), 5..9=1 (from second call)
            &[0, 1, 1, 1, 0, 1, 1, 1, 1, 1],
            &[0, 1, 1, 1, 0, 1, 1, 1, 1, 1],
        );
    }
}
