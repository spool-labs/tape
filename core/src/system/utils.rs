use crate::types::EpochNumber;
use super::SystemError;

/// Get the start and end offsets in the usage buffer for the specified epoch range.
pub fn get_offsets<const N: usize>(
    base_epoch: EpochNumber,
    start_epoch: EpochNumber,
    end_epoch: EpochNumber,
) -> Result<(usize, usize), SystemError> {
    if start_epoch < base_epoch {
        return Err(SystemError::StartNotAfterBase);
    }
    if end_epoch <= start_epoch {
        return Err(SystemError::EndNotAfterStart);
    }

    // Window and horizon (end is exclusive)
    let epoch_count = (end_epoch - start_epoch).as_u64();
    let horizon = (end_epoch - base_epoch).as_u64();

    // Must be addressable within the ring capacity
    if epoch_count > N as u64 {
        return Err(SystemError::RangeTooLarge);
    }
    if horizon > N as u64 {
        return Err(SystemError::ExceedsFutureEpochs);
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
    use crate::types::EpochNumber;

    const N: usize = 5;

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
            Err(SystemError::StartNotAfterBase)
        );

        // End not after start
        assert_eq!(
            get_offsets::<N>(base, EpochNumber(10), EpochNumber(10)),
            Err(SystemError::EndNotAfterStart)
        );
        assert_eq!(
            get_offsets::<N>(base, EpochNumber(11), EpochNumber(10)),
            Err(SystemError::EndNotAfterStart)
        );

        // Range too large
        assert_eq!(
            get_offsets::<N>(base, EpochNumber(10), EpochNumber(10 + N as u64 + 1)),
            Err(SystemError::RangeTooLarge)
        );
    }

}
