use crate::types::{EpochNumber, StorageUnits};

pub type MergeResult = (
    EpochNumber,  // active_epoch
    EpochNumber,  // expiry_epoch
    StorageUnits, // storage_capacity
    StorageUnits  // storage_used
);

/// Computes the outcome of merging two tapes.
pub fn merge_tapes(
    source_active: EpochNumber,
    source_expiry: EpochNumber,
    source_capacity: StorageUnits,
    source_used: StorageUnits,
    dest_active: EpochNumber,
    dest_expiry: EpochNumber,
    dest_capacity: StorageUnits,
    dest_used: StorageUnits,
) -> Option<MergeResult> {

    if source_used > source_capacity || dest_used > dest_capacity {
        return None;
    }

    let total_capacity = dest_capacity
        .checked_add(source_capacity)?;

    let max_used = source_used
        .max(dest_used);

    // Identical windows
    if source_active == dest_active && source_expiry == dest_expiry {
        let total_used = dest_used.checked_add(source_used)?;
        if total_used > total_capacity {
            return None;
        }
        return Some((dest_active, dest_expiry, total_capacity, total_used));
    }

    // Touching windows; source before dest
    if source_expiry == dest_active {
        return Some((source_active, dest_expiry, total_capacity, max_used));
    }

    // Touching windows; dest before source
    if dest_expiry == source_active {
        return Some((dest_active, source_expiry, total_capacity, max_used));
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn epoch(n: u64) -> EpochNumber { EpochNumber(n) }
    fn units(n: u64) -> StorageUnits { StorageUnits(n) }


    #[test]
    fn same_window() {
        let res = merge_tapes(
            epoch(10), epoch(20), units(200), units(30),  // source
            epoch(10), epoch(20), units(100), units(20),  // dest
        );
        assert_eq!(res, Some((epoch(10), epoch(20), units(300), units(50))));
    }

    #[test]
    fn exceeds_capacity() {
        // used sums to 400, capacity sums to 300 -> reject
        let res = merge_tapes(
            epoch(10), epoch(20), units(200), units(250),
            epoch(10), epoch(20), units(100), units(150),
        );
        assert_eq!(res, None);
    }

    #[test]
    fn touching_extend() {
        // dest [10,20), source [20,30) -> result [10,30), capacity sums
        let res = merge_tapes(
            epoch(20), epoch(30), units(500), units(0),   // source
            epoch(10), epoch(20), units(500), units(123), // dest
        );
        assert_eq!(res, Some((epoch(10), epoch(30), units(1000), units(123))));
    }

    #[test]
    fn touching_backward() {
        // source [40,50), dest [50,60) -> result [40,60), capacity sums
        let res = merge_tapes(
            epoch(40), epoch(50), units(250), units(10),  // source
            epoch(50), epoch(60), units(250), units(0),   // dest
        );
        assert_eq!(res, Some((epoch(40), epoch(60), units(500), units(10))));
    }

    #[test]
    fn overlap() {
        assert_eq!(
            merge_tapes(
                epoch(19), epoch(25), units(100), units(10),
                epoch(10), epoch(20), units(100), units(5),
            ),
            None
        );
    }

    #[test]
    fn gap() {
        assert_eq!(
            merge_tapes(
                epoch(21), epoch(25), units(100), units(10),
                epoch(10), epoch(20), units(100), units(5),
            ),
            None
        );
    }

    #[test]
    fn reject_invalid() {
        // Invalid input: source used > source capacity
        let res = merge_tapes(
            epoch(20), epoch(30), units(250), units(300),
            epoch(10), epoch(20), units(250), units(200),
        );
        assert_eq!(res, None);
    }
}
