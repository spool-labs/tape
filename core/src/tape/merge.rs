use crate::types::{EpochNumber, StorageUnits};

pub type MergeResult = (
    EpochNumber,  // active_epoch
    EpochNumber,  // expiry_epoch
    StorageUnits, // storage_capacity
    StorageUnits  // storage_used
);

/// Computes the outcome of merging two tapes.
pub fn merge_tapes(
    source_active_epoch: EpochNumber,
    source_expiry_epoch: EpochNumber,
    source_capacity: StorageUnits,
    source_used: StorageUnits,
    dest_active_epoch: EpochNumber,
    dest_expiry_epoch: EpochNumber,
    dest_capacity: StorageUnits,
    dest_used: StorageUnits,
) -> Option<MergeResult> {

    // Basic invariants: each tape's used must not exceed its own capacity
    if source_used > source_capacity || dest_used > dest_capacity {
        return None;
    }

    // Case 1: identical windows -> increase capacity (sum capacity and used)
    if source_active_epoch == dest_active_epoch && source_expiry_epoch == dest_expiry_epoch {
        let combined_capacity = dest_capacity
            .checked_add(source_capacity)?;
        let combined_used = dest_used
            .checked_add(source_used)?;

        if combined_used > combined_capacity {
            return None;
        }

        return Some((dest_active_epoch, dest_expiry_epoch, combined_capacity, combined_used));
    }

    // Case 2: touching windows, source before dest -> extend backward
    if source_expiry_epoch == dest_active_epoch {

        let combined_capacity = dest_capacity
            .checked_add(source_capacity)?;

        let merged_used = if source_used > dest_used {
            source_used 
        } else { 
            dest_used 
        };

        return Some((source_active_epoch, dest_expiry_epoch, combined_capacity, merged_used));
    }

    // Case 3: touching windows, dest before source -> extend forward
    if dest_expiry_epoch == source_active_epoch {

        let combined_capacity = dest_capacity
            .checked_add(source_capacity)?;

        let merged_used = if source_used > dest_used { 
            source_used 
        } else { 
            dest_used 
        };

        return Some((dest_active_epoch, source_expiry_epoch, combined_capacity, merged_used));
    }

    // Overlap or gap -> reject
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
