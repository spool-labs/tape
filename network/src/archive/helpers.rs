use std::sync::Arc;
use solana_sdk::pubkey::Pubkey;

use crate::store::*;

/// Determines if synchronization is needed for a tape (i.e., if more segments need to be fetched).
pub fn sync_needed(
    store: &Arc<TapeStore>,
    tape_address: &Pubkey,
    total_segments: u64,
) -> Result<bool, StoreError> {
    let sector_count = store.get_sector_count(tape_address).unwrap_or(0);
    let upper_bound = sector_count as u64 * SECTOR_LEAVES as u64;

    if total_segments > upper_bound {
        // More sectors are needed 
        Ok(true)
    } else if total_segments > 0 && total_segments <= upper_bound {
        // Check if the last sector has enough segments in it
        let last_sector_index = sector_count.saturating_sub(1) as u64;
        if let Ok(last_sector) = store.get_sector(tape_address, last_sector_index) {
            let segments_in_last_sector = last_sector.count_segments() as u64;
            let total_stored_segments = segments_in_last_sector + (last_sector_index * SECTOR_LEAVES as u64);
            Ok(total_stored_segments < total_segments)
        } else {
            // Last sector doesn't exist, so we need to sync
            Ok(true)
        }
    } else {
        // No segments are needed
        Ok(false)
    }
}
