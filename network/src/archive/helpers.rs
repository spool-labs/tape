use std::sync::Arc;
use solana_sdk::pubkey::Pubkey;

use crate::store::*;

/// Determines if synchronization is needed for a tape (i.e., if more segments need to be fetched).
pub fn sync_needed(
    store: &Arc<TapeStore>,
    tape_address: &Pubkey,
    total_segments: u64,
) -> Result<bool, StoreError> {

    let current_count = store
        .get_segment_count(tape_address)
        .unwrap_or(0);

    Ok(current_count < total_segments as usize)
}
