use tape_core::erasure::slice_for_spool;
use tape_core::spooler::SpoolIndex;
use tape_store::types::TrackInfo;

/// Validate an untrusted slice before local persistence.
pub fn validate_slice_entry(
    spool: SpoolIndex,
    track_info: &TrackInfo,
    data: &[u8],
) -> Result<(), String> {
    let slice_index = slice_for_spool(track_info.spool_group, spool)
        .ok_or_else(|| "track not mapped to this spool group".to_string())?;

    if track_info.original_size > 0 && data.is_empty() {
        return Err("empty slice for non-empty track".to_string());
    }

    let expected_max = track_info
        .stripe_size
        .checked_mul(track_info.stripe_count)
        .ok_or_else(|| "invalid stripe dimensions".to_string())?;
    if expected_max > 0 && data.len() as u64 > expected_max {
        return Err("slice exceeds expected decoded size".to_string());
    }

    if !track_info.verify_slice(slice_index, data) {
        return Err("slice does not match commitment".to_string());
    }

    Ok(())
}
