use tape_core::erasure::{slice_for_spool, spool_in_group};
use tape_store::ops::{ObjectInfoOps, SliceOps, TrackOps};
use tape_store::types::{ObjectInfo, TrackInfo};

/// Validate an untrusted slice before local persistence.
pub fn validate_slice_entry(
    spool: u16,
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

/// Check whether any certified track in this spool's group is missing its slice.
pub fn has_missing_slices(
    store: &(impl TrackOps + ObjectInfoOps + SliceOps),
    spool: u16,
) -> Result<bool, String> {
    let mut cursor = None;
    const BATCH: usize = 100;

    loop {
        let tracks = store
            .iter_tracks_from(cursor, BATCH)
            .map_err(|e| format!("iter_tracks: {e}"))?;

        if tracks.is_empty() {
            break;
        }

        for (track_addr, track_info) in &tracks {
            if !spool_in_group(spool, track_info.spool_group) {
                continue;
            }

            let certified = match store.get_object_info(*track_addr) {
                Ok(Some(ObjectInfo::Valid {
                    certified_epoch: Some(_),
                    ..
                })) => true,
                Ok(_) => false,
                Err(e) => return Err(format!("get_object_info: {e}")),
            };
            if !certified {
                continue;
            }

            let has = store
                .has_slice(spool, *track_addr)
                .map_err(|e| format!("has_slice: {e}"))?;

            if !has {
                return Ok(true);
            }
        }

        cursor = tracks.last().map(|(addr, _)| *addr);
    }

    Ok(false)
}
