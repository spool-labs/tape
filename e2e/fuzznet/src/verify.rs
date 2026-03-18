use tape_core::erasure::slice_for_spool;
use tape_core::spooler::SpoolIndex;
use tape_store::ops::{SliceOps, SpoolOps, TrackOps};
use tape_e2e_simnet::TestNode;
use tape_store::types::TrackInfo;

/// Verify all slices in Active spools match their track commitments.
/// Logs errors for any integrity violations found.
pub fn verify_spool_integrity(nodes: &[TestNode]) {
    let mut total_checked = 0u64;
    let mut total_failures = 0u64;
    let mut skipped_no_track = 0u64;

    for (i, node) in nodes.iter().enumerate() {
        if !node.is_running() {
            continue;
        }
        let store = &node.context().store;

        let spools = store.iter_all_spools().expect("iter spools");
        for (spool_id, state) in &spools {
            if !state.is_active() {
                continue;
            }

            let slices = store.iter_slices_by_spool(*spool_id).expect("iter slices");
            for (track_addr, slice_data) in &slices {
                let track_info = match store.get_track(*track_addr).expect("read track") {
                    Some(t) => t,
                    None => {
                        skipped_no_track += 1;
                        continue;
                    }
                };

                if let Err(e) = validate_slice_entry(*spool_id, &track_info, slice_data) {
                    tracing::error!(
                        node = i,
                        spool = %spool_id,
                        track = ?track_addr,
                        spool_group = %track_info.spool_group,
                        slice_len = slice_data.len(),
                        original_size = track_info.original_size,
                        error = %e,
                        "spool integrity violation"
                    );
                    total_failures += 1;
                }
                total_checked += 1;
            }
        }
    }

    if total_failures > 0 {
        tracing::error!(
            total_checked,
            total_failures,
            skipped_no_track,
            "spool integrity check found violations"
        );
    } else {
        tracing::info!(
            total_checked,
            skipped_no_track,
            "spool integrity check passed"
        );
    }
}

fn validate_slice_entry(
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
