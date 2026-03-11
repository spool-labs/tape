use tape_node::tasks::spool_support::validate_slice_entry;
use tape_store::ops::{SliceOps, SpoolOps, TrackOps};
use tape_e2e_simnet::TestNode;

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
