use std::collections::HashMap;

use store::Store;
use tape_core::spooler::{SpoolGroup, SpoolIndex};
use tape_store::ops::{SliceOps, SpoolOps, TrackOps};
use tape_store::TapeStore;
use tracing::debug;

use super::error::RecoveryError;
use super::SCAN_BATCH_SIZE;

/// Sentinel spool ID for the shared scan cursor, outside valid [0, 1000).
const SCAN_CURSOR_SPOOL: u16 = u16::MAX;

pub struct ScanResult {
    pub enqueued: usize,
    pub scanned: usize,
}

/// Single-pass scan of all tracks, bucketing missing slices into recovering spools.
///
/// Instead of scanning once per spool (50 spools = 50 full scans), this iterates
/// all tracks exactly once and checks each track against all recovering spools
/// via an O(1) group lookup.
///
/// The scan runs to completion internally (local DB reads only, ~5s for 1M tracks).
/// Uses a sentinel spool cursor for crash-resumability.
pub fn run_scan<S: Store>(
    store: &TapeStore<S>,
    recovering_spools: &[(SpoolIndex, SpoolGroup)],
) -> Result<ScanResult, RecoveryError> {
    if recovering_spools.is_empty() {
        return Ok(ScanResult {
            enqueued: 0,
            scanned: 0,
        });
    }

    // Build group → [spool_indices] map for O(1) lookup per track.
    let mut by_group: HashMap<SpoolGroup, Vec<SpoolIndex>> = HashMap::new();
    for &(spool, group) in recovering_spools {
        by_group.entry(group).or_default().push(spool);
    }

    let mut cursor = store.get_spool_sync_cursor(SCAN_CURSOR_SPOOL)?;
    let mut total_enqueued = 0usize;
    let mut total_scanned = 0usize;

    loop {
        let batch = store.iter_tracks_from(cursor, SCAN_BATCH_SIZE)?;
        if batch.is_empty() {
            break;
        }

        let batch_len = batch.len();
        let mut last_key = None;

        for (track_address, track_info) in &batch {
            last_key = Some(*track_address);
            total_scanned += 1;

            // Skip empty tracks
            if track_info.original_size == 0 {
                continue;
            }

            let group = track_info.spool_group;

            // Check if any of our recovering spools belong to this group
            let spools = match by_group.get(&group) {
                Some(s) => s,
                None => continue,
            };

            // For each recovering spool in this group, check if we're missing the slice
            for &spool in spools {
                if store.has_slice(spool, *track_address)? {
                    continue;
                }
                store.add_pending_recovery(spool, *track_address)?;
                total_enqueued += 1;
            }
        }

        // Persist cursor for crash-resumability
        if let Some(key) = last_key {
            store.set_spool_sync_cursor(SCAN_CURSOR_SPOOL, key)?;
            cursor = Some(key);
        }

        if batch_len < SCAN_BATCH_SIZE {
            break;
        }
    }

    // Clear sentinel cursor on completion
    store.remove_spool_sync_cursor(SCAN_CURSOR_SPOOL)?;

    if total_enqueued > 0 {
        debug!(enqueued = total_enqueued, scanned = total_scanned, "scan complete");
    }

    Ok(ScanResult {
        enqueued: total_enqueued,
        scanned: total_scanned,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use store_memory::MemoryStore;
    use tape_core::erasure::{group_for_spool, group_start};
    use tape_store::types::{Pubkey, TrackInfo};

    fn test_store() -> TapeStore<MemoryStore> {
        TapeStore::new(MemoryStore::new())
    }

    fn make_track(group: SpoolGroup, size: u64) -> TrackInfo {
        TrackInfo {
            tape_address: Pubkey::new_unique(),
            spool_group: group,
            original_size: size,
            stripe_size: 0,
            stripe_count: 0,
            encoding_type: 0,
            encoding_params: 0,
            commitment: vec![],
        }
    }

    #[test]
    fn scan_empty_store() {
        let store = test_store();
        let result = run_scan(&store, &[(5, group_for_spool(5))]).unwrap();
        assert_eq!(result.enqueued, 0);
        assert_eq!(result.scanned, 0);
    }

    #[test]
    fn scan_no_recovering_spools() {
        let store = test_store();
        let addr = Pubkey::new_unique();
        store
            .put_track(addr, make_track(0, 1024))
            .unwrap();
        let result = run_scan(&store, &[]).unwrap();
        assert_eq!(result.enqueued, 0);
        assert_eq!(result.scanned, 0);
    }

    #[test]
    fn scan_enqueues_missing_slices() {
        let store = test_store();
        let group: SpoolGroup = 0;
        let spool: SpoolIndex = group_start(group) + 3; // position 3

        let addr = Pubkey::new_unique();
        store
            .put_track(addr, make_track(group, 1024))
            .unwrap();

        let result = run_scan(&store, &[(spool, group)]).unwrap();
        assert_eq!(result.enqueued, 1);
        assert_eq!(result.scanned, 1);
        assert!(store.has_pending_recovery(spool, addr).unwrap());
    }

    #[test]
    fn scan_skips_existing_slices() {
        let store = test_store();
        let group: SpoolGroup = 0;
        let spool: SpoolIndex = group_start(group) + 3;

        let addr = Pubkey::new_unique();
        store
            .put_track(addr, make_track(group, 1024))
            .unwrap();
        store.put_slice(spool, addr, vec![1, 2, 3]).unwrap();

        let result = run_scan(&store, &[(spool, group)]).unwrap();
        assert_eq!(result.enqueued, 0);
        assert_eq!(result.scanned, 1);
        assert!(!store.has_pending_recovery(spool, addr).unwrap());
    }

    #[test]
    fn scan_skips_empty_tracks() {
        let store = test_store();
        let group: SpoolGroup = 0;
        let spool: SpoolIndex = group_start(group);

        let addr = Pubkey::new_unique();
        store
            .put_track(addr, make_track(group, 0))
            .unwrap();

        let result = run_scan(&store, &[(spool, group)]).unwrap();
        assert_eq!(result.enqueued, 0);
        assert_eq!(result.scanned, 1);
    }

    #[test]
    fn scan_skips_wrong_group() {
        let store = test_store();
        let group: SpoolGroup = 0;
        let other_group: SpoolGroup = 5;
        let spool: SpoolIndex = group_start(group);

        let addr = Pubkey::new_unique();
        store
            .put_track(addr, make_track(other_group, 1024))
            .unwrap();

        let result = run_scan(&store, &[(spool, group)]).unwrap();
        assert_eq!(result.enqueued, 0);
        assert_eq!(result.scanned, 1);
    }

    #[test]
    fn scan_multiple_groups() {
        let store = test_store();
        let group0: SpoolGroup = 0;
        let group1: SpoolGroup = 1;
        let spool0: SpoolIndex = group_start(group0) + 2;
        let spool1: SpoolIndex = group_start(group1) + 5;

        let addr0 = Pubkey::new_unique();
        let addr1 = Pubkey::new_unique();
        store
            .put_track(addr0, make_track(group0, 1024))
            .unwrap();
        store
            .put_track(addr1, make_track(group1, 2048))
            .unwrap();

        let result =
            run_scan(&store, &[(spool0, group0), (spool1, group1)]).unwrap();
        assert_eq!(result.enqueued, 2);
        assert_eq!(result.scanned, 2);
        assert!(store.has_pending_recovery(spool0, addr0).unwrap());
        assert!(store.has_pending_recovery(spool1, addr1).unwrap());
    }

    #[test]
    fn scan_idempotent() {
        let store = test_store();
        let group: SpoolGroup = 0;
        let spool: SpoolIndex = group_start(group);

        let addr = Pubkey::new_unique();
        store
            .put_track(addr, make_track(group, 1024))
            .unwrap();

        let r1 = run_scan(&store, &[(spool, group)]).unwrap();
        assert_eq!(r1.enqueued, 1);

        // Second scan re-enqueues (add_pending_recovery is idempotent)
        let r2 = run_scan(&store, &[(spool, group)]).unwrap();
        assert_eq!(r2.enqueued, 1);

        // Still only one pending entry
        let pending = store.iter_pending_recoveries(spool, 100).unwrap();
        assert_eq!(pending.len(), 1);
    }

    #[test]
    fn scan_cursor_cleared() {
        let store = test_store();
        let group: SpoolGroup = 0;
        let spool: SpoolIndex = group_start(group);

        let addr = Pubkey::new_unique();
        store
            .put_track(addr, make_track(group, 1024))
            .unwrap();

        run_scan(&store, &[(spool, group)]).unwrap();

        // Sentinel cursor should be cleared after completion
        assert!(store.get_spool_sync_cursor(SCAN_CURSOR_SPOOL).unwrap().is_none());
    }

    #[test]
    fn scan_large_dataset() {
        let store = test_store();
        let group0: SpoolGroup = 0;
        let group1: SpoolGroup = 2;
        let spool0: SpoolIndex = group_start(group0) + 1;
        let spool1: SpoolIndex = group_start(group1) + 7;

        // 2500 tracks: 1500 in group0, 1000 in group1
        for _ in 0..1500 {
            store
                .put_track(Pubkey::new_unique(), make_track(group0, 512))
                .unwrap();
        }
        for _ in 0..1000 {
            store
                .put_track(Pubkey::new_unique(), make_track(group1, 512))
                .unwrap();
        }

        let result =
            run_scan(&store, &[(spool0, group0), (spool1, group1)]).unwrap();
        assert_eq!(result.scanned, 2500);
        assert_eq!(result.enqueued, 2500);

        let pending0 = store.iter_pending_recoveries(spool0, 2000).unwrap();
        assert_eq!(pending0.len(), 1500);
        let pending1 = store.iter_pending_recoveries(spool1, 2000).unwrap();
        assert_eq!(pending1.len(), 1000);
    }

}
