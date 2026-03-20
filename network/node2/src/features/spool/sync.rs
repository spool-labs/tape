use std::sync::Arc;

use rpc::Rpc;
use store::Store;
use tape_core::spooler::SpoolIndex;
use tape_core::types::NodeId;
use tape_protocol::{Api, ApiError};
use tape_protocol::api::ops::SyncReq;
use tape_store::ops::{SliceOps, SpoolOps, TrackOps};
use tape_store::types::{Pubkey, TrackInfo};
use tokio_util::sync::CancellationToken;
use tracing::warn;

use crate::config::SpoolManagerConfig;
use crate::context::NodeContext;
use crate::core::peer_call::call_peer;
use crate::features::spool::types::SyncResult;

// Purpose: Transfer slice data for a spool from its previous owner
//          after a spool reassignment.
//
// Algorithm:
// 1. Load spool state from the store. If missing, return Done.
// 2. Determine source: if no previous owner, or we are the previous
//    owner, return Done (nothing to sync).
// 3. Paginated pull from the previous owner via call_peer + api.sync:
//    - Load the sync cursor (last track we left off at).
//    - Loop:
//      a. Check cancellation.
//      b. Send SyncReq to previous owner with cursor + batch limit.
//      c. For each entry in the response:
//         - Skip if we already have the slice locally (has_slice).
//         - If we have the track metadata, validate the slice against
//           the commitment. Skip invalid entries.
//         - put_slice to store.
//      d. Advance the cursor to the last track in the batch.
//         Persist cursor so we can resume if interrupted.
//      e. Stop when the peer returns no entries and no next cursor.
// 4. Clear the sync cursor. Return Done.
//
// If the previous owner is unreachable we return after retrying.
// The FSM treats unreachable the same as Done — it moves to Scan,
// which will identify the gaps, and repair/recover will fetch from
// the rest of the spool group.

struct SyncBatch {
    next_cursor: Option<Pubkey>,
    synced: usize,
    fetched_bytes: u64,
    persisted_bytes: u64,
}

pub async fn run<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: Arc<NodeContext<Db, Cluster, Blockchain>>,
    config: &SpoolManagerConfig,
    spool: SpoolIndex,
    token: &CancellationToken,
) -> SyncResult {

    // If the spool state is missing, we don't own this spool
    let Some(state) = ctx.store.get_spool_state(spool).ok().flatten() else {
        return SyncResult::Done { synced: 0 };
    };

    // If the spool has no previous owner, then there's no peer to sync from
    let Some(prev_owner) = state.prev_owner else {
        return SyncResult::Done { synced: 0 };
    };

    // If we're the previous owner, then we can't sync from ourselves
    if prev_owner == ctx.node_id() {
        return SyncResult::Done { synced: 0 };
    }

    // Sync in batches, persisting the cursor after each batch so we can resume if interrupted.
    let mut cursor = ctx.store.get_spool_sync_cursor(spool).ok().flatten();
    let mut synced = 0;

    loop {
        if token.is_cancelled() {
            return SyncResult::Done { synced };
        }

        // Optimistically pull a batch of slices from the previous owner. If the peer is
        // unreachable, or any error occurs, we keep going, the repair/recovery will fill the gaps
        // from the rest of the group.

        match pull_batch(ctx.as_ref(), config, spool, prev_owner, cursor, token).await {
            Ok(batch) => {
                synced += batch.synced;

                if batch.fetched_bytes > 0 {
                    ctx.metrics.add_sync_fetched(batch.fetched_bytes);
                }

                if batch.persisted_bytes > 0 {
                    ctx.metrics.add_sync_persisted(batch.persisted_bytes);
                }

                match batch.next_cursor {
                    Some(c) => {
                        cursor = Some(c);
                        if let Err(error) = ctx.store.set_spool_sync_cursor(spool, c) {
                            warn!(spool, %error, "set_spool_sync_cursor failed");
                        }
                    }
                    None => {
                        if let Err(error) = ctx.store.remove_spool_sync_cursor(spool) {
                            warn!(spool, %error, "remove_spool_sync_cursor failed");
                        }
                        return SyncResult::Done { synced };
                    }
                }
            }

            Err(e) => {
                warn!(spool, %e, "pull_batch failed during sync");
                return SyncResult::Done { synced };
            }
        }
    }
}

/// Pull one page of slices from the previous owner, persist each valid entry.
/// Returns the next cursor plus fetched/persisted batch accounting.
async fn pull_batch<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: &NodeContext<Db, Cluster, Blockchain>,
    config: &SpoolManagerConfig,
    spool: SpoolIndex,
    prev_owner: NodeId,
    cursor: Option<Pubkey>,
    token: &CancellationToken,
) -> Result<SyncBatch, ApiError> {

    let mut synced = 0;
    let mut fetched_bytes = 0u64;
    let mut persisted_bytes = 0u64;

    let req = SyncReq {
        spool_index: spool,
        cursor: cursor.map(|track| track.0),
        limit: config.sync_batch_size.max(1) as u32,
    };

    let res = call_peer(
        &ctx.peer_manager,
        config.peer_retry,
        prev_owner,
        Some(token),
        || { ctx.api.sync(prev_owner, &req) },
    ).await?;

    for entry in res.entries {
        let track_addr = Pubkey(entry.track_address);
        let slice_len = entry.slice_data.len() as u64;

        fetched_bytes += slice_len;

        let Some(track_info) = ctx.store.get_track(track_addr).ok().flatten() else {
            warn!(spool, track = %track_addr, "missing track metadata for synced slice, skipping");
            continue;
        };

        if !verify_slice(spool, &track_info, &entry.slice_data) {
            warn!(spool, track = %track_addr, "skipping invalid synced slice");
            continue;
        }

        if let Err(e) = ctx.store.put_slice(spool, track_addr, entry.slice_data) {
            warn!(spool, track = %track_addr, %e, "put_slice failed, skipping");
            continue;
        }

        synced += 1;
        persisted_bytes += slice_len;
    }

    let next_cursor = res.next_cursor.map(Pubkey);

    Ok(SyncBatch {
        next_cursor,
        synced,
        fetched_bytes,
        persisted_bytes,
    })
}

fn verify_slice(spool: SpoolIndex, track_info: &TrackInfo, data: &[u8]) -> bool {
    let Some(position) = track_info.spool_group.slice_of(spool) else {
        return false;
    };

    if track_info.original_size > 0 && data.is_empty() {
        return false;
    }

    if let Some(max_len) = track_info.stripe_size.checked_mul(track_info.stripe_count) {
        if max_len > 0 && data.len() as u64 > max_len {
            return false;
        }
    }

    track_info.verify_slice(position, data)
}

#[cfg(test)]
mod tests {
    use super::*;
    use peer_memory::MemoryApi;
    use tape_core::encoding::EncodingProfile;
    use tape_core::spooler::SpoolGroup;
    use tape_core::types::EpochNumber;
    use tape_protocol::api::ops::{PeerReq, PeerRes, SyncRes};
    use tape_protocol::api::types::SyncSpoolEntry;
    use tape_slicer::{ClayCoder, ErasureCoder, SliceMetadata, Slicer};
    use tape_store::types::{SpoolState, SpoolStatus};

    use crate::context::test_utils::{test_context, test_context_with_api};

    const SPOOL: SpoolIndex = 5;
    const PEER: NodeId = NodeId(99);

    fn addr(n: u8) -> Pubkey {
        Pubkey([n; 32])
    }

    fn entry(track: Pubkey, data: &[u8]) -> SyncSpoolEntry {
        SyncSpoolEntry {
            track_address: track.0,
            slice_data: data.to_vec(),
        }
    }

    fn sync_state(epoch: EpochNumber, prev: Option<NodeId>) -> SpoolState {
        let mut state = SpoolState::new(SpoolStatus::Sync, epoch);
        state.prev_owner = prev;
        state
    }

    fn clay_slices(fill: u8, size: usize) -> Vec<Vec<u8>> {
        let profile = EncodingProfile::clay_default();
        let mut slicer = Slicer::with_profile(
            ClayCoder::from_params(profile.clay_params()),
            512,
            true,
            profile,
        );
        slicer.encode(&vec![fill; size]).unwrap()
    }

    fn clay_track(size: u64, slices: &[Vec<u8>]) -> TrackInfo {
        let profile = EncodingProfile::clay_default();
        let metadata = SliceMetadata::from_slice(&slices[0]).unwrap();
        let stripe_size = metadata.stripe_size() as u64;
        let commitment = slices
            .iter()
            .map(|slice| tape_crypto::merkle::hash_leaf(slice))
            .collect();

        TrackInfo {
            tape_address: Pubkey([0; 32]),
            spool_group: SpoolGroup::of(SPOOL),
            original_size: size,
            stripe_size,
            stripe_count: size.div_ceil(stripe_size),
            encoding_type: profile.encoding as u64,
            encoding_params: profile.params,
            commitment,
        }
    }

    fn local_slice(fill: u8, size: usize) -> (TrackInfo, Vec<u8>) {
        let slices = clay_slices(fill, size);
        let track_info = clay_track(size as u64, &slices);
        let position = track_info.spool_group.slice_of(SPOOL).unwrap();
        (track_info, slices[position].clone())
    }

    #[tokio::test]
    async fn no_prev_owner() {
        let ctx = test_context();
        ctx.store
            .set_spool_state(SPOOL, sync_state(EpochNumber(3), None))
            .unwrap();

        let result = run(ctx.clone(), &SpoolManagerConfig::default(), SPOOL, &CancellationToken::new()).await;
        assert!(matches!(result, SyncResult::Done { .. }));
    }

    #[tokio::test]
    async fn pulls_slices() {
        let a = addr(1);
        let (track_info, data) = local_slice(0xAB, 1024);
        let data_clone = data.clone();

        let ctx = test_context_with_api(MemoryApi::new(move |_, req| match req {
            PeerReq::Sync(_) => PeerRes::Sync(Ok(SyncRes {
                entries: vec![entry(a, &data_clone)],
                next_cursor: None,
            })),
            _ => panic!("unexpected request"),
        }));

        ctx.store
            .set_spool_state(SPOOL, sync_state(EpochNumber(3), Some(PEER)))
            .unwrap();
        ctx.store.put_track(a, track_info).unwrap();

        let result = run(ctx.clone(), &SpoolManagerConfig::default(), SPOOL, &CancellationToken::new()).await;

        assert!(matches!(result, SyncResult::Done { synced: 1 }));
        assert!(ctx.store.has_slice(SPOOL, a).unwrap());
        assert_eq!(ctx.store.get_slice(SPOOL, a).unwrap().unwrap(), data);
        assert!(ctx.store.get_spool_sync_cursor(SPOOL).unwrap().is_none());
    }

    #[tokio::test]
    async fn overwrites_existing() {
        let a = addr(1);
        let (track_info, synced_data) = local_slice(0xAB, 1024);
        let (_, stale_data) = local_slice(0xCD, 1024);
        let synced_data_clone = synced_data.clone();

        let ctx = test_context_with_api(MemoryApi::new(move |_, req| match req {
            PeerReq::Sync(_) => PeerRes::Sync(Ok(SyncRes {
                entries: vec![entry(a, &synced_data_clone)],
                next_cursor: None,
            })),
            _ => panic!("unexpected request"),
        }));

        ctx.store
            .set_spool_state(SPOOL, sync_state(EpochNumber(3), Some(PEER)))
            .unwrap();

        ctx.store.put_track(a, track_info).unwrap();
        ctx.store.put_slice(SPOOL, a, stale_data).unwrap();

        let result = run(ctx.clone(), &SpoolManagerConfig::default(), SPOOL, &CancellationToken::new()).await;
        assert!(matches!(result, SyncResult::Done { synced: 1 }));

        let stored = ctx.store.get_slice(SPOOL, a).unwrap().unwrap();
        assert_eq!(stored, synced_data);
    }

    #[tokio::test]
    async fn peer_unavailable() {
        let ctx = test_context(); // noop api returns errors
        ctx.store
            .set_spool_state(SPOOL, sync_state(EpochNumber(3), Some(PEER)))
            .unwrap();

        let result = run(ctx.clone(), &SpoolManagerConfig::default(), SPOOL, &CancellationToken::new()).await;
        assert_eq!(result, SyncResult::Done { synced: 0 });
    }

    #[tokio::test]
    async fn skips_missing_track() {
        let a = addr(1);
        let (_, data) = local_slice(0xAB, 1024);
        let data_clone = data.clone();

        let ctx = test_context_with_api(MemoryApi::new(move |_, req| match req {
            PeerReq::Sync(_) => PeerRes::Sync(Ok(SyncRes {
                entries: vec![entry(a, &data_clone)],
                next_cursor: None,
            })),
            _ => panic!("unexpected request"),
        }));

        ctx.store
            .set_spool_state(SPOOL, sync_state(EpochNumber(3), Some(PEER)))
            .unwrap();

        let result = run(ctx.clone(), &SpoolManagerConfig::default(), SPOOL, &CancellationToken::new()).await;

        assert_eq!(result, SyncResult::Done { synced: 0 });
        assert!(!ctx.store.has_slice(SPOOL, a).unwrap());
        assert!(ctx.store.get_spool_sync_cursor(SPOOL).unwrap().is_none());
    }

    #[tokio::test]
    async fn skips_invalid_data() {
        let a = addr(1);
        let (track_info, _) = local_slice(0xAB, 1024);
        let invalid_data = vec![0xEE; 64];
        let invalid_data_clone = invalid_data.clone();

        let ctx = test_context_with_api(MemoryApi::new(move |_, req| match req {
            PeerReq::Sync(_) => PeerRes::Sync(Ok(SyncRes {
                entries: vec![entry(a, &invalid_data_clone)],
                next_cursor: None,
            })),
            _ => panic!("unexpected request"),
        }));

        ctx.store
            .set_spool_state(SPOOL, sync_state(EpochNumber(3), Some(PEER)))
            .unwrap();
        ctx.store.put_track(a, track_info).unwrap();

        let result = run(ctx.clone(), &SpoolManagerConfig::default(), SPOOL, &CancellationToken::new()).await;

        assert_eq!(result, SyncResult::Done { synced: 0 });
        assert!(!ctx.store.has_slice(SPOOL, a).unwrap());
        assert!(ctx.store.get_spool_sync_cursor(SPOOL).unwrap().is_none());
    }

    #[tokio::test]
    async fn resumes_cursor() {
        let a1 = addr(1);
        let a2 = addr(2);
        let (track_info1, slice1) = local_slice(0x11, 1024);
        let (track_info2, slice2) = local_slice(0x22, 1024);
        let call_count = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));
        let counter = call_count.clone();

        let ctx = test_context_with_api(MemoryApi::new(move |_, req| match req {
            PeerReq::Sync(_) => {
                let n = counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                if n == 0 {
                    PeerRes::Sync(Ok(SyncRes {
                        entries: vec![entry(a1, &slice1)],
                        next_cursor: Some(a2.0),
                    }))
                } else {
                    PeerRes::Sync(Ok(SyncRes {
                        entries: vec![entry(a2, &slice2)],
                        next_cursor: None,
                    }))
                }
            }
            _ => panic!("unexpected request"),
        }));

        ctx.store
            .set_spool_state(SPOOL, sync_state(EpochNumber(3), Some(PEER)))
            .unwrap();
        ctx.store.put_track(a1, track_info1).unwrap();
        ctx.store.put_track(a2, track_info2).unwrap();

        let result = run(ctx.clone(), &SpoolManagerConfig::default(), SPOOL, &CancellationToken::new()).await;
        assert!(matches!(result, SyncResult::Done { .. }));
        assert!(ctx.store.has_slice(SPOOL, a1).unwrap());
        assert!(ctx.store.has_slice(SPOOL, a2).unwrap());
        assert_eq!(call_count.load(std::sync::atomic::Ordering::Relaxed), 2);
    }
}
