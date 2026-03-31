use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use tracing::warn;

use rpc::Rpc;
use store::Store;
use tape_core::track::data::TrackData;
use tape_core::track::types::CompressedTrack;
use tape_core::spooler::{SpoolGroup, SpoolIndex};
use tape_core::types::NodeId;
use tape_protocol::{Api, ApiError};
use tape_protocol::api::ops::{GetTrackDataReq, SyncSlicesReq};
use tape_store::ops::{SliceOps, SpoolOps, TrackDataOps, TrackOps};
use tape_store::types::{BlobInfo, Pubkey};
use tape_retry::RetryConfig;

use crate::config::recovery::RecoveryConfig;
use crate::context::NodeContext;
use crate::core::peer_call::call_peer;
use crate::features::spool::types::SyncResult;

// Purpose: Transfer slice data for a spool from its previous owner
//          after a spool reassignment.
//
// Algorithm:
// 1. Load spool state from the store. If missing, return Done.
// 2. Sync track_data from any peer in the spool group.
// 3. Determine slice source: if no previous owner, or we are the previous
//    owner, skip slice sync.
// 4. Paginated pull from the previous owner via call_peer + api.sync_slices:
//    - Load the sync cursor (last track we left off at).
//    - Loop:
//      a. Check cancellation.
//      b. Send SyncSlicesReq to previous owner with cursor + batch limit.
//      c. For each entry in the response:
//         - Skip if we already have the slice locally (has_slice).
//         - If we have the track metadata, validate the slice against
//           the commitment. Skip invalid entries.
//         - put_slice to store.
//      d. Advance the cursor to the last track in the batch.
//         Persist cursor so we can resume if interrupted.
//      e. Stop when the peer returns no entries and no next cursor.
// 5. Clear the sync cursor. Return Done.
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

pub async fn run<Db: Store, Cluster: Api + 'static, Blockchain: Rpc>(
    ctx: Arc<NodeContext<Db, Cluster, Blockchain>>,
    config: &RecoveryConfig,
    spool: SpoolIndex,
    token: &CancellationToken,
) -> SyncResult {

    // If the spool state is missing, we don't own this spool
    let state = match ctx.store.get_spool_state(spool) {
        Ok(Some(state)) => state,
        Ok(None) => {
            return SyncResult::Done {
                synced_tracks: 0,
                synced_slices: 0,
            };
        }
        Err(error) => {
            warn!(spool, %error, "failed to read spool state");
            return SyncResult::Done {
                synced_tracks: 0,
                synced_slices: 0,
            };
        }
    };

    // Sync track data first, since it's needed to validate slices. We can sync track data from any
    // peer in the spool group, not just the previous owner, which increases our chances of success
    // if the previous owner is unavailable.
    let synced_tracks = sync_track_data(
        ctx.clone(), 
        spool, 
        config.sync_batch.max(1), 
        token)
    .await;

    // If the spool has no previous owner, then there's no slice peer to sync from.
    let Some(prev_owner) = state.prev_owner else {
        return SyncResult::Done {
            synced_tracks,
            synced_slices: 0,
        };
    };

    // If we're the previous owner, then we can't sync slices from ourselves.
    if prev_owner == ctx.node_id() {
        return SyncResult::Done {
            synced_tracks,
            synced_slices: 0,
        };
    }

    // Sync in batches, persisting the cursor after each batch so we can resume if interrupted.
    let mut cursor = match ctx.store.get_spool_sync_cursor(spool) {
        Ok(cursor) => cursor,
        Err(error) => {
            warn!(spool, %error, "failed to read sync cursor");
            None
        }
    };
    let mut synced_slices = 0;

    loop {
        if token.is_cancelled() {
            return SyncResult::Done {
                synced_tracks,
                synced_slices,
            };
        }

        // Optimistically pull a batch of slices from the previous owner. If the peer is
        // unreachable, or any error occurs, we keep going, the repair/recovery will fill the gaps
        // from the rest of the group.

        match pull_batch(ctx.as_ref(), config, spool, prev_owner, cursor, token).await {
            Ok(batch) => {
                synced_slices += batch.synced;

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
                        return SyncResult::Done {
                            synced_tracks,
                            synced_slices,
                        };
                    }
                }
            }

            Err(e) => {
                warn!(spool, %e, "pull_batch failed during sync");
                return SyncResult::Done {
                    synced_tracks,
                    synced_slices,
                };
            }
        }
    }
}

/// Pull one page of slices from the previous owner, persist each valid entry.
/// Returns the next cursor plus fetched/persisted batch accounting.
async fn pull_batch<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: &NodeContext<Db, Cluster, Blockchain>,
    config: &RecoveryConfig,
    spool: SpoolIndex,
    prev_owner: NodeId,
    cursor: Option<Pubkey>,
    token: &CancellationToken,
) -> Result<SyncBatch, ApiError> {

    let mut synced = 0;
    let mut fetched_bytes = 0u64;
    let mut persisted_bytes = 0u64;

    let req = SyncSlicesReq {
        spool_index: spool,
        cursor: cursor.map(|track| track.0),
        limit: config.sync_batch.max(1) as u32,
    };

    let res = call_peer(
        &ctx.peer_manager,
        RetryConfig::ten(),
        prev_owner,
        Some(token),
        || { ctx.api.sync_slices(prev_owner, &req) },
    ).await?;

    for entry in res.entries {
        let track_addr = Pubkey(entry.track_address);
        let slice_len = entry.slice_data.len() as u64;

        fetched_bytes += slice_len;

        let track_info = match ctx.store.get_track(track_addr) {
            Ok(Some(info)) => info,
            Ok(None) => {
                warn!(spool, track = %track_addr, "missing track metadata for synced slice, skipping");
                continue;
            }
            Err(error) => {
                warn!(spool, track = %track_addr, %error, "failed to read track metadata, skipping");
                continue;
            }
        };

        if !track_info.is_blob() {
            warn!(spool, track = %track_addr, "received synced slice for raw track, skipping");
            continue;
        }

        let track_data = match ctx.store.get_track_data(track_addr) {
            Ok(Some(TrackData::Blob(data))) => data,
            Ok(Some(_)) => {
                warn!(spool, track = %track_addr, "track data is not a blob, skipping");
                continue;
            }
            Ok(None) => {
                warn!(spool, track = %track_addr, "missing blob track data for synced slice, skipping");
                continue;
            }
            Err(error) => {
                warn!(spool, track = %track_addr, %error, "failed to read track data, skipping");
                continue;
            }
        };

        if !verify_slice(spool, &track_info, &track_data, &entry.slice_data) {
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

fn verify_slice(
    spool: SpoolIndex,
    track_info: &CompressedTrack,
    track_data: &BlobInfo,
    data: &[u8],
) -> bool {
    let Some(position) = track_info.spool_group.slice_of(spool) else {
        return false;
    };

    if track_data.size.0 > 0 && data.is_empty() {
        return false;
    }

    if let Some(max_len) = track_data.stripe_size.checked_mul(track_data.stripe_count) {
        if max_len > 0 && data.len() as u64 > max_len {
            return false;
        }
    }

    track_data.verify_slice(position, data)
}

pub async fn sync_track_data<Db: Store, Cluster: Api + 'static, Blockchain: Rpc>(
    ctx: Arc<NodeContext<Db, Cluster, Blockchain>>,
    spool: SpoolIndex,
    batch_size: usize,
    token: &CancellationToken,
) -> usize {
    let mut cursor = None;
    let mut synced = 0usize;
    let peers = track_data_peers(ctx.as_ref(), spool);

    loop {
        if token.is_cancelled() {
            break;
        }

        let tracks = match ctx.store.iter_tracks_from(cursor, batch_size.max(1)) {
            Ok(tracks) => tracks,
            Err(error) => {
                warn!(spool, %error, "iter_tracks_from failed during track-data sync");
                break;
            }
        };

        if tracks.is_empty() {
            break;
        }

        for (track_addr, track) in &tracks {
            if token.is_cancelled() {
                break;
            }

            if !track.spool_group.contains(spool) {
                continue;
            }

            match ctx.store.has_track_data(*track_addr) {
                Ok(true) => continue,
                Ok(false) => {}
                Err(error) => {
                    warn!(spool, track = %track_addr, %error, "has_track_data failed");
                    continue;
                }
            }

            match fetch_track_data_from_group(ctx.as_ref(), *track_addr, &peers, token).await {
                Ok(data) => {
                    if let Err(error) = ctx.store.put_track_data(*track_addr, data) {
                        warn!(spool, track = %track_addr, %error, "put_track_data failed");
                        continue;
                    }
                    synced += 1;
                }
                Err(()) => {
                    warn!(spool, track = %track_addr, "failed to fetch track data from group");
                }
            }
        }

        cursor = tracks.last().map(|(track_addr, _)| *track_addr);
    }

    synced
}

fn track_data_peers<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: &NodeContext<Db, Cluster, Blockchain>,
    spool: SpoolIndex,
) -> Vec<NodeId> {
    let group = SpoolGroup::of(spool);
    let mut peers = Vec::new();

    for (peer_spool, node_id) in ctx.state().group_peers(group) {
        if peer_spool == spool || node_id == ctx.node_id() || peers.contains(&node_id) {
            continue;
        }
        peers.push(node_id);
    }

    peers
}

async fn fetch_track_data_from_group<Db: Store, Cluster: Api + 'static, Blockchain: Rpc>(
    ctx: &NodeContext<Db, Cluster, Blockchain>,
    track: Pubkey,
    peers: &[NodeId],
    token: &CancellationToken,
) -> Result<TrackData, ()> {
    for &node_id in peers {
        let req = GetTrackDataReq { track: track.into() };

        match call_peer(
            &ctx.peer_manager,
            RetryConfig::three(),
            node_id,
            Some(token),
            || ctx.api.get_track_data(node_id, &req),
        )
        .await
        {
            Ok(res) => return Ok(res.data),
            Err(_) => continue,
        }
    }

    Err(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use peer_memory::MemoryApi;
    use tape_core::encoding::EncodingProfile;
    use tape_core::spooler::SpoolGroup;
    use tape_core::track::types::{CompressedTrack, TrackKind, TrackState};
    use tape_core::types::{EpochNumber, StorageUnits, TrackNumber};
    use tape_crypto::Hash;
    use tape_protocol::api::ops::{PeerReq, PeerRes, SyncSlicesRes};
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

    fn clay_track(size: u64, slices: &[Vec<u8>]) -> CompressedTrack {
        let profile = EncodingProfile::clay_default();
        let metadata = SliceMetadata::from_slice(&slices[0]).unwrap();
        let stripe_size = metadata.stripe_size() as u64;
        let _commitment: Vec<_> = slices
            .iter()
            .map(|slice| tape_crypto::merkle::hash_leaf(slice))
            .collect();
        let _stripe_count = size.div_ceil(stripe_size);
        let _ = profile;

        CompressedTrack {
            tape: Pubkey([0; 32]),
            key: Hash::new_unique(),
            track_number: TrackNumber(0),
            kind: TrackKind::Blob as u64,
            state: TrackState::Certified as u64,
            size: StorageUnits::from_bytes(size),
            spool_group: SpoolGroup::of(SPOOL),
            value_hash: Hash::new_unique(),
        }
    }

    fn local_slice(fill: u8, size: usize) -> (CompressedTrack, Vec<u8>) {
        let slices = clay_slices(fill, size);
        let track_info = clay_track(size as u64, &slices);
        let position = track_info.spool_group.slice_of(SPOOL).unwrap() as usize;
        (track_info, slices[position].clone())
    }

    #[tokio::test]
    async fn no_prev_owner() {
        let ctx = test_context();
        ctx.store
            .set_spool_state(SPOOL, sync_state(EpochNumber(3), None))
            .unwrap();

        let result = run(ctx.clone(), &RecoveryConfig::default(), SPOOL, &CancellationToken::new()).await;
        assert!(matches!(result, SyncResult::Done { .. }));
    }

    #[tokio::test]
    async fn pulls_slices() {
        let a = addr(1);
        let (track_info, data) = local_slice(0xAB, 1024);
        let data_clone = data.clone();

        let ctx = test_context_with_api(MemoryApi::new(move |_, req| match req {
            PeerReq::SyncSlices(_) => PeerRes::SyncSlices(Ok(SyncSlicesRes {
                entries: vec![entry(a, &data_clone)],
                next_cursor: None,
            })),
            _ => panic!("unexpected request"),
        }));

        ctx.store
            .set_spool_state(SPOOL, sync_state(EpochNumber(3), Some(PEER)))
            .unwrap();
        ctx.store.put_track(a, track_info).unwrap();

        let result = run(ctx.clone(), &RecoveryConfig::default(), SPOOL, &CancellationToken::new()).await;

        assert!(matches!(
            result,
            SyncResult::Done {
                synced_tracks: 0,
                synced_slices: 1,
            }
        ));
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
            PeerReq::SyncSlices(_) => PeerRes::SyncSlices(Ok(SyncSlicesRes {
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

        let result = run(ctx.clone(), &RecoveryConfig::default(), SPOOL, &CancellationToken::new()).await;
        assert!(matches!(
            result,
            SyncResult::Done {
                synced_tracks: 0,
                synced_slices: 1,
            }
        ));

        let stored = ctx.store.get_slice(SPOOL, a).unwrap().unwrap();
        assert_eq!(stored, synced_data);
    }

    #[tokio::test]
    async fn peer_unavailable() {
        let ctx = test_context(); // noop api returns errors
        ctx.store
            .set_spool_state(SPOOL, sync_state(EpochNumber(3), Some(PEER)))
            .unwrap();

        let result = run(ctx.clone(), &RecoveryConfig::default(), SPOOL, &CancellationToken::new()).await;
        assert_eq!(
            result,
            SyncResult::Done {
                synced_tracks: 0,
                synced_slices: 0,
            }
        );
    }

    #[tokio::test]
    async fn skips_missing_track() {
        let a = addr(1);
        let (_, data) = local_slice(0xAB, 1024);
        let data_clone = data.clone();

        let ctx = test_context_with_api(MemoryApi::new(move |_, req| match req {
            PeerReq::SyncSlices(_) => PeerRes::SyncSlices(Ok(SyncSlicesRes {
                entries: vec![entry(a, &data_clone)],
                next_cursor: None,
            })),
            _ => panic!("unexpected request"),
        }));

        ctx.store
            .set_spool_state(SPOOL, sync_state(EpochNumber(3), Some(PEER)))
            .unwrap();

        let result = run(ctx.clone(), &RecoveryConfig::default(), SPOOL, &CancellationToken::new()).await;

        assert_eq!(
            result,
            SyncResult::Done {
                synced_tracks: 0,
                synced_slices: 0,
            }
        );
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
            PeerReq::SyncSlices(_) => PeerRes::SyncSlices(Ok(SyncSlicesRes {
                entries: vec![entry(a, &invalid_data_clone)],
                next_cursor: None,
            })),
            _ => panic!("unexpected request"),
        }));

        ctx.store
            .set_spool_state(SPOOL, sync_state(EpochNumber(3), Some(PEER)))
            .unwrap();
        ctx.store.put_track(a, track_info).unwrap();

        let result = run(ctx.clone(), &RecoveryConfig::default(), SPOOL, &CancellationToken::new()).await;

        assert_eq!(
            result,
            SyncResult::Done {
                synced_tracks: 0,
                synced_slices: 0,
            }
        );
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
            PeerReq::SyncSlices(_) => {
                let n = counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                if n == 0 {
                    PeerRes::SyncSlices(Ok(SyncSlicesRes {
                        entries: vec![entry(a1, &slice1)],
                        next_cursor: Some(a2.0),
                    }))
                } else {
                    PeerRes::SyncSlices(Ok(SyncSlicesRes {
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

        let result = run(ctx.clone(), &RecoveryConfig::default(), SPOOL, &CancellationToken::new()).await;
        assert!(matches!(result, SyncResult::Done { .. }));
        assert!(ctx.store.has_slice(SPOOL, a1).unwrap());
        assert!(ctx.store.has_slice(SPOOL, a2).unwrap());
        assert_eq!(call_count.load(std::sync::atomic::Ordering::Relaxed), 2);
    }
}
