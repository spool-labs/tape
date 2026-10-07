use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use tracing::warn;

use tape_rpc::Rpc;
use tape_store_api::Store;
use tape_core::erasure::leaf_position;
use tape_core::track::data::BlobData;
use tape_core::spooler::{GroupFold, GroupIndex, VirtualGroup};
use tape_core::system::SpoolStatus;
use tape_core::types::{EpochNumber, SpoolIndex};
use tape_core::types::StorageUnits;
use tape_core::track::blob::BlobEncoding;
use tape_crypto::address::Address;
use tape_protocol::{Api, ApiError};
use tape_protocol::api::ops::{GetTrackDataReq, SyncSlicesReq};
use tape_store::ops::{SliceOps, TrackDataOps, TrackOps};
use tape_retry::RetryConfig;

use crate::config::recovery::RecoveryConfig;
use crate::context::NodeContext;
use crate::core::peer_call::call_peer;
use crate::features::spool::manager::carried_rows;
use crate::features::spool::types::SyncResult;

// Purpose: Transfer slice data for a live spool from its previous owner
//          after a spool reassignment. One run covers every stored spool
//          the live spool carries.
//
// Algorithm:
// 1. Load the rows waiting to sync under the live spool. If none, return Done.
// 2. Sync track_data from any peer in the spool group.
// 3. Determine slice sources: the previous owners in those rows, leaving
//    out this node. There is one, or one per retired group in the epoch
//    after the group count drops.
// 4. Paginated pull from each previous owner via call_peer + api.sync_slices:
//    - Loop:
//      a. Check cancellation.
//      b. Send SyncSlicesReq to previous owner with cursor + batch limit.
//      c. For each entry in the response:
//         - Skip one whose track belongs to another live group.
//         - Validate the slice against the commitment. Skip invalid entries.
//         - put_slice to store, under the track's own stored spool.
//      d. Carry the peer's cursor into the next request.
//      e. Stop when the peer returns no next cursor.
// 5. Return Done.
//
// If a previous owner is unreachable we give up on it after retrying.
// The FSM treats unreachable the same as Done, it moves to Scan,
// which will identify the gaps, and repair/recover will fetch from
// the rest of the spool group.

struct SyncBatch {
    next_cursor: Option<Vec<u8>>,
    synced: usize,
    fetched_bytes: u64,
    persisted_bytes: u64,
}

pub async fn run<Db: Store, Cluster: Api + 'static, Blockchain: Rpc>(
    ctx: Arc<NodeContext<Db, Cluster, Blockchain>>,
    config: &RecoveryConfig,
    spool: SpoolIndex,
    epoch: EpochNumber,
    token: &CancellationToken,
) -> SyncResult {

    let nothing_synced = SyncResult::Done {
        synced_tracks: 0,
        synced_slices: 0,
    };

    // The spool is a live one. The fold of the epoch the rows were written for
    // says which stored spools it carries.
    let Some(fold) = ctx.state().fold_at(epoch) else {
        warn!(spool = %spool, epoch = epoch.0, "no fold for the epoch");
        return nothing_synced;
    };

    // If no carried row waits to sync, we don't own this spool
    let rows = match carried_rows(&ctx.store, fold, spool, epoch, SpoolStatus::Sync) {
        Ok(rows) => rows,
        Err(error) => {
            warn!(spool = %spool, %error, "failed to read spool state");
            return nothing_synced;
        }
    };
    if rows.is_empty() {
        return nothing_synced;
    }

    // Sync track data first, since it's needed to validate slices. We can sync track data from any
    // peer in the spool group, not just the previous owner, which increases our chances of success
    // if the previous owner is unavailable.
    let synced_tracks = sync_track_data(
        ctx.clone(),
        spool,
        fold,
        config.sync_batch.max(1),
        token)
    .await;

    // The stored spools under a live spool share one previous owner, except in
    // the epoch after the group count drops, when each retired group had its
    // own. A row with no previous owner, or with this node as the owner, has no
    // slice peer to sync from.
    let mut prev_owners: Vec<Address> = Vec::new();
    for (_, state) in &rows {
        let Some(prev_owner) = state.prev_owner else {
            continue;
        };

        if prev_owner != ctx.node_address() && !prev_owners.contains(&prev_owner) {
            prev_owners.push(prev_owner);
        }
    }

    let mut synced_slices = 0;

    for prev_owner in prev_owners {
        let mut cursor: Option<Vec<u8>> = None;

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

            match pull_batch(ctx.as_ref(), config, spool, fold, prev_owner, cursor, token).await {
                Ok(batch) => {
                    synced_slices += batch.synced;

                    if batch.fetched_bytes > 0 {
                        ctx.metrics.add_sync_fetched(batch.fetched_bytes);
                    }

                    if batch.persisted_bytes > 0 {
                        ctx.metrics.add_sync_persisted(batch.persisted_bytes);
                    }

                    match batch.next_cursor {
                        Some(next) => cursor = Some(next),
                        None => break,
                    }
                }

                Err(e) => {
                    warn!(spool = %spool, peer = %prev_owner, %e, "pull_batch failed during sync");
                    break;
                }
            }
        }
    }

    SyncResult::Done {
        synced_tracks,
        synced_slices,
    }
}

/// Pull one page of slices from the previous owner, persist each valid entry.
/// Returns the next cursor plus fetched/persisted batch accounting.
async fn pull_batch<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: &NodeContext<Db, Cluster, Blockchain>,
    config: &RecoveryConfig,
    spool: SpoolIndex,
    fold: GroupFold,
    prev_owner: Address,
    cursor: Option<Vec<u8>>,
    token: &CancellationToken,
) -> Result<SyncBatch, ApiError> {

    let mut synced = 0;
    let mut fetched_bytes = 0u64;
    let mut persisted_bytes = 0u64;

    let group = GroupIndex::containing(spool);
    let position = leaf_position(spool).as_usize();

    let req = SyncSlicesReq {
        spool_index: spool,
        live_groups: fold.live(),
        cursor: cursor.clone(),
        limit: config.sync_batch.max(1) as u32,
    };

    let res = call_peer(
        &ctx.peer_manager,
        RetryConfig::ten(),
        prev_owner,
        Some(token),
        || { ctx.api.sync_slices(prev_owner, &req) },
    ).await?;

    // What the batch needs read, read once each rather than twice per entry: the
    // peer answered every slice in one call and the local halves follow suit.
    let mut addresses: Vec<Address> = Vec::with_capacity(res.entries.len());
    for entry in &res.entries {
        addresses.push(Address::new(entry.track_address));
    }
    // A read that fails for the batch fails for every entry in it, so the page is
    // given up on and the cursor still advances: the peer will be asked again.
    let track_infos = match ctx.store.get_tracks(&addresses) {
        Ok(infos) => infos,
        Err(error) => {
            warn!(spool = %spool, %error, "failed to read track metadata, skipping batch");
            return Ok(SyncBatch {
                next_cursor: res.next_cursor.clone(),
                synced: 0,
                fetched_bytes: 0,
                persisted_bytes: 0,
            });
        }
    };
    let track_datas = match ctx.store.get_track_datas(&addresses) {
        Ok(datas) => datas,
        Err(error) => {
            warn!(spool = %spool, %error, "failed to read track data, skipping batch");
            return Ok(SyncBatch {
                next_cursor: res.next_cursor.clone(),
                synced: 0,
                fetched_bytes: 0,
                persisted_bytes: 0,
            });
        }
    };

    for ((entry, info), data) in res.entries.into_iter().zip(track_infos).zip(track_datas) {
        let track_addr = Address::new(entry.track_address);
        let slice_len = entry.slice_data.len() as u64;

        fetched_bytes += slice_len;

        let Some(track_info) = info else {
            warn!(spool = %spool, track = %track_addr, "missing track metadata for synced slice, skipping");
            continue;
        };

        if !track_info.is_coded() {
            warn!(spool = %spool, track = %track_addr, "received synced slice for raw track, skipping");
            continue;
        }

        // A slice is kept under its track's own group, and only one this live
        // spool carries has a row here.
        if fold.live_group(track_info.group) != group {
            warn!(spool = %spool, track = %track_addr, "received synced slice for another group, skipping");
            continue;
        }
        let stored = track_info.group.spool_at(position);

        let track_data = match data {
            Some(BlobData::Coded(data)) => data,
            Some(_) => {
                warn!(spool = %spool, track = %track_addr, "track data is not a blob, skipping");
                continue;
            }
            None => {
                warn!(spool = %spool, track = %track_addr, "missing blob track data for synced slice, skipping");
                continue;
            }
        };

        if !verify_slice(position, &track_data, &entry.slice_data) {
            warn!(spool = %stored, track = %track_addr, "skipping invalid synced slice");
            continue;
        }

        if let Err(e) = ctx.store.put_slice(stored, track_addr, entry.slice_data) {
            warn!(spool = %stored, track = %track_addr, %e, "put_slice failed, skipping");
            continue;
        }

        synced += 1;
        persisted_bytes += slice_len;
    }

    let next_cursor = res.next_cursor;

    Ok(SyncBatch {
        next_cursor,
        synced,
        fetched_bytes,
        persisted_bytes,
    })
}

fn verify_slice(
    position: usize,
    track_data: &BlobEncoding,
    data: &[u8],
) -> bool {
    if track_data.size.0 > 0 && data.is_empty() {
        return false;
    }

    if let Some(max_len) = track_data
        .stripe_size
        .checked_mul(StorageUnits::from_bytes(track_data.stripe_count.as_u64()))
    {
        if max_len > StorageUnits::zero() && data.len() as u64 > max_len.as_u64() {
            return false;
        }
    }

    track_data.verify_slice(SpoolIndex::from(position as u64), data)
}

pub async fn sync_track_data<Db: Store, Cluster: Api + 'static, Blockchain: Rpc>(
    ctx: Arc<NodeContext<Db, Cluster, Blockchain>>,
    spool: SpoolIndex,
    fold: GroupFold,
    batch_size: usize,
    token: &CancellationToken,
) -> usize {
    let mut cursor: Option<Vec<u8>> = None;
    let mut synced = 0usize;
    let group = GroupIndex::containing(spool);

    loop {
        if token.is_cancelled() {
            break;
        }

        let (tracks, next) = match ctx.store.sweep_tracks(cursor.as_deref(), batch_size.max(1)) {
            Ok(swept) => swept,
            Err(error) => {
                warn!(spool = %spool, %error, "sweep_tracks failed during track-data sync");
                break;
            }
        };

        for (track_addr, track) in &tracks {
            if token.is_cancelled() {
                break;
            }

            if fold.live_group(track.group) != group {
                continue;
            }

            match ctx.store.has_track_data(*track_addr) {
                Ok(true) => continue,
                Ok(false) => {}
                Err(error) => {
                    warn!(spool = %spool, track = %track_addr, %error, "has_track_data failed");
                    continue;
                }
            }

            let peers = track_data_peers(ctx.as_ref(), track.group);

            match fetch_track_data_from_group(ctx.as_ref(), *track_addr, &peers, token).await {
                Ok(data) => {
                    if let Err(error) = ctx.store.put_track_data(*track_addr, data) {
                        warn!(spool = %spool, track = %track_addr, %error, "put_track_data failed");
                        continue;
                    }
                    synced += 1;
                }
                Err(()) => {
                    warn!(spool = %spool, track = %track_addr, "failed to fetch track data from group");
                }
            }
        }

        match next {
            Some(next) => cursor = Some(next),
            None => break,
        }
    }

    synced
}

fn track_data_peers<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: &NodeContext<Db, Cluster, Blockchain>,
    group: VirtualGroup,
) -> Vec<Address> {
    let state = ctx.state();
    let mut peers = Vec::new();

    // Last epoch's holders come first. After the group count drops they are
    // the only nodes that hold a retired group's inline payloads.
    let holders = state
        .group_peers_prev(group)
        .into_iter()
        .chain(state.group_peers(group));

    for (_, node_id) in holders {
        if node_id == ctx.node_address() || peers.contains(&node_id) {
            continue;
        }
        peers.push(node_id);
    }

    peers
}

async fn fetch_track_data_from_group<Db: Store, Cluster: Api + 'static, Blockchain: Rpc>(
    ctx: &NodeContext<Db, Cluster, Blockchain>,
    track: Address,
    peers: &[Address],
    token: &CancellationToken,
) -> Result<BlobData, ()> {
    for &node_id in peers {
        let req = GetTrackDataReq { track };

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
    use peer_memory::MemoryApi;
    use tape_core::encoding::TAPENET_PROFILE;
    use std::sync::Mutex;

    use tape_core::erasure::{SLICE_TREE_HEIGHT, slice_root};
    use tape_core::spooler::VirtualGroup;
    use tape_core::track::types::{CompressedTrack, TrackFlags, TrackKind, TrackState};
    use tape_core::types::{StorageUnits, StripeCount, TapeNumber, TrackNumber};
    use tape_crypto::address::Address;
    use tape_crypto::Hash;
    use tape_crypto::merkle::root_from_leaf_hashes;
    use tape_protocol::api::ops::{PeerReq, PeerRes, SyncSlicesRes};
    use tape_protocol::api::types::SyncSliceEntry;
    use tape_slicer::{ErasureCoder, SliceMetadata, Slicer};
    use tape_core::system::{SpoolState, SpoolStatus};
    use tape_store::ops::SpoolOps;

    use super::*;
    use crate::harness::{NodeHarness, TestContext};

    const SPOOL: SpoolIndex = SpoolIndex(5);

    const WIDTH: usize = TAPENET_PROFILE.n() as usize;

    fn addr(n: u8) -> Address {
        Address::from([n; 32])
    }

    fn peer() -> Address {
        addr(99)
    }

    async fn test_context() -> TestContext {
        test_context_with_api(MemoryApi::noop()).await
    }

    async fn test_context_with_api(api: MemoryApi) -> TestContext {
        context_with_groups(api, 1).await
    }

    async fn context_with_groups(api: MemoryApi, live_groups: u64) -> TestContext {
        NodeHarness::builder()
            .nodes(live_groups as usize * WIDTH + 5)
            .current_group_count(live_groups)
            .no_prev_snapshot_tape()
            .api(api)
            .build()
            .await
            .expect("build harness")
            .ctx_for(SPOOL.as_usize())
    }

    async fn sync(ctx: &TestContext) -> SyncResult {
        let epoch = ctx.state().epoch();
        run(ctx.clone(), &RecoveryConfig::default(), SPOOL, epoch, &CancellationToken::new()).await
    }

    fn carried_spool(group: VirtualGroup) -> SpoolIndex {
        group.spool_at(SPOOL.as_usize())
    }

    fn entry(track: Address, data: &[u8]) -> SyncSliceEntry {
        SyncSliceEntry {
            track_address: track.to_bytes(),
            slice_data: data.to_vec(),
        }
    }

    fn sync_state(epoch: EpochNumber, prev: Option<Address>) -> SpoolState {
        let mut state = SpoolState::new(SpoolStatus::Sync, epoch);
        state.prev_owner = prev;
        state
    }

    fn clay_slices(fill: u8, size: usize) -> Vec<Vec<u8>> {
        let mut slicer = Slicer::try_from_profile(TAPENET_PROFILE).unwrap();
        slicer.encode(&vec![fill; size]).unwrap()
    }

    fn clay_blob(size: u64, slices: &[Vec<u8>]) -> BlobEncoding {
        let metadata = SliceMetadata::parse(&slices[0]).unwrap();
        let stripe_size = metadata.stripe_size() as u64;
        let leaves = core::array::from_fn(|index| slice_root(&slices[index]).expect("slice within capacity"));
        let commitment = root_from_leaf_hashes::<SLICE_TREE_HEIGHT>(&leaves);

        BlobEncoding {
            size: StorageUnits::from_bytes(size),
            commitment,
            stripe_size: StorageUnits::from_bytes(stripe_size),
            stripe_count: StripeCount(size.div_ceil(stripe_size)),
            leaves,
        }
    }

    fn clay_track(size: u64, slices: &[Vec<u8>]) -> CompressedTrack {
        let blob = clay_blob(size, slices);

        CompressedTrack {
            tape: Address::from([0; 32]),
            tape_number: TapeNumber(1),
            key: Hash::new_unique(),
            track_number: TrackNumber(0),
            kind: TrackKind::Coded as u64,
            state: TrackState::Certified as u64,
            flags: TrackFlags::NONE,
            size: StorageUnits::from_bytes(size),
            group: VirtualGroup::containing(SPOOL),
            value_hash: blob.get_hash(),
        }
    }

    fn local_slice(fill: u8, size: usize) -> (CompressedTrack, BlobEncoding, Vec<u8>) {
        let slices = clay_slices(fill, size);
        let track_info = clay_track(size as u64, &slices);
        let blob = clay_blob(size as u64, &slices);
        let position = track_info.group.position_of(SPOOL).unwrap() as usize;
        (track_info, blob, slices[position].clone())
    }

    #[tokio::test]
    async fn no_prev_owner() {
        let ctx = test_context().await;
        ctx.store
            .set_spool_state(SPOOL, sync_state(ctx.state().epoch(),None))
            .unwrap();

        let result = sync(&ctx).await;
        assert!(matches!(result, SyncResult::Done { .. }));
    }

    #[tokio::test]
    async fn pulls_slices() {
        let a = addr(1);
        let (track_info, track_blob, data) = local_slice(0xAB, 1024);
        let data_clone = data.clone();

        let ctx = test_context_with_api(MemoryApi::new(move |_, req| match req {
            PeerReq::SyncSlices(_) => PeerRes::SyncSlices(Ok(SyncSlicesRes {
                entries: vec![entry(a, &data_clone)],
                next_cursor: None,
            })),
            _ => panic!("unexpected request"),
        }))
        .await;

        ctx.store
            .set_spool_state(SPOOL, sync_state(ctx.state().epoch(),Some(peer())))
            .unwrap();
        ctx.store.put_track(a, track_info).unwrap();
        ctx.store.put_track_data(a, BlobData::Coded(track_blob)).unwrap();

        let result = sync(&ctx).await;

        assert!(matches!(
            result,
            SyncResult::Done {
                synced_tracks: 0,
                synced_slices: 1,
            }
        ));
        assert!(ctx.store.has_slice(SPOOL, a).unwrap());
        assert_eq!(ctx.store.get_slice(SPOOL, a).unwrap().unwrap(), data);
    }

    #[tokio::test]
    async fn overwrites_existing() {
        let a = addr(1);
        let (track_info, track_blob, synced_data) = local_slice(0xAB, 1024);
        let (_, _, stale_data) = local_slice(0xCD, 1024);
        let synced_data_clone = synced_data.clone();

        let ctx = test_context_with_api(MemoryApi::new(move |_, req| match req {
            PeerReq::SyncSlices(_) => PeerRes::SyncSlices(Ok(SyncSlicesRes {
                entries: vec![entry(a, &synced_data_clone)],
                next_cursor: None,
            })),
            _ => panic!("unexpected request"),
        }))
        .await;

        ctx.store
            .set_spool_state(SPOOL, sync_state(ctx.state().epoch(),Some(peer())))
            .unwrap();

        ctx.store.put_track(a, track_info).unwrap();
        ctx.store.put_track_data(a, BlobData::Coded(track_blob)).unwrap();
        ctx.store.put_slice(SPOOL, a, stale_data).unwrap();

        let result = sync(&ctx).await;
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
        let ctx = test_context().await; // noop api returns errors
        ctx.store
            .set_spool_state(SPOOL, sync_state(ctx.state().epoch(),Some(peer())))
            .unwrap();

        let result = sync(&ctx).await;
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
        let (_, track_blob, data) = local_slice(0xAB, 1024);
        let data_clone = data.clone();

        let ctx = test_context_with_api(MemoryApi::new(move |_, req| match req {
            PeerReq::SyncSlices(_) => PeerRes::SyncSlices(Ok(SyncSlicesRes {
                entries: vec![entry(a, &data_clone)],
                next_cursor: None,
            })),
            _ => panic!("unexpected request"),
        }))
        .await;

        ctx.store
            .set_spool_state(SPOOL, sync_state(ctx.state().epoch(),Some(peer())))
            .unwrap();
        ctx.store.put_track_data(a, BlobData::Coded(track_blob)).unwrap();

        let result = sync(&ctx).await;

        assert_eq!(
            result,
            SyncResult::Done {
                synced_tracks: 0,
                synced_slices: 0,
            }
        );
        assert!(!ctx.store.has_slice(SPOOL, a).unwrap());
    }

    #[tokio::test]
    async fn skips_invalid_data() {
        let a = addr(1);
        let (track_info, track_blob, _) = local_slice(0xAB, 1024);
        let invalid_data = vec![0xEE; 64];
        let invalid_data_clone = invalid_data.clone();

        let ctx = test_context_with_api(MemoryApi::new(move |_, req| match req {
            PeerReq::SyncSlices(_) => PeerRes::SyncSlices(Ok(SyncSlicesRes {
                entries: vec![entry(a, &invalid_data_clone)],
                next_cursor: None,
            })),
            _ => panic!("unexpected request"),
        }))
        .await;

        ctx.store
            .set_spool_state(SPOOL, sync_state(ctx.state().epoch(),Some(peer())))
            .unwrap();
        ctx.store.put_track(a, track_info).unwrap();
        ctx.store.put_track_data(a, BlobData::Coded(track_blob)).unwrap();

        let result = sync(&ctx).await;

        assert_eq!(
            result,
            SyncResult::Done {
                synced_tracks: 0,
                synced_slices: 0,
            }
        );
        assert!(!ctx.store.has_slice(SPOOL, a).unwrap());
    }

    #[tokio::test]
    async fn resumes_cursor() {
        let a1 = addr(1);
        let a2 = addr(2);
        let (track_info1, blob1, slice1) = local_slice(0x11, 1024);
        let (track_info2, blob2, slice2) = local_slice(0x22, 1024);
        let cursors = Arc::new(Mutex::new(Vec::new()));
        let seen = cursors.clone();

        // The second page is only served to a request that carries the first page's cursor.
        let ctx = test_context_with_api(MemoryApi::new(move |_, req| match req {
            PeerReq::SyncSlices(req) => {
                let mut seen = seen.lock().expect("cursor list");
                seen.push(req.cursor.clone());

                match req.cursor {
                    None => PeerRes::SyncSlices(Ok(SyncSlicesRes {
                        entries: vec![entry(a1, &slice1)],
                        next_cursor: Some(a2.to_bytes().to_vec()),
                    })),
                    Some(_) => PeerRes::SyncSlices(Ok(SyncSlicesRes {
                        entries: vec![entry(a2, &slice2)],
                        next_cursor: None,
                    })),
                }
            }
            _ => panic!("unexpected request"),
        }))
        .await;

        ctx.store
            .set_spool_state(SPOOL, sync_state(ctx.state().epoch(),Some(peer())))
            .unwrap();
        ctx.store.put_track(a1, track_info1).unwrap();
        ctx.store.put_track(a2, track_info2).unwrap();
        ctx.store.put_track_data(a1, BlobData::Coded(blob1)).unwrap();
        ctx.store.put_track_data(a2, BlobData::Coded(blob2)).unwrap();

        let result = sync(&ctx).await;
        assert!(matches!(result, SyncResult::Done { .. }));
        assert!(ctx.store.has_slice(SPOOL, a1).unwrap());
        assert!(ctx.store.has_slice(SPOOL, a2).unwrap());
        assert_eq!(
            *cursors.lock().expect("cursor list"),
            [None, Some(a2.to_bytes().to_vec())],
        );
    }

    // slices of two carried groups arrive on one stream and land under their own stored spools
    #[tokio::test]
    async fn one_stream() {
        let own = addr(1);
        let carried = addr(2);
        let group = VirtualGroup(1);
        let (own_track, own_blob, own_slice) = local_slice(0x11, 1024);
        let (mut carried_track, carried_blob, carried_slice) = local_slice(0x22, 1024);
        carried_track.group = group;

        let requests = Arc::new(Mutex::new(Vec::new()));
        let seen = requests.clone();
        let entries = vec![entry(own, &own_slice), entry(carried, &carried_slice)];

        let ctx = test_context_with_api(MemoryApi::new(move |_, req| match req {
            PeerReq::SyncSlices(req) => {
                seen.lock().expect("request list").push(req);
                PeerRes::SyncSlices(Ok(SyncSlicesRes {
                    entries: entries.clone(),
                    next_cursor: None,
                }))
            }
            _ => panic!("unexpected request"),
        }))
        .await;

        for spool in [SPOOL, carried_spool(group)] {
            ctx.store
                .set_spool_state(spool, sync_state(ctx.state().epoch(), Some(peer())))
                .unwrap();
        }
        ctx.store.put_track(own, own_track).unwrap();
        ctx.store.put_track(carried, carried_track).unwrap();
        ctx.store.put_track_data(own, BlobData::Coded(own_blob)).unwrap();
        ctx.store.put_track_data(carried, BlobData::Coded(carried_blob)).unwrap();

        let result = sync(&ctx).await;

        assert_eq!(
            result,
            SyncResult::Done {
                synced_tracks: 0,
                synced_slices: 2,
            }
        );
        assert!(ctx.store.has_slice(SPOOL, own).unwrap());
        assert!(ctx.store.has_slice(carried_spool(group), carried).unwrap());
        assert!(!ctx.store.has_slice(SPOOL, carried).unwrap());

        let requests = requests.lock().expect("request list");
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].spool_index, SPOOL);
        assert_eq!(requests[0].live_groups, 1);
    }

    // the epoch after the group count drops, the holder of each retired group is asked once
    #[tokio::test]
    async fn asks_each_owner() {
        let kept = addr(98);
        let retired = addr(99);
        let asked = Arc::new(Mutex::new(Vec::new()));
        let seen = asked.clone();

        let ctx = test_context_with_api(MemoryApi::new(move |node, req| match req {
            PeerReq::SyncSlices(_) => {
                seen.lock().expect("peer list").push(node);
                PeerRes::SyncSlices(Ok(SyncSlicesRes {
                    entries: Vec::new(),
                    next_cursor: None,
                }))
            }
            _ => panic!("unexpected request"),
        }))
        .await;

        let epoch = ctx.state().epoch();
        ctx.store.set_spool_state(SPOOL, sync_state(epoch, Some(kept))).unwrap();
        for group in [VirtualGroup(1), VirtualGroup(3)] {
            ctx.store
                .set_spool_state(carried_spool(group), sync_state(epoch, Some(retired)))
                .unwrap();
        }

        sync(&ctx).await;

        assert_eq!(*asked.lock().expect("peer list"), [kept, retired]);
    }

    // with two live groups, a slice whose track is in the other one is left out
    #[tokio::test]
    async fn skips_other_group() {
        let a = addr(1);
        let other = VirtualGroup(1);
        let (mut track_info, track_blob, data) = local_slice(0xAB, 1024);
        track_info.group = other;

        let api = MemoryApi::new(move |_, req| match req {
            PeerReq::SyncSlices(_) => PeerRes::SyncSlices(Ok(SyncSlicesRes {
                entries: vec![entry(a, &data)],
                next_cursor: None,
            })),
            _ => panic!("unexpected request"),
        });
        let ctx = context_with_groups(api, 2).await;

        ctx.store
            .set_spool_state(SPOOL, sync_state(ctx.state().epoch(), Some(peer())))
            .unwrap();
        ctx.store.put_track(a, track_info).unwrap();
        ctx.store.put_track_data(a, BlobData::Coded(track_blob)).unwrap();

        let result = sync(&ctx).await;

        assert_eq!(
            result,
            SyncResult::Done {
                synced_tracks: 0,
                synced_slices: 0,
            }
        );
        assert!(!ctx.store.has_slice(SPOOL, a).unwrap());
        assert!(!ctx.store.has_slice(carried_spool(other), a).unwrap());
    }
}
