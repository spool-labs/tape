//! Track metadata and proof endpoints

use std::collections::BTreeMap;

use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::IntoResponse;

use tape_rpc::Rpc;
use tape_store_api::Store;
use tape_api::program::tapedrive::TrackAddress;
use tape_core::tape::TapeFlags;
use tape_core::track::TRACK_TREE_HEIGHT;
use tape_core::track::types::{CompressedTrack, CompressedTrackProof};
use tape_core::types::TrackNumber;
use tape_crypto::address::Address;
use tape_crypto::Hash;
use tape_crypto::merkle::{MerkleLeafTree, hash_leaf};
use tape_protocol::Api;
use tape_protocol::api::{
    BINARY_CONTENT, FindTrackRequest, ListTracksByTapeRequest, ListTracksByTapeResponse,
    ListObjectsRequest, ListObjectsResponse, ObjectListItem, TrackDataResponse,
    TrackProofResponse, TrackResponse, ops::FindTrackVersion,
};
use tape_store::ops::{
    ObjectInfoOps, ObjectListOps, TapeOps, TrackDataOps, TrackOps, object_has_left,
    track_has_left,
};
use tape_store::types::{ObjectInfo, TapeInfo};

use crate::features::http::auth::{local_access_threshold, MaybeActivePeer, MaybeStakedPeer};
use crate::features::blacklist::refuses_object;
use crate::features::http::error::RouteError;
use crate::features::http::handlers::track::{
    parse_tape, parse_track, resolve_tape_track, resolve_track,
};
use crate::features::http::share::check_share_token;
use crate::features::http::peer_identity::PeerIdentity;
use crate::features::http::state::AppState;
use crate::features::store::util::is_responsible_for_group;

const MAX_TRACK_SCAN_LIMIT: usize = u32::MAX as usize;
const MAX_OBJECT_LIST_LIMIT: usize = 1_000;

pub async fn get_track<Db: Store, Cluster: Api, Blockchain: Rpc>(
    State(state): State<AppState<Db, Cluster, Blockchain>>,
    Path(track_id): Path<String>,
) -> Result<impl IntoResponse, RouteError> {

    let track_addr = parse_track(&track_id)?;

    let track = resolve_track(&state, track_addr)?
        .ok_or(RouteError::NotFound)?;

    let body = wincode::serialize(&TrackResponse {
        track: track.pack(),
    })
    .map_err(|error| RouteError::Internal(format!("serialize track response: {error}")))?;

    Ok((StatusCode::OK, [(header::CONTENT_TYPE, BINARY_CONTENT)], body))
}

pub async fn get_track_data<Db: Store, Cluster: Api, Blockchain: Rpc>(
    State(state): State<AppState<Db, Cluster, Blockchain>>,
    MaybeStakedPeer(staked_peer): MaybeStakedPeer,
    MaybeActivePeer(active_peer): MaybeActivePeer,
    identity: PeerIdentity,
    Path(track_id): Path<String>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, RouteError> {

    let track_addr = parse_track(&track_id)?;

    let track = resolve_track(&state, track_addr)?
        .ok_or(RouteError::NotFound)?;

    if track.is_inline() && local_access_threshold(&state).0 > 0 && staked_peer.is_none() {
        return Err(RouteError::Forbidden("staked peer required".into()));
    }

    // A node answers while it holds any spool of the track's group, one it is
    // handing over included: after the group count drops, the new holders pull
    // inline payloads from the nodes that held the group last epoch.
    let is_owner = is_responsible_for_group(
        state.context.store.as_ref(),
        track.group,
        state.context.params().profile,
    )
    .map_err(store_error)?;

    if !is_owner {
        return Err(RouteError::NotResponsible);
    }

    if refuses_object(
        state.context.store.as_ref(),
        &state.context.blacklist,
        state.context.node_address(),
        state.context.state().epoch(),
        track_addr,
        track.tape,
    )
    .map_err(store_error)?
    {
        return Err(RouteError::BlacklistedObject);
    }

    check_share_token(&state, &track, identity, active_peer, &headers)?;

    let data = match state.context.pending.track_data(track_addr) {
        Some(data) => data,
        None => state
            .context
            .store
            .get_track_data(track_addr)
            .map_err(store_error)?
            .ok_or(RouteError::NotFound)?,
    };

    let body = wincode::serialize(&TrackDataResponse {
        data,
    })
    .map_err(|error| RouteError::Internal(format!("serialize track data response: {error}")))?;

    state.context.metrics.add_downloaded(body.len() as u64);
    Ok((StatusCode::OK, [(header::CONTENT_TYPE, BINARY_CONTENT)], body))
}

pub async fn get_track_proof<Db: Store, Cluster: Api, Blockchain: Rpc>(
    State(state): State<AppState<Db, Cluster, Blockchain>>,
    MaybeStakedPeer(staked_peer): MaybeStakedPeer,
    Path(track_id): Path<String>,
) -> Result<impl IntoResponse, RouteError> {

    let track_addr = parse_track(&track_id)?;

    let track = resolve_track(&state, track_addr)?
        .ok_or(RouteError::NotFound)?;

    // The chain has no leaf for a deleted track this node still holds.
    let info = state.context.store.get_object_info(track_addr).map_err(store_error)?;
    if matches!(info, Some(ObjectInfo::Deleted { .. })) {
        return Err(RouteError::NotFound);
    }

    // The reservation is folded in too. A track resolved from the overlay whose
    // tape was only read from disk is not-found on every peer at once until the
    // reservation finalizes, which is a whole finality window of refused deletes
    // on any freshly reserved tape.
    let in_store = state.context.store.get_tape(track.tape).map_err(store_error)?;
    let tape = state
        .context
        .pending
        .apply_to_tape(track.tape, in_store)
        .ok_or(RouteError::NotFound)?;

    // The chain has no leaf for a track that left its tape, which this node
    // holds until its sweep.
    if track_has_left(Some(&tape), &track) {
        return Err(RouteError::NotFound);
    }

    if local_access_threshold(&state).0 > 0
        && staked_peer.is_none()
        && !TapeFlags::is_system(tape.flags)
    {
        return Err(RouteError::Forbidden("staked peer required".into()));
    }

    let pending_tracks = state
        .context
        .pending
        .registered_tracks_by_tape(track.tape);

    let pending_leaf_count = pending_tracks
        .iter()
        .filter(|(_, pending)| !track_has_left(Some(&tape), pending))
        .map(|(_, track)| track.track_number.next().as_usize())
        .max()
        .unwrap_or(0);

    // A tape's tree starts over when it is cleared and the track counter keeps
    // counting, so a track sits at its number less the tree's start. A track
    // from before the start has no leaf.
    let tree_start = tape.tree_start.as_usize();
    let track_index = (track.track_number.0 as usize)
        .checked_sub(tree_start)
        .ok_or(RouteError::NotFound)?;

    let leaf_count = (tape.next_track_number.0 as usize)
        .max(pending_leaf_count)
        .max(track.track_number.next().as_usize())
        .saturating_sub(tree_start);

    if leaf_count == 0
        || leaf_count > (1usize << TRACK_TREE_HEIGHT)
        || track_index >= leaf_count
    {
        return Err(RouteError::Internal("invalid tape track numbering".into()));
    }

    let empty_hash = hash_leaf(&[]);
    let mut leaves = vec![empty_hash; leaf_count];

    let disk_tracks = state
        .context
        .store
        .iter_tracks_by_tape_from(track.tape, tape.id, tape.tree_start.checked_prev(), leaf_count)
        .map_err(store_error)?;

    for tape_track in merge_pending_tape_tracks(
        &state,
        Some(&tape),
        disk_tracks,
        pending_tracks,
    )? {
        let Some(index) = (tape_track.track_number.0 as usize).checked_sub(tree_start) else {
            continue;
        };
        if index < leaf_count {
            leaves[index] = tape_track.get_hash();
        }
    }

    let proof: [Hash; TRACK_TREE_HEIGHT] = MerkleLeafTree::new(&leaves, TRACK_TREE_HEIGHT)
        .and_then(|tree| tree.proof_at_n::<{ TRACK_TREE_HEIGHT }>(track_index))
        .map_err(|_| RouteError::Internal("invalid track proof".into()))?;

    let body = wincode::serialize(&TrackProofResponse {
        proof: CompressedTrackProof { state: track, proof }.pack(),
    })
    .map_err(|error| RouteError::Internal(format!("serialize track proof response: {error}")))?;

    Ok((StatusCode::OK, [(header::CONTENT_TYPE, BINARY_CONTENT)], body))
}

pub async fn get_track_by_number<Db: Store, Cluster: Api, Blockchain: Rpc>(
    State(state): State<AppState<Db, Cluster, Blockchain>>,
    Path((tape_id, track_number)): Path<(String, u64)>,
) -> Result<impl IntoResponse, RouteError> {

    let tape = parse_tape(&tape_id)?;
    let track = resolve_tape_track(&state, tape, TrackNumber(track_number))?
        .ok_or(RouteError::NotFound)?;

    let body = wincode::serialize(&TrackResponse {
        track: track.pack(),
    })
    .map_err(|error| RouteError::Internal(format!("serialize track response: {error}")))?;

    Ok((StatusCode::OK, [(header::CONTENT_TYPE, BINARY_CONTENT)], body))
}

pub async fn find_track<Db: Store, Cluster: Api, Blockchain: Rpc>(
    State(state): State<AppState<Db, Cluster, Blockchain>>,
    MaybeStakedPeer(staked_peer): MaybeStakedPeer,
    Path(tape_id): Path<String>,
    body: Bytes,
) -> Result<impl IntoResponse, RouteError> {

    let tape: Address = tape_id
        .parse()
        .map_err(|error| RouteError::BadRequest(format!("invalid tape id: {error}")))?;

    if local_access_threshold(&state).0 > 0 && staked_peer.is_none() {
        return Err(RouteError::Forbidden("staked peer required".into()));
    }

    let request: FindTrackRequest = wincode::deserialize(&body)
        .map_err(|error| RouteError::BadRequest(format!("find track request: {error}")))?;

    let pending_tracks = state.context.pending
        .registered_tracks_by_tape(tape);

    // The row has the number of the tape at this address now, and the tracks
    // from its tree's start on are the ones the tape holds.
    let in_store = state.context.store.get_tape(tape).map_err(store_error)?;
    let tape_row = state.context.pending.apply_to_tape(tape, in_store);
    let disk_tracks = match &tape_row {
        Some(tape_row) => state
            .context
            .store
            .iter_tracks_by_tape_from(
                tape,
                tape_row.id,
                tape_row.tree_start.checked_prev(),
                MAX_TRACK_SCAN_LIMIT,
            )
            .map_err(store_error)?,
        None => Vec::new(),
    };
    let mut matches =
        merge_pending_tape_tracks(&state, tape_row.as_ref(), disk_tracks, pending_tracks)?
        .into_iter()
        .filter(|track| track.key == request.key)
        .collect::<Vec<_>>();

    matches.sort_by_key(|track| track.track_number.0);

    let track = match request.version {
        FindTrackVersion::Latest => matches.pop(),
        FindTrackVersion::Number(track_number) => {
            matches.into_iter().find(|track| track.track_number == track_number)
        }
    }
    .ok_or(RouteError::NotFound)?;

    let body = wincode::serialize(&TrackResponse {
        track: track.pack(),
    })
    .map_err(|error| RouteError::Internal(format!("serialize track response: {error}")))?;

    Ok((StatusCode::OK, [(header::CONTENT_TYPE, BINARY_CONTENT)], body))
}

pub async fn list_tracks_by_tape<Db: Store, Cluster: Api, Blockchain: Rpc>(
    State(state): State<AppState<Db, Cluster, Blockchain>>,
    MaybeStakedPeer(staked_peer): MaybeStakedPeer,
    Path(tape_id): Path<String>,
    body: Bytes,
) -> Result<impl IntoResponse, RouteError> {

    let tape: Address = tape_id
        .parse()
        .map_err(|error| RouteError::BadRequest(format!("invalid tape id: {error}")))?;

    let in_store = state.context.store.get_tape(tape).map_err(store_error)?;
    let tape_row = state.context.pending.apply_to_tape(tape, in_store);

    if local_access_threshold(&state).0 > 0
        && staked_peer.is_none()
        && !tape_row.as_ref().is_some_and(|info| TapeFlags::is_system(info.flags))
    {
        return Err(RouteError::Forbidden("staked peer required".into()));
    }

    let request: ListTracksByTapeRequest = wincode::deserialize(&body)
        .map_err(|error| RouteError::BadRequest(format!("list tracks request: {error}")))?;

    let limit = (request.limit as usize).clamp(1, MAX_TRACK_SCAN_LIMIT);

    let pending_tracks = state.context.pending
        .registered_tracks_by_tape(tape);

    let disk_limit = limit
        .saturating_add(pending_tracks.len())
        .saturating_add(1)
        .min(MAX_TRACK_SCAN_LIMIT);

    // The row has the number of the tape at this address now, and the tracks
    // from its tree's start on are the ones the tape holds.
    let disk_tracks = match &tape_row {
        Some(tape_row) => {
            let after = request.cursor.max(tape_row.tree_start.checked_prev());
            state
                .context
                .store
                .iter_tracks_by_tape_from(tape, tape_row.id, after, disk_limit)
                .map_err(store_error)?
        }
        None => Vec::new(),
    };

    // A full scan means more rows may follow it, however many of these the
    // merge leaves out as deleted.
    let scanned_to = match disk_tracks.last() {
        Some(last) if disk_tracks.len() == disk_limit => Some(last.track_number),
        Some(_) | None => None,
    };

    let mut tracks =
        merge_pending_tape_tracks(&state, tape_row.as_ref(), disk_tracks, pending_tracks)?;
    if let Some(cursor) = request.cursor {
        tracks.retain(|track| track.track_number > cursor);
    }
    if let Some(scanned_to) = scanned_to {
        tracks.retain(|track| track.track_number <= scanned_to);
    }

    tracks.sort_by_key(|track| track.track_number.0);
    // The cursor is the last track sent, since the next page starts after it.
    // A page the deleted rows left short still continues after its scan.
    let next_cursor = if tracks.len() > limit {
        Some(tracks[limit - 1].track_number)
    } else {
        scanned_to
    };

    let tracks = tracks
        .into_iter()
        .take(limit)
        .map(|track| track.pack())
        .collect();

    let body = wincode::serialize(&ListTracksByTapeResponse { tracks, next_cursor })
        .map_err(|error| RouteError::Internal(format!("serialize list tracks response: {error}")))?;

    Ok((StatusCode::OK, [(header::CONTENT_TYPE, BINARY_CONTENT)], body))
}

pub async fn list_objects<Db: Store, Cluster: Api, Blockchain: Rpc>(
    State(state): State<AppState<Db, Cluster, Blockchain>>,
    MaybeStakedPeer(staked_peer): MaybeStakedPeer,
    Path(tape_id): Path<String>,
    body: Bytes,
) -> Result<impl IntoResponse, RouteError> {
    let bucket: Address = tape_id
        .parse()
        .map_err(|error| RouteError::BadRequest(format!("invalid tape id: {error}")))?;

    if local_access_threshold(&state).0 > 0 && staked_peer.is_none() {
        return Err(RouteError::Forbidden("staked peer required".into()));
    }

    let request: ListObjectsRequest = wincode::deserialize(&body)
        .map_err(|error| RouteError::BadRequest(format!("list objects request: {error}")))?;

    let limit = (request.limit as usize).clamp(1, MAX_OBJECT_LIST_LIMIT);
    let delimiter = request
        .delimiter
        .as_deref()
        .filter(|delimiter| !delimiter.is_empty());

    let page = state
        .context
        .store
        .list_objects(
            bucket,
            &request.prefix,
            delimiter,
            request.cursor.as_deref(),
            limit,
        )
        .map_err(store_error)?;

    // The store's listing follows the stored row. A close, rewind or renew
    // that is still pending takes its objects out here.
    let in_store = state.context.store.get_tape(bucket).map_err(store_error)?;
    let tape_row = state.context.pending.apply_to_tape(bucket, in_store);

    let mut objects = Vec::with_capacity(page.objects.len());
    for (name, entry) in page.objects {
        if object_has_left(tape_row.as_ref(), &entry) {
            continue;
        }

        objects.push(ObjectListItem {
            name,
            size: entry.size,
            etag: entry.etag,
            block_time: entry.block_time,
            slot: entry.slot,
            data_tape: entry.data_tape,
            track_number: entry.track_number,
            track: entry.track,
            kind: entry.kind,
            content_type: entry.content_type,
        });
    }

    let body = wincode::serialize(&ListObjectsResponse {
        objects,
        common_prefixes: page.common_prefixes,
        next_cursor: page.next,
        is_truncated: page.is_truncated,
    })
    .map_err(|error| RouteError::Internal(format!("serialize list objects response: {error}")))?;

    Ok((StatusCode::OK, [(header::CONTENT_TYPE, BINARY_CONTENT)], body))
}

/// A tape's stored tracks with its pending ones folded in. `tape_row` is the
/// row at the tape's address, and a pending track that has left it is dropped:
/// one written before a rewind or a close that is itself still pending.
fn merge_pending_tape_tracks<Db: Store, Cluster: Api, Blockchain: Rpc>(
    state: &AppState<Db, Cluster, Blockchain>,
    tape_row: Option<&TapeInfo>,
    disk_tracks: Vec<CompressedTrack>,
    pending_tracks: Vec<(Address, CompressedTrack)>,
) -> Result<Vec<CompressedTrack>, RouteError> {
    let mut by_number = BTreeMap::new();

    let addresses: Vec<Address> = disk_tracks.iter().map(|track| track.address()).collect();
    let infos = state
        .context
        .store
        .get_object_infos(&addresses)
        .map_err(store_error)?;

    for ((disk_track, track_addr), info) in disk_tracks.into_iter().zip(addresses).zip(infos) {

        if matches!(info, Some(ObjectInfo::Deleted { .. })) {
            continue;
        }

        if let Some(track) = state
            .context
            .pending
            .apply_to_track(track_addr, Some(disk_track))
        {
            by_number.insert(track.track_number, track);
        }
    }

    for (_, track) in pending_tracks {
        if !track_has_left(tape_row, &track) {
            by_number.insert(track.track_number, track);
        }
    }

    Ok(by_number.into_values().collect())
}

fn store_error(error: impl core::fmt::Display) -> RouteError {
    RouteError::Internal(error.to_string())
}

#[cfg(test)]
mod tests {
    use bytemuck::Zeroable;
    use tape_peer_manager::PeerNode;
    use tape_core::spooler::VirtualGroup;
    use tape_core::track::archive::TrackArchive;
    use tape_core::track::blob::BlobEncoding;
    use tape_core::track::data::BlobData;
    use tape_core::track::types::{TrackFlags, TrackKind, TrackState};
    use tape_core::types::coin::TAPE;
    use tape_core::types::{EpochNumber, SlotNumber, StorageUnits, TapeNumber};
    use tape_recover::state::SpoolState;
    use tape_store::ops::{SpoolOps, TapeOps, TrackOps};
    use tape_store::types::TapeInfo;

    use super::*;
    use crate::features::http::auth::StakedPeer;
    use crate::harness::{NodeHarness, TestContext};

    async fn call_get_track_data(
        ctx: &TestContext,
        track: Address,
    ) -> Result<impl IntoResponse, RouteError> {
        get_track_data::<_, _, _>(
            State(AppState {
                context: ctx.clone(),
            }),
            MaybeStakedPeer(None),
            MaybeActivePeer(None),
            PeerIdentity::anonymous(),
            Path(track.to_string()),
            HeaderMap::new(),
        )
        .await
    }

    // a node answers for a track while it holds a spool of the track's group,
    // one it is handing over included, so a new holder can pull from it
    #[tokio::test]
    async fn track_data_follows_held_spools() {
        let ctx = test_context().await;
        let track = seed_track(&ctx, false);
        ctx.store
            .put_track_data(track, BlobData::Coded(BlobEncoding::zeroed()))
            .expect("track data");

        let refused = call_get_track_data(&ctx, track).await;
        assert!(matches!(refused, Err(RouteError::NotResponsible)));

        let row = SpoolState { lost_at: EpochNumber(1), ..SpoolState::new_scanning() };
        ctx.store
            .put_spool_states(&[(VirtualGroup(0).spool_at(3), row)])
            .expect("spool row");

        assert!(call_get_track_data(&ctx, track).await.is_ok());
    }

    async fn test_context() -> TestContext {
        NodeHarness::builder()
            .nodes(25)
            .no_prev_snapshot_tape()
            .build()
            .await
            .expect("build harness")
            .ctx_for(0)
    }

    fn set_access_threshold(ctx: &TestContext) {
        let mut preferences = tape_core::system::NodePreferences::zeroed();
        preferences.access_threshold = TAPE(1);
        ctx.peer_manager.add_peer(PeerNode {
            node: ctx.node_address(),
            bls_pubkey: Zeroable::zeroed(),
            tls_pubkey: Zeroable::zeroed(),
            network_address: Zeroable::zeroed(),
            name: Zeroable::zeroed(),
            preferences,
            stake: TAPE(0),
        });
    }

    fn seed_track(ctx: &TestContext, system: bool) -> Address {
        let tape = Address::new_unique();
        let flags = if system { TapeFlags::SYSTEM } else { 0 };
        ctx.store
            .put_tape(
                tape,
                TapeInfo {
                    id: TapeNumber(1),
                    flags,
                    end_epoch: EpochNumber(100),
                    next_track_number: TrackNumber(1),
                    tree_start: TrackNumber(0),
                    authority: Address::default(),
                    delegate: Address::default(),
                    nonce: Hash::default(),
                },
            )
            .expect("put tape");

        let track = CompressedTrack {
            tape,
            tape_number: TapeNumber(1),
            track_number: TrackNumber(0),
            key: Hash::from([1u8; 32]),
            kind: TrackKind::Coded as u64,
            state: TrackState::Certified as u64,
            flags: TrackFlags::NONE,
            size: StorageUnits::from_bytes(64),
            group: VirtualGroup(0),
            value_hash: Hash::from([2u8; 32]),
        };
        let track_addr = track.address();
        ctx.store.put_track(track_addr, track).expect("put track");
        track_addr
    }

    async fn call_get_track_proof(
        ctx: &TestContext,
        track: Address,
        staked_peer: Option<StakedPeer>,
    ) -> Result<impl IntoResponse, RouteError> {
        get_track_proof::<_, _, _>(
            State(AppState {
                context: ctx.clone(),
            }),
            MaybeStakedPeer(staked_peer),
            Path(track.to_string()),
        )
        .await
    }

    #[tokio::test]
    async fn anonymous_user_tape_proof_requires_stake_when_threshold_positive() {
        let ctx = test_context().await;
        set_access_threshold(&ctx);
        let track = seed_track(&ctx, false);

        let result = call_get_track_proof(&ctx, track, None).await;

        assert!(matches!(result, Err(RouteError::Forbidden(_))));
    }

    #[tokio::test]
    async fn anonymous_system_tape_proof_allowed_when_threshold_positive() {
        let ctx = test_context().await;
        set_access_threshold(&ctx);
        let track = seed_track(&ctx, true);

        let result = call_get_track_proof(&ctx, track, None).await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn staked_peer_can_fetch_user_tape_proof_when_threshold_positive() {
        let ctx = test_context().await;
        set_access_threshold(&ctx);
        let track = seed_track(&ctx, false);
        let staked_peer = StakedPeer {
            node: Address::new_unique(),
            tls_pubkey: Zeroable::zeroed(),
            stake: TAPE(1),
        };

        let result = call_get_track_proof(&ctx, track, Some(staked_peer)).await;

        assert!(result.is_ok());
    }

    // paging with a small limit returns every track once
    #[tokio::test]
    async fn pages_cover_every_track() {
        let ctx = test_context().await;
        let (tape, _) = seed_tape(&ctx);

        let mut seen = Vec::new();
        let mut cursor = None;
        loop {
            let request = wincode::serialize(&ListTracksByTapeRequest { cursor, limit: 2 }).expect("encode");
            let response = list_tracks_by_tape::<_, _, _>(
                State(AppState { context: ctx.clone() }),
                MaybeStakedPeer(None),
                Path(tape.to_string()),
                Bytes::from(request),
            )
            .await
            .expect("list")
            .into_response();
            let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.expect("body");
            let page: ListTracksByTapeResponse = wincode::deserialize(&body).expect("decode");
            seen.extend(page.tracks.into_iter().map(|track| CompressedTrack::unpack(track).track_number.0));
            match page.next_cursor {
                Some(next) => cursor = Some(next),
                None => break,
            }
        }

        assert_eq!(seen, vec![0, 1, 2, 3, 4]);
    }

    /// Five tracks on one tape, sharing a key, with the tape's cursor past them.
    fn seed_tape(ctx: &TestContext) -> (Address, Vec<CompressedTrack>) {
        let tape = Address::new_unique();
        ctx.store
            .put_tape(
                tape,
                TapeInfo {
                    id: TapeNumber(1),
                    flags: 0,
                    end_epoch: EpochNumber(100),
                    next_track_number: TrackNumber(5),
                    tree_start: TrackNumber(0),
                    authority: Address::default(),
                    delegate: Address::default(),
                    nonce: Hash::default(),
                },
            )
            .expect("put tape");

        let mut tracks = Vec::new();
        for number in 0..5 {
            let track = CompressedTrack {
                tape,
                tape_number: TapeNumber(1),
                track_number: TrackNumber(number),
                key: Hash::from([1u8; 32]),
                kind: TrackKind::Coded as u64,
                state: TrackState::Certified as u64,
                flags: TrackFlags::NONE,
                size: StorageUnits::from_bytes(64),
                group: VirtualGroup(0),
                value_hash: Hash::from([2u8; 32]),
            };
            ctx.store.put_track(track.address(), track).expect("put track");
            tracks.push(track);
        }

        (tape, tracks)
    }

    /// Flag a track the way a delete applied on a storage node leaves it.
    fn flag_deleted(ctx: &TestContext, track: &CompressedTrack) {
        ctx.store
            .put_object_info(
                track.address(),
                ObjectInfo::Deleted {
                    kind: None,
                    track_address: track.address(),
                    registered_epoch: EpochNumber(1),
                    certified_epoch: EpochNumber(1),
                    deleted_epoch: EpochNumber(3),
                },
            )
            .expect("put object info");
    }

    // a listing leaves out deleted tracks and still reaches the rows after them
    #[tokio::test]
    async fn pages_skip_deleted() {
        let ctx = test_context().await;
        let (tape, tracks) = seed_tape(&ctx);
        for number in [1, 2, 3] {
            flag_deleted(&ctx, &tracks[number]);
        }

        let mut seen = Vec::new();
        let mut cursor = None;
        loop {
            let request = wincode::serialize(&ListTracksByTapeRequest { cursor, limit: 2 }).expect("encode");
            let response = list_tracks_by_tape::<_, _, _>(
                State(AppState { context: ctx.clone() }),
                MaybeStakedPeer(None),
                Path(tape.to_string()),
                Bytes::from(request),
            )
            .await
            .expect("list")
            .into_response();
            let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.expect("body");
            let page: ListTracksByTapeResponse = wincode::deserialize(&body).expect("decode");
            seen.extend(page.tracks.into_iter().map(|track| CompressedTrack::unpack(track).track_number.0));
            match page.next_cursor {
                Some(next) => cursor = Some(next),
                None => break,
            }
        }

        assert_eq!(seen, vec![0, 4]);
    }

    // a proof holds an empty leaf where a deleted track sits, as the chain's
    // tree does, and the deleted track has no proof of its own
    #[tokio::test]
    async fn proof_blanks_deleted() {
        let ctx = test_context().await;
        let (_, tracks) = seed_tape(&ctx);
        flag_deleted(&ctx, &tracks[1]);

        let response = call_get_track_proof(&ctx, tracks[0].address(), None)
            .await
            .expect("proof")
            .into_response();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.expect("body");
        let response: TrackProofResponse = wincode::deserialize(&body).expect("decode");
        let proof = CompressedTrackProof::unpack(response.proof);

        let mut leaves: Vec<Hash> = tracks.iter().map(|track| track.get_hash()).collect();
        leaves[1] = hash_leaf(&[]);
        let expected: [Hash; TRACK_TREE_HEIGHT] = MerkleLeafTree::new(&leaves, TRACK_TREE_HEIGHT)
            .and_then(|tree| tree.proof_at_n::<{ TRACK_TREE_HEIGHT }>(0))
            .expect("expected proof");

        assert_eq!(proof.proof, expected);

        let deleted = call_get_track_proof(&ctx, tracks[1].address(), None).await;
        assert!(matches!(deleted, Err(RouteError::NotFound)));
    }

    // on a tape whose tree was cleared, a proof puts the track at its position
    // in the chain's tree, and a track from before the clear has no proof
    #[tokio::test]
    async fn proof_after_clear() {
        let ctx = test_context().await;
        let (tape, tracks) = seed_tape(&ctx);
        let start = 2;

        let mut row = ctx.store.get_tape(tape).expect("get tape").expect("tape row");
        row.tree_start = tracks[start].track_number;
        ctx.store.put_tape(tape, row).expect("put tape");

        // One more track is confirmed and not yet final.
        let pending = CompressedTrack { track_number: TrackNumber(5), ..tracks[4] };
        ctx.pending.apply_register(
            SlotNumber(10),
            pending.address(),
            pending,
            BlobData::Inline(vec![]),
        );

        let mut in_tree = tracks[start..].to_vec();
        in_tree.push(pending);

        let mut archive = TrackArchive::zeroed();
        archive.next_number = tracks[start].track_number;
        for track in &in_tree {
            archive.append(track).expect("append");
        }

        for track in &in_tree {
            let response = call_get_track_proof(&ctx, track.address(), None)
                .await
                .expect("proof")
                .into_response();
            let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.expect("body");
            let response: TrackProofResponse = wincode::deserialize(&body).expect("decode");
            let proof = CompressedTrackProof::unpack(response.proof);

            archive.verify(&proof).expect("the chain's tree accepts the proof");
        }

        let before = call_get_track_proof(&ctx, tracks[start - 1].address(), None).await;
        assert!(matches!(before, Err(RouteError::NotFound)));
    }

    // the latest track for a key is the latest one the chain still holds
    #[tokio::test]
    async fn find_skips_deleted() {
        let ctx = test_context().await;
        let (tape, tracks) = seed_tape(&ctx);
        flag_deleted(&ctx, &tracks[4]);

        let request = wincode::serialize(&FindTrackRequest {
            key: tracks[0].key,
            version: FindTrackVersion::Latest,
        })
        .expect("encode");
        let response = find_track::<_, _, _>(
            State(AppState { context: ctx.clone() }),
            MaybeStakedPeer(None),
            Path(tape.to_string()),
            Bytes::from(request),
        )
        .await
        .expect("find")
        .into_response();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.expect("body");
        let found: TrackResponse = wincode::deserialize(&body).expect("decode");

        assert_eq!(CompressedTrack::unpack(found.track).track_number, TrackNumber(3));
    }

    // a track and the tape it names both resolve through the pending overlay,
    // so a fresh tape's first track is servable at confirmed latency
    #[tokio::test]
    async fn pending_track_on_pending_tape() {
        let ctx = test_context().await;
        let tape = Address::new_unique();
        let track = CompressedTrack {
            tape,
            tape_number: TapeNumber(1),
            track_number: TrackNumber(0),
            key: Hash::from([1u8; 32]),
            kind: TrackKind::Coded as u64,
            state: TrackState::Certified as u64,
            flags: TrackFlags::NONE,
            size: StorageUnits::from_bytes(64),
            group: VirtualGroup(0),
            value_hash: Hash::from([2u8; 32]),
        };
        let track_addr = track.address();

        // The registration is confirmed but not finalized, which is exactly
        // where a just-uploaded track sits.
        ctx.pending.apply_register(
            tape_core::types::SlotNumber(10),
            track_addr,
            track,
            tape_core::track::data::BlobData::Inline(vec![]),
        );

        // Neither the track nor the tape is durable yet.
        assert!(
            matches!(
                call_get_track_proof(&ctx, track_addr, None).await,
                Err(RouteError::NotFound)
            ),
            "a track with no tape anywhere should not resolve"
        );

        ctx.pending.apply_reserve(
            tape_core::types::SlotNumber(10),
            tape,
            TapeInfo {
                id: TapeNumber(1),
                flags: 0,
                end_epoch: EpochNumber(100),
                next_track_number: TrackNumber(0),
                tree_start: TrackNumber(0),
                authority: Address::default(),
                delegate: Address::default(),
                nonce: Hash::default(),
            },
        );
        assert!(
            call_get_track_proof(&ctx, track_addr, None).await.is_ok(),
            "the overlay resolved the track but not its reservation"
        );

        // Dropping the slot takes the reservation with it.
        ctx.pending.drop_slot(tape_core::types::SlotNumber(10));
        assert!(matches!(
            call_get_track_proof(&ctx, track_addr, None).await,
            Err(RouteError::NotFound)
        ));
    }

    async fn track_by_number(
        ctx: &TestContext,
        tape: Address,
        number: u64,
    ) -> Result<CompressedTrack, RouteError> {
        let response = get_track_by_number::<_, _, _>(
            State(AppState { context: ctx.clone() }),
            Path((tape.to_string(), number)),
        )
        .await?
        .into_response();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.expect("body");
        let decoded: TrackResponse = wincode::deserialize(&body).expect("decode");
        Ok(CompressedTrack::unpack(decoded.track))
    }

    // a by-number read finds a stored track and a pending one
    #[tokio::test]
    async fn by_number_stored_and_pending() {
        let ctx = test_context().await;
        let stored_addr = seed_track(&ctx, false);
        let stored = ctx.store.get_track(stored_addr).expect("get track").expect("stored track");
        let tape = stored.tape;

        assert_eq!(track_by_number(&ctx, tape, 0).await.expect("stored"), stored);
        assert!(matches!(track_by_number(&ctx, tape, 1).await, Err(RouteError::NotFound)));

        let pending = CompressedTrack {
            track_number: TrackNumber(1),
            key: Hash::from([3u8; 32]),
            ..stored
        };
        ctx.pending.apply_register(
            tape_core::types::SlotNumber(10),
            pending.address(),
            pending,
            tape_core::track::data::BlobData::Inline(vec![]),
        );

        assert_eq!(track_by_number(&ctx, tape, 1).await.expect("pending"), pending);
        assert!(matches!(track_by_number(&ctx, tape, 2).await, Err(RouteError::NotFound)));
    }

    // a lookup by number has nothing where a deleted track sits
    #[tokio::test]
    async fn by_number_skips_deleted() {
        let ctx = test_context().await;
        let (tape, tracks) = seed_tape(&ctx);
        flag_deleted(&ctx, &tracks[2]);

        assert!(matches!(track_by_number(&ctx, tape, 2).await, Err(RouteError::NotFound)));
        assert_eq!(track_by_number(&ctx, tape, 3).await.expect("live"), tracks[3]);
    }

    async fn listed_numbers(ctx: &TestContext, tape: Address) -> Vec<u64> {
        let request = wincode::serialize(&ListTracksByTapeRequest { cursor: None, limit: 16 })
            .expect("encode");
        let response = list_tracks_by_tape::<_, _, _>(
            State(AppState { context: ctx.clone() }),
            MaybeStakedPeer(None),
            Path(tape.to_string()),
            Bytes::from(request),
        )
        .await
        .expect("list")
        .into_response();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.expect("body");
        let page: ListTracksByTapeResponse = wincode::deserialize(&body).expect("decode");

        page.tracks
            .into_iter()
            .map(|track| CompressedTrack::unpack(track).track_number.0)
            .collect()
    }

    // after a rewind, reads by tape leave out the tracks from before it, and
    // after a close they find nothing. A read by track address still answers
    // until the sweep, as it does for a deleted track.
    #[tokio::test]
    async fn left_tracks_hidden() {
        let ctx = test_context().await;
        let (tape, tracks) = seed_tape(&ctx);

        let mut row = ctx.store.get_tape(tape).expect("get tape").expect("tape row");
        row.tree_start = TrackNumber(2);
        ctx.store.put_tape(tape, row).expect("put tape");

        assert_eq!(listed_numbers(&ctx, tape).await, vec![2, 3, 4]);
        assert!(matches!(track_by_number(&ctx, tape, 1).await, Err(RouteError::NotFound)));
        assert_eq!(track_by_number(&ctx, tape, 2).await.expect("on the tape"), tracks[2]);

        ctx.store.delete_tape(tape).expect("delete tape");

        assert!(listed_numbers(&ctx, tape).await.is_empty());
        assert!(matches!(track_by_number(&ctx, tape, 2).await, Err(RouteError::NotFound)));
        let by_address = call_get_track_proof(&ctx, tracks[2].address(), None).await;
        assert!(matches!(by_address, Err(RouteError::NotFound)));
        assert!(resolve_track(&AppState { context: ctx.clone() }, tracks[2].address())
            .expect("resolve")
            .is_some());
    }

    // a rewind that is confirmed and not yet final already moves the tree's
    // start: the tracks from before it are hidden, and a track written after
    // it gets a proof at its position in the new tree
    #[tokio::test]
    async fn pending_rewind() {
        let ctx = test_context().await;
        let (tape, tracks) = seed_tape(&ctx);
        let tape_number = tracks[0].tape_number;
        let written = CompressedTrack { track_number: TrackNumber(5), ..tracks[4] };

        ctx.pending.apply_rewind(SlotNumber(10), tape, tape_number, TrackNumber(5), Hash::default());
        ctx.pending.apply_register(SlotNumber(11), written.address(), written, BlobData::Inline(vec![]));

        assert_eq!(listed_numbers(&ctx, tape).await, vec![5]);
        assert!(matches!(track_by_number(&ctx, tape, 4).await, Err(RouteError::NotFound)));

        let response = call_get_track_proof(&ctx, written.address(), None)
            .await
            .expect("proof")
            .into_response();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.expect("body");
        let response: TrackProofResponse = wincode::deserialize(&body).expect("decode");
        let proof = CompressedTrackProof::unpack(response.proof);

        let mut archive = TrackArchive::zeroed();
        archive.next_number = written.track_number;
        archive.append(&written).expect("append");
        archive.verify(&proof).expect("the rewound tree accepts the proof");

        let old = call_get_track_proof(&ctx, tracks[4].address(), None).await;
        assert!(matches!(old, Err(RouteError::NotFound)));
    }

    // a tape reserved again at its old address has a new number, and reads by
    // tape answer with its tracks alone while the old tape's are still stored
    #[tokio::test]
    async fn reserved_again() {
        let ctx = test_context().await;
        let (tape, old_tracks) = seed_tape(&ctx);

        let mut row = ctx.store.get_tape(tape).expect("get tape").expect("tape row");
        row.id = TapeNumber(2);
        row.next_track_number = TrackNumber(1);
        ctx.store.put_tape(tape, row).expect("put tape");

        let track = CompressedTrack {
            tape_number: TapeNumber(2),
            key: Hash::from([3u8; 32]),
            ..old_tracks[0]
        };
        ctx.store.put_track(track.address(), track).expect("put track");

        assert_eq!(track_by_number(&ctx, tape, 0).await.expect("new track"), track);
        assert!(matches!(track_by_number(&ctx, tape, 1).await, Err(RouteError::NotFound)));

        let request = wincode::serialize(&FindTrackRequest {
            key: old_tracks[0].key,
            version: FindTrackVersion::Latest,
        })
        .expect("encode");
        let found = find_track::<_, _, _>(
            State(AppState { context: ctx.clone() }),
            MaybeStakedPeer(None),
            Path(tape.to_string()),
            Bytes::from(request),
        )
        .await;
        assert!(matches!(found, Err(RouteError::NotFound)));

        let response = call_get_track_proof(&ctx, track.address(), None)
            .await
            .expect("proof")
            .into_response();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.expect("body");
        let response: TrackProofResponse = wincode::deserialize(&body).expect("decode");
        let proof = CompressedTrackProof::unpack(response.proof);

        let mut archive = TrackArchive::zeroed();
        archive.append(&track).expect("append");
        archive.verify(&proof).expect("the new tape's tree accepts the proof");
    }
}
