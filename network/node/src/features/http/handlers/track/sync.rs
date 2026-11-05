use std::collections::HashMap;
use std::fmt::Display;

use axum::body::Bytes;
use axum::extract::State;
use axum::http::{header, StatusCode};
use axum::response::IntoResponse;

use tape_rpc::Rpc;
use tape_store_api::Store;
use tape_core::erasure::leaf_position;
use tape_core::spooler::{GroupFold, GroupIndex, VirtualGroup};
use tape_core::system::BlacklistEntry;
use tape_core::track::data::BlobData;
use tape_core::types::SpoolIndex;
use tape_crypto::address::Address;
use tape_protocol::Api;
use tape_protocol::api::{
    BINARY_CONTENT, SYNC_MAX_BYTES, SYNC_MAX_TRACKS, SyncSliceEntry, SyncSlicesRequest,
    SyncSlicesResponse, SyncTrackDataEntry, SyncTrackDataRequest, SyncTrackDataResponse,
};
use tape_slicer::coded_slice_len;
use tape_store::ops::{SliceOps, SpoolOps, TrackDataOps, TrackOps};

use crate::context::NodeContext;
use crate::features::http::auth::ActivePeer;
use crate::features::http::error::RouteError;
use crate::features::http::share::holds_group_spool;
use crate::features::http::state::AppState;
use crate::features::store::util::is_responsible_for_group;

pub async fn sync_slices<Db: Store, Cluster: Api, Blockchain: Rpc>(
    State(state): State<AppState<Db, Cluster, Blockchain>>,
    active_peer: ActivePeer,
    body: Bytes,
) -> Result<impl IntoResponse, RouteError> {
    let request: SyncSlicesRequest = wincode::deserialize(&body)
        .map_err(|error| RouteError::BadRequest(format!("sync request: {error}")))?;

    let response = answer(&state.context, &request, active_peer)?;

    let bytes = wincode::serialize(&response)
        .map_err(|error| RouteError::Internal(format!("serialize sync response: {error}")))?;

    Ok((
        StatusCode::OK,
        [(header::CONTENT_TYPE, BINARY_CONTENT)],
        bytes,
    ))
}

/// Serve the asked-for slices that fit the byte budget, in request order, and
/// list the tracks this node has no slice of to give the caller
///
/// Tracks past the budget are in neither list, and the caller asks again.
fn answer<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: &NodeContext<Db, Cluster, Blockchain>,
    request: &SyncSlicesRequest,
    caller: ActivePeer,
) -> Result<SyncSlicesResponse, RouteError> {
    if request.tracks.len() > SYNC_MAX_TRACKS {
        return Err(RouteError::BadRequest(format!(
            "sync request: {} tracks",
            request.tracks.len()
        )));
    }

    // The fold comes from the count the caller sent, so a node that has not
    // applied the caller's epoch yet answers for the same stored spools.
    let fold = GroupFold::new(request.live_groups).ok_or_else(|| {
        RouteError::BadRequest(format!("sync request: {} live groups", request.live_groups))
    })?;
    let group = GroupIndex::containing(request.spool_index);
    let position = leaf_position(request.spool_index).as_usize();
    let max_bytes = request.max_bytes.min(SYNC_MAX_BYTES).as_u64();

    let store = ctx.store.as_ref();
    let refused = ctx
        .blacklist
        .entries(store, ctx.node_address(), ctx.state().epoch())
        .map_err(store_error)?;
    let metadata = store.get_tracks(&request.tracks).map_err(store_error)?;

    let mut missing = Vec::new();
    let mut held: Vec<(Address, SpoolIndex)> = Vec::with_capacity(request.tracks.len());
    // A node answers only for the stored spools it kept a row for
    let mut rows: HashMap<SpoolIndex, bool> = HashMap::new();
    let mut is_holder = false;

    for (track_address, track) in request.tracks.iter().zip(metadata) {
        let Some(track) = track else {
            missing.push(*track_address);
            continue;
        };

        if fold.live_group(track.group) != group || !track.is_coded() {
            missing.push(*track_address);
            continue;
        }

        let stored = track.group.spool_at(position);
        let has_row = match rows.get(&stored) {
            Some(has_row) => *has_row,
            None => {
                let has_row = store.get_spool_state(stored).map_err(store_error)?.is_some();
                rows.insert(stored, has_row);
                has_row
            }
        };
        is_holder |= has_row;

        let is_refused = refused.contains(&BlacklistEntry::track(*track_address))
            || refused.contains(&BlacklistEntry::tape(track.tape));
        // A hidden track's slice goes only to a holder in its group
        let is_hidden_from_caller = track.is_hidden() && !holds_group_spool(ctx, &track, Some(caller));

        if !has_row || is_refused || is_hidden_from_caller {
            missing.push(*track_address);
            continue;
        }

        held.push((*track_address, stored));
    }

    if !is_holder {
        return Err(RouteError::NotResponsible);
    }

    // Each slice's size comes from its encoding, so nothing past the budget is read
    let profile = ctx.params().profile;
    let addresses: Vec<Address> = held.iter().map(|(track_address, _)| *track_address).collect();
    let encodings = store.get_track_datas(&addresses).map_err(store_error)?;

    let mut chosen: Vec<(SpoolIndex, Address)> = Vec::with_capacity(held.len());
    let mut total = 0u64;
    for ((track_address, stored), data) in held.into_iter().zip(encodings) {
        let Some(BlobData::Coded(blob)) = data else {
            missing.push(track_address);
            continue;
        };
        let (Ok(size), Ok(stripe_size), Ok(stripe_count)) = (
            blob.size.try_as_usize(),
            blob.stripe_size.try_as_usize(),
            blob.stripe_count.try_as_usize(),
        ) else {
            missing.push(track_address);
            continue;
        };
        let slice_len = coded_slice_len(profile, size, stripe_size, stripe_count) as u64;

        // The first slice goes even when it's over the budget on its own
        if !chosen.is_empty() && total + slice_len > max_bytes {
            break;
        }

        total += slice_len;
        chosen.push((stored, track_address));
    }

    let slices = store.get_slices(&chosen).map_err(store_error)?;
    let mut entries = Vec::with_capacity(chosen.len());
    for ((_, track_address), slice) in chosen.into_iter().zip(slices) {
        match slice {
            Some(slice_data) => entries.push(SyncSliceEntry {
                track_address: track_address.to_bytes(),
                slice_data,
            }),
            None => missing.push(track_address),
        }
    }

    Ok(SyncSlicesResponse { entries, missing })
}

pub async fn sync_track_data<Db: Store, Cluster: Api, Blockchain: Rpc>(
    State(state): State<AppState<Db, Cluster, Blockchain>>,
    active_peer: ActivePeer,
    body: Bytes,
) -> Result<impl IntoResponse, RouteError> {
    let request: SyncTrackDataRequest = wincode::deserialize(&body)
        .map_err(|error| RouteError::BadRequest(format!("track data request: {error}")))?;

    let response = answer_track_data(&state.context, &request, active_peer)?;

    let bytes = wincode::serialize(&response)
        .map_err(|error| RouteError::Internal(format!("serialize track data response: {error}")))?;

    Ok((
        StatusCode::OK,
        [(header::CONTENT_TYPE, BINARY_CONTENT)],
        bytes,
    ))
}

/// Serve the asked-for inline payloads that fit the byte budget, in request
/// order, and list the tracks this node has no payload of to give the caller
fn answer_track_data<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: &NodeContext<Db, Cluster, Blockchain>,
    request: &SyncTrackDataRequest,
    caller: ActivePeer,
) -> Result<SyncTrackDataResponse, RouteError> {
    if request.tracks.len() > SYNC_MAX_TRACKS {
        return Err(RouteError::BadRequest(format!(
            "track data request: {} tracks",
            request.tracks.len()
        )));
    }

    let max_bytes = request.max_bytes.min(SYNC_MAX_BYTES).as_u64();
    let store = ctx.store.as_ref();
    let profile = ctx.params().profile;
    let refused = ctx
        .blacklist
        .entries(store, ctx.node_address(), ctx.state().epoch())
        .map_err(store_error)?;
    let metadata = store.get_tracks(&request.tracks).map_err(store_error)?;
    // Inline payloads are small, so the asked-for ones are read in one go
    let payloads = store.get_track_datas(&request.tracks).map_err(store_error)?;

    let mut entries = Vec::new();
    let mut missing = Vec::new();
    // A node answers while it holds any spool of the track's group, one it is
    // handing over included
    let mut groups: HashMap<VirtualGroup, bool> = HashMap::new();
    let mut is_holder = false;
    let mut total = 0u64;

    for ((track_address, track), data) in request.tracks.iter().zip(metadata).zip(payloads) {
        let Some(track) = track else {
            missing.push(*track_address);
            continue;
        };
        if !track.is_inline() {
            missing.push(*track_address);
            continue;
        }

        let is_responsible = match groups.get(&track.group) {
            Some(is_responsible) => *is_responsible,
            None => {
                let is_responsible =
                    is_responsible_for_group(store, track.group, profile).map_err(store_error)?;
                groups.insert(track.group, is_responsible);
                is_responsible
            }
        };
        is_holder |= is_responsible;

        let is_refused = refused.contains(&BlacklistEntry::track(*track_address))
            || refused.contains(&BlacklistEntry::tape(track.tape));
        // A hidden track's payload goes only to a holder in its group
        let is_hidden_from_caller = track.is_hidden() && !holds_group_spool(ctx, &track, Some(caller));

        let Some(BlobData::Inline(payload)) = data else {
            missing.push(*track_address);
            continue;
        };
        if !is_responsible || is_refused || is_hidden_from_caller {
            missing.push(*track_address);
            continue;
        }

        // The first payload goes even when it's over the budget on its own
        let payload_len = payload.len() as u64;
        if !entries.is_empty() && total + payload_len > max_bytes {
            break;
        }

        total += payload_len;
        entries.push(SyncTrackDataEntry {
            track: *track_address,
            payload,
        });
    }

    if !is_holder {
        return Err(RouteError::NotResponsible);
    }

    Ok(SyncTrackDataResponse { entries, missing })
}

fn store_error(error: impl Display) -> RouteError {
    RouteError::Internal(error.to_string())
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use tape_core::erasure::MAX_GROUP_SIZE;
    use tape_core::spooler::VirtualGroup;
    use tape_core::system::SpoolState;
    use tape_core::track::blob::BlobEncoding;
    use tape_core::track::types::{CompressedTrack, TrackFlags, TrackKind, TrackState};
    use tape_core::types::{EpochNumber, StorageUnits, StripeCount, TapeNumber, TrackNumber};
    use tape_core::types::tls::NetworkTlsPubkey;
    use tape_crypto::Hash;

    use super::*;
    use crate::features::blacklist::tests::seed_entry;
    use crate::harness::{NodeHarness, TestContext};

    const SPOOL: SpoolIndex = SpoolIndex(5);

    async fn test_context() -> TestContext {
        NodeHarness::builder()
            .nodes(25)
            .no_prev_snapshot_tape()
            .build()
            .await
            .expect("build harness")
            .ctx_for(SPOOL.as_usize())
    }

    fn addr(n: u8) -> Address {
        Address::from([n; 32])
    }

    fn own_group() -> VirtualGroup {
        VirtualGroup::containing(SPOOL)
    }

    fn track(group: VirtualGroup) -> CompressedTrack {
        CompressedTrack {
            tape: addr(0),
            tape_number: TapeNumber(1),
            key: Hash::new_unique(),
            track_number: TrackNumber(0),
            kind: TrackKind::Coded as u64,
            state: TrackState::Certified as u64,
            flags: TrackFlags::NONE,
            size: StorageUnits::from_bytes(4096),
            group,
            value_hash: Hash::new_unique(),
        }
    }

    // the encoding of a one-stripe track of `size` bytes
    fn encoding(size: u64) -> BlobEncoding {
        BlobEncoding {
            size: StorageUnits::from_bytes(size),
            commitment: Hash::default(),
            stripe_size: StorageUnits::from_bytes(size),
            stripe_count: StripeCount(1),
            leaves: [Hash::default(); MAX_GROUP_SIZE],
        }
    }

    fn slice_len(ctx: &TestContext, size: u64) -> usize {
        coded_slice_len(ctx.params().profile, size as usize, size as usize, 1)
    }

    // a track this node keeps a slice of, sized as its encoding says
    fn seed(ctx: &TestContext, track_address: Address, track: CompressedTrack) {
        let size = track.size.as_u64();
        let stored = track.group.spool_at(SPOOL.as_usize());
        ctx.store.put_track(track_address, track).expect("put track");
        ctx.store
            .put_track_data(track_address, BlobData::Coded(encoding(size)))
            .expect("put track data");
        ctx.store
            .put_slice(stored, track_address, vec![7; slice_len(ctx, size)])
            .expect("put slice");
    }

    // a stored spool the node keeps for its next holder
    fn hold(ctx: &TestContext, spool: SpoolIndex) {
        let row = SpoolState { locked_at: EpochNumber(1), ..SpoolState::gained() };
        ctx.store
            .put_spool_states(&[(spool, row)])
            .expect("set spool row");
    }

    fn request(tracks: Vec<Address>, max_bytes: usize) -> SyncSlicesRequest {
        SyncSlicesRequest {
            spool_index: SPOOL,
            live_groups: 1,
            tracks,
            max_bytes: StorageUnits::from_bytes(max_bytes as u64),
        }
    }

    // another peer in the track's group, as a new holder would be
    fn holder(ctx: &TestContext) -> ActivePeer {
        let (_, node) = ctx
            .state()
            .group_peers(own_group())
            .into_iter()
            .find(|(spool, _)| *spool != SPOOL)
            .expect("a peer in the group");
        ActivePeer {
            node,
            tls_pubkey: NetworkTlsPubkey::new_unique(),
        }
    }

    fn outsider() -> ActivePeer {
        ActivePeer {
            node: addr(200),
            tls_pubkey: NetworkTlsPubkey::new_unique(),
        }
    }

    fn served(response: &SyncSlicesResponse) -> Vec<Address> {
        response
            .entries
            .iter()
            .map(|entry| Address::new(entry.track_address))
            .collect()
    }

    fn missing(response: &SyncSlicesResponse) -> HashSet<Address> {
        response.missing.iter().copied().collect()
    }

    // slices go in request order until the budget is full, and the rest are left out
    #[tokio::test]
    async fn fills_budget_in_order() {
        let ctx = test_context().await;
        hold(&ctx, SPOOL);
        for n in 1..=3 {
            seed(&ctx, addr(n), track(own_group()));
        }
        let budget = 2 * slice_len(&ctx, 4096);

        let response = answer(&ctx, &request(vec![addr(3), addr(1), addr(2)], budget), holder(&ctx)).expect("answer");

        assert_eq!(served(&response), [addr(3), addr(1)]);
        assert!(response.missing.is_empty());
    }

    // the first slice goes even when it alone is over the budget
    #[tokio::test]
    async fn first_slice_over_budget() {
        let ctx = test_context().await;
        hold(&ctx, SPOOL);
        seed(&ctx, addr(1), track(own_group()));
        seed(&ctx, addr(2), track(own_group()));

        let response = answer(&ctx, &request(vec![addr(1), addr(2)], 1), holder(&ctx)).expect("answer");

        assert_eq!(served(&response), [addr(1)]);
        assert!(response.missing.is_empty());
    }

    // a caller asking for more than the shared limit gets the shared limit
    #[tokio::test]
    async fn budget_capped() {
        let ctx = test_context().await;
        hold(&ctx, SPOOL);
        // Slices of about 3 MiB, so two fit under the limit and three don't.
        let size = 21 * 1024 * 1024;
        let cap = SYNC_MAX_BYTES.as_usize();
        let len = slice_len(&ctx, size);
        assert!(2 * len <= cap && 3 * len > cap, "slice of {len} bytes");
        for n in 1..=4 {
            seed(&ctx, addr(n), CompressedTrack {
                size: StorageUnits::from_bytes(size),
                ..track(own_group())
            });
        }
        let tracks = (1..=4).map(addr).collect();

        let response = answer(&ctx, &request(tracks, 4 * cap), holder(&ctx)).expect("answer");

        assert_eq!(served(&response), [addr(1), addr(2)]);
    }

    // a track with no metadata, in another group, inline, or with no slice is missing
    #[tokio::test]
    async fn reports_missing() {
        let ctx = test_context().await;
        hold(&ctx, SPOOL);
        let other_group = VirtualGroup(own_group().0 + 1);
        hold(&ctx, other_group.spool_at(SPOOL.as_usize()));

        seed(&ctx, addr(1), track(own_group()));
        seed(&ctx, addr(2), track(other_group));
        ctx.store
            .put_track(addr(3), CompressedTrack {
                kind: TrackKind::Inline as u64,
                ..track(own_group())
            })
            .expect("put track");
        ctx.store.put_track(addr(4), track(own_group())).expect("put track");
        ctx.store
            .put_track_data(addr(4), BlobData::Coded(encoding(4096)))
            .expect("put track data");

        // With two live groups, the second virtual group folds to the other one.
        let tracks = vec![addr(1), addr(2), addr(3), addr(4), addr(9)];
        let response = answer(&ctx, &SyncSlicesRequest { live_groups: 2, ..request(tracks, 1 << 20) }, holder(&ctx))
            .expect("answer");

        assert_eq!(served(&response), [addr(1)]);
        assert_eq!(missing(&response), HashSet::from([addr(2), addr(3), addr(4), addr(9)]));
    }

    // a blacklisted track, or any track on a blacklisted tape, is missing
    #[tokio::test]
    async fn refuses_blacklisted() {
        let ctx = test_context().await;
        hold(&ctx, SPOOL);
        let refused_tape = addr(50);
        seed(&ctx, addr(1), track(own_group()));
        seed(&ctx, addr(2), track(own_group()));
        seed(&ctx, addr(3), CompressedTrack { tape: refused_tape, ..track(own_group()) });
        seed_entry(&ctx.store, ctx.node_address(), TrackNumber(0), BlacklistEntry::track(addr(2)));
        seed_entry(&ctx.store, ctx.node_address(), TrackNumber(1), BlacklistEntry::tape(refused_tape));

        let response = answer(&ctx, &request(vec![addr(1), addr(2), addr(3)], 1 << 20), holder(&ctx)).expect("answer");

        assert_eq!(served(&response), [addr(1)]);
        assert_eq!(missing(&response), HashSet::from([addr(2), addr(3)]));
    }

    // a hidden track's slice goes to a holder in its group and to no one else
    #[tokio::test]
    async fn hidden_for_group_only() {
        let ctx = test_context().await;
        hold(&ctx, SPOOL);
        seed(&ctx, addr(1), CompressedTrack {
            flags: TrackFlags::HIDDEN,
            ..track(own_group())
        });
        let tracks = vec![addr(1)];

        let to_outsider = answer(&ctx, &request(tracks.clone(), 1 << 20), outsider()).expect("answer");
        let to_holder = answer(&ctx, &request(tracks, 1 << 20), holder(&ctx)).expect("answer");

        assert!(to_outsider.entries.is_empty());
        assert_eq!(missing(&to_outsider), HashSet::from([addr(1)]));
        assert_eq!(served(&to_holder), [addr(1)]);
    }

    // a node with no row for any stored spool the request touches refuses it
    #[tokio::test]
    async fn refuses_without_rows() {
        let ctx = test_context().await;
        seed(&ctx, addr(1), track(own_group()));

        let result = answer(&ctx, &request(vec![addr(1)], 1 << 20), holder(&ctx));

        assert!(matches!(result, Err(RouteError::NotResponsible)));
    }

    // a request for more tracks than the shared limit is refused
    #[tokio::test]
    async fn refuses_too_many_tracks() {
        let ctx = test_context().await;
        hold(&ctx, SPOOL);
        let tracks = vec![addr(1); SYNC_MAX_TRACKS + 1];

        let result = answer(&ctx, &request(tracks, 1 << 20), holder(&ctx));

        assert!(matches!(result, Err(RouteError::BadRequest(_))));
    }

    // an inline track this node keeps the payload of
    fn seed_inline(ctx: &TestContext, track_address: Address, track: CompressedTrack, payload: &[u8]) {
        let track = CompressedTrack {
            kind: TrackKind::Inline as u64,
            ..track
        };
        ctx.store.put_track(track_address, track).expect("put track");
        ctx.store
            .put_track_data(track_address, BlobData::Inline(payload.to_vec()))
            .expect("put track data");
    }

    fn inline_request(tracks: Vec<Address>) -> SyncTrackDataRequest {
        SyncTrackDataRequest {
            tracks,
            max_bytes: SYNC_MAX_BYTES,
        }
    }

    fn inline_missing(response: &SyncTrackDataResponse) -> HashSet<Address> {
        response.missing.iter().copied().collect()
    }

    fn inline_served(response: &SyncTrackDataResponse) -> Vec<(Address, Vec<u8>)> {
        response
            .entries
            .iter()
            .map(|entry| (entry.track, entry.payload.clone()))
            .collect()
    }

    // inline payloads go in request order, and a coded or unknown track is missing
    #[tokio::test]
    async fn serves_inline_payloads() {
        let ctx = test_context().await;
        hold(&ctx, SPOOL);
        seed_inline(&ctx, addr(1), track(own_group()), &[1; 4]);
        seed_inline(&ctx, addr(2), track(own_group()), &[2; 8]);
        seed(&ctx, addr(3), track(own_group()));

        let tracks = vec![addr(2), addr(1), addr(3), addr(9)];
        let response = answer_track_data(&ctx, &inline_request(tracks), holder(&ctx)).expect("answer");

        assert_eq!(inline_served(&response), [(addr(2), vec![2; 8]), (addr(1), vec![1; 4])]);
        assert_eq!(inline_missing(&response), HashSet::from([addr(3), addr(9)]));
    }

    // a blacklisted inline track, or one hidden from a caller outside its group, is missing
    #[tokio::test]
    async fn inline_refusals() {
        let ctx = test_context().await;
        hold(&ctx, SPOOL);
        seed_inline(&ctx, addr(1), track(own_group()), &[1; 4]);
        seed_inline(&ctx, addr(2), CompressedTrack { flags: TrackFlags::HIDDEN, ..track(own_group()) }, &[2; 4]);
        seed_entry(&ctx.store, ctx.node_address(), TrackNumber(0), BlacklistEntry::track(addr(1)));
        let tracks = vec![addr(1), addr(2)];

        let to_outsider = answer_track_data(&ctx, &inline_request(tracks.clone()), outsider()).expect("answer");
        let to_holder = answer_track_data(&ctx, &inline_request(tracks), holder(&ctx)).expect("answer");

        assert!(to_outsider.entries.is_empty());
        assert_eq!(inline_missing(&to_outsider), HashSet::from([addr(1), addr(2)]));
        assert_eq!(inline_served(&to_holder), [(addr(2), vec![2; 4])]);
    }

    // a node that holds no spool of the asked-for groups refuses the request
    #[tokio::test]
    async fn inline_refuses_without_rows() {
        let ctx = test_context().await;
        seed_inline(&ctx, addr(1), track(own_group()), &[1; 4]);

        let result = answer_track_data(&ctx, &inline_request(vec![addr(1)]), holder(&ctx));

        assert!(matches!(result, Err(RouteError::NotResponsible)));
    }
}
