use std::fmt::Display;

use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::IntoResponse;

use tape_rpc::Rpc;
use tape_store_api::Store;
use tape_core::erasure::{slice_root_from_sidecar, slice_sidecar, SLICE_TREE_HEIGHT};
use tape_core::track::data::BlobData;
use tape_core::types::{SpoolIndex, TrackNumber};
use tape_crypto::address::Address;
use tape_crypto::merkle::verify_proof_hash;
use tape_protocol::Api;
use tape_protocol::api::{BINARY_CONTENT, BlsSignResponse, SliceBody, encode_slice_window};
use tape_store::ops::{SliceOps, SpoolOps, TrackDataOps};
use tape_store::types::slice::fuse;
use tracing::{debug, trace};

use crate::features::blacklist::refuses_object;
use crate::features::http::auth::{
    local_access_threshold, ActivePeer, MaybeActivePeer, MaybeStakedPeer, StakedPeer,
};
use crate::features::http::error::RouteError;
use crate::features::http::handlers::track::{
    parse_tape, parse_track, resolve_spool_track, resolve_tape_track,
};
use crate::features::http::share::check_share_token;
use crate::features::http::peer_identity::PeerIdentity;
use crate::features::http::state::{AppState, current_epoch};

pub async fn get_slice<Db: Store, Cluster: Api, Blockchain: Rpc>(
    State(state): State<AppState<Db, Cluster, Blockchain>>,
    MaybeStakedPeer(staked_peer): MaybeStakedPeer,
    MaybeActivePeer(active_peer): MaybeActivePeer,
    identity: PeerIdentity,
    Path((track_id, spool_id)): Path<(String, SpoolIndex)>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, RouteError> {
    trace!(track_id = %track_id, spool_id = %spool_id, "http get_slice start");

    let caller = SliceCaller {
        staked_peer,
        active_peer,
        identity,
    };
    let track_key = check_slice_read(&state, caller, &track_id, spool_id, &headers)?;

    let data = state
        .context
        .store
        .get_slice(spool_id, track_key)
        .map_err(store_error)?
        .ok_or(RouteError::NotFound)?;

    state.context.metrics.add_downloaded(data.len() as u64);

    Ok((
        StatusCode::OK,
        [(header::CONTENT_TYPE, BINARY_CONTENT)],
        data,
    ))
}

/// Serve part of a slice with its sidecar, so a reader can check the part alone
pub async fn get_slice_window<Db: Store, Cluster: Api, Blockchain: Rpc>(
    State(state): State<AppState<Db, Cluster, Blockchain>>,
    MaybeStakedPeer(staked_peer): MaybeStakedPeer,
    MaybeActivePeer(active_peer): MaybeActivePeer,
    identity: PeerIdentity,
    Path((track_id, spool_id, offset, len)): Path<(String, SpoolIndex, u64, u64)>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, RouteError> {
    if len == 0 || len > MAX_SLICE_WINDOW_BYTES {
        return Err(RouteError::BadRequest("slice window length out of bounds".into()));
    }
    let (offset, len) = match (usize::try_from(offset), usize::try_from(len)) {
        (Ok(offset), Ok(len)) => (offset, len),
        _ => return Err(RouteError::BadRequest("slice window out of bounds".into())),
    };

    let caller = SliceCaller {
        staked_peer,
        active_peer,
        identity,
    };
    let track_key = check_slice_read(&state, caller, &track_id, spool_id, &headers)?;

    let (sidecar, data) = state
        .context
        .store
        .slice_window(spool_id, track_key, offset, len)
        .map_err(store_error)?
        .ok_or(RouteError::NotFound)?;

    state.context.metrics.add_downloaded(data.len() as u64);

    Ok((
        StatusCode::OK,
        [(header::CONTENT_TYPE, BINARY_CONTENT)],
        encode_slice_window(&sidecar, &data),
    ))
}

/// A window request may ask for at most this many bytes of a slice
const MAX_SLICE_WINDOW_BYTES: u64 = 64 * 1024 * 1024;

/// Who is asking for a slice, as the request extractors saw it
struct SliceCaller {
    staked_peer: Option<StakedPeer>,
    active_peer: Option<ActivePeer>,
    identity: PeerIdentity,
}

/// Check a slice read against staking, spool ownership, the blacklist and share tokens
fn check_slice_read<Db: Store, Cluster: Api, Blockchain: Rpc>(
    state: &AppState<Db, Cluster, Blockchain>,
    caller: SliceCaller,
    track_id: &str,
    spool_id: SpoolIndex,
    headers: &HeaderMap,
) -> Result<Address, RouteError> {
    if local_access_threshold(state).0 > 0 && caller.staked_peer.is_none() {
        return Err(RouteError::Forbidden("staked peer required".into()));
    }

    let track_key = parse_track(track_id)?;

    state
        .context
        .store
        .get_spool_state(spool_id)
        .map_err(store_error)?
        .ok_or(RouteError::NotResponsible)?;

    // Resolve through pending so a freshly written slice serves at confirmed
    // latency instead of waiting for finalization; the blacklist check below only
    // needs the tape, which pending state carries.
    let track = resolve_spool_track(state, track_key, spool_id)?
        .ok_or(RouteError::NotFound)?;

    if refuses_object(
        state.context.store.as_ref(),
        &state.context.blacklist,
        state.context.node_address(),
        state.context.state().epoch(),
        track_key,
        track.tape,
    )
    .map_err(store_error)?
    {
        return Err(RouteError::BlacklistedObject);
    }

    check_share_token(state, &track, caller.identity, caller.active_peer, headers)?;
    Ok(track_key)
}

pub async fn put_slice<Db: Store, Cluster: Api, Blockchain: Rpc>(
    State(state): State<AppState<Db, Cluster, Blockchain>>,
    Path((tape_id, track_number, spool_id)): Path<(String, u64, SpoolIndex)>,
    body: Bytes,
) -> Result<impl IntoResponse, RouteError> {
    trace!(
        tape_id = %tape_id,
        track_number,
        spool_id = %spool_id,
        payload_bytes = body.len(),
        "http put_slice start"
    );

    // The receipt signs one track, which its tape and track number identify.
    let tape = parse_tape(&tape_id)?;
    let track_number = TrackNumber(track_number);
    let epoch = current_epoch(&state)?;
    let payload = SliceBody::parse(&body)
        .map_err(|error| RouteError::BadRequest(format!("slice payload: {error}")))?;

    let (track_key, track) = resolve_tape_track(&state, tape, track_number)?
        .ok_or(RouteError::NotFound)?;

    if refuses_object(
        state.context.store.as_ref(),
        &state.context.blacklist,
        state.context.node_address(),
        epoch,
        track_key,
        track.tape,
    )
    .map_err(store_error)?
    {
        return Err(RouteError::BlacklistedObject);
    }

    if !track.is_coded() {
        return Err(RouteError::BadRequest("raw tracks do not accept slices".into()));
    }

    let track_data = match state.context.pending.track_data(track_key, track_number) {
        Some(data) => data,
        None => state
            .context
            .store
            .get_track_data(track_key, track_number)
            .map_err(store_error)?
            .ok_or(RouteError::NotFound)?,
    };
    let BlobData::Coded(blob) = track_data else {
        return Err(RouteError::BadRequest("track data is not blob metadata".into()));
    };

    // Settle ownership and proof shape before hashing, so a slice we would drop
    // anyway never costs a sub-leaf tree build.
    let spool_state = state
        .context
        .store
        .get_spool_state(spool_id)
        .map_err(store_error)?
        .ok_or(RouteError::NotResponsible)?;

    if spool_state.is_locked() {
        return Err(RouteError::NotResponsible);
    }

    if payload.merkle_proof.len() != SLICE_TREE_HEIGHT {
        return Err(RouteError::BadRequest("invalid merkle proof".into()));
    }

    // The registered leaf is already on hand, so settle the claim with a 32 byte
    // compare before hashing the slice to check the claim is honest.
    let leaf_pos = track
        .group
        .position_of(spool_id)
        .ok_or_else(|| RouteError::BadRequest("spool outside the track's group".into()))?;
    if payload.leaf_hash != blob.leaves[leaf_pos] {
        return Err(RouteError::BadRequest("leaf hash mismatch".into()));
    }

    // One hash of the slice serves both jobs: the root that settles the claim,
    // and the sidecar the store keeps so a later challenge answers off one window.
    let data = payload.data;
    let data_len = data.len() as u64;
    let sidecar = slice_sidecar(&data);
    let root = sidecar.as_deref().map(slice_root_from_sidecar);
    let value = fuse(&data, sidecar.as_deref());
    let Some(root) = root else {
        return Err(RouteError::BadRequest("slice exceeds sub-leaf tree capacity".into()));
    };
    if root != payload.leaf_hash {
        return Err(RouteError::BadRequest("leaf hash mismatch".into()));
    }

    if !verify_proof_hash(
        root,
        &blob.commitment,
        &payload.merkle_proof,
        leaf_pos as u64,
        SLICE_TREE_HEIGHT,
    ) {
        return Err(RouteError::BadRequest("invalid merkle proof".into()));
    }

    state
        .context
        .store
        .put_fused_slice_wait(spool_id, track_key, value)
        .await
        .map_err(store_error)?;
    state.context.metrics.add_uploaded(data_len);

    let signature = state
        .context
        .sign_track_write(epoch, track.get_hash())
        .map_err(|error| RouteError::Internal(format!("bls sign: {error:?}")))?;

    let receipt = BlsSignResponse {
        signature,
        node: state.context.node_address(),
        epoch,
    };
    let bytes = wincode::serialize(&receipt)
        .map_err(|error| RouteError::Internal(format!("serialize slice receipt: {error}")))?;

    debug!(track = %track_key, spool_id = %spool_id, "http put_slice success");

    Ok((
        StatusCode::OK,
        [(header::CONTENT_TYPE, BINARY_CONTENT)],
        bytes,
    ))
}

fn store_error(error: impl Display) -> RouteError {
    RouteError::Internal(error.to_string())
}

#[cfg(test)]
mod tests {
    use axum::body::to_bytes;
    use axum::extract::{Path, State};
    use axum::http::StatusCode;
    use axum::response::IntoResponse;
    use bytemuck::Zeroable;

    use tape_api::program::tapedrive::{snapshot_tape_pda, TrackAddress};
    use tape_core::cert::track::TrackWriteMessage;
    use tape_core::erasure::{slice_root, MAX_GROUP_SIZE, SLICE_TREE_HEIGHT};
    use tape_core::prelude::{SpoolState, SpoolStatus};
    use tape_snapshot::snapshot_chunk_key;
    use tape_core::spooler::GroupIndex;
    use tape_core::tape::{snapshot_tape_number, TapeFlags};
    use tape_core::track::blob::BlobEncoding;
    use tape_core::track::data::BlobData;
    use tape_core::track::types::{CompressedTrack, TrackFlags, TrackKind, TrackState};
    use tape_core::types::coin::TAPE;
    use tape_core::types::{
        ChunkNumber, EpochNumber, SlotNumber, StorageUnits, StripeCount, TapeNumber,
        TrackNumber,
    };
    use tape_core::share::{unix_now, ShareToken};
    use tape_core::types::tls::NetworkTlsPubkey;
    use tape_crypto::ed25519::{Keypair, SecretKey};
    use tape_crypto::Hash;
    use tape_crypto::merkle::{create_proof_from_leaf_hashes, root_from_leaf_hashes};
    use tape_protocol::api::{encode_share_token, SlicePayload, SHARE_TOKEN_HEADER};
    use tape_store::ops::{ObjectInfoOps, SpoolOps, TapeOps, TrackDataOps, TrackOps};
    use tape_store::types::{ObjectInfo, SystemObjectKind, TapeInfo};

    use super::*;
    use crate::features::http::state::AppState;
    use crate::harness::{NodeHarness, TestContext};

    async fn test_context() -> TestContext {
        NodeHarness::builder()
            .nodes(25)
            .no_prev_snapshot_tape()
            .build()
            .await
            .expect("build harness")
            .ctx_for(0)
    }

    fn seed_projected_snapshot_track(ctx: &TestContext) -> (Address, SpoolIndex, Vec<u8>) {
        let epoch = EpochNumber(5);

        let group = GroupIndex(2);
        let track_number = TrackNumber(9);
        let owned_spool = group.spool_at(5);
        let slice_bytes = vec![0xAB; 96];

        let leaves = [Hash::from([0x44; 32]); MAX_GROUP_SIZE];
        let commitment = root_from_leaf_hashes::<SLICE_TREE_HEIGHT>(&leaves);
        let blob = BlobEncoding {
            size: StorageUnits::from_bytes(1_537),
            commitment,
            stripe_size: StorageUnits::from_bytes(512),
            stripe_count: StripeCount(4),
            leaves,
        };

        let (snapshot_tape, _) = snapshot_tape_pda(epoch);

        ctx.store
            .put_tape(
                snapshot_tape,
                TapeInfo {
                    id: snapshot_tape_number(epoch),
                    flags: TapeFlags::SYSTEM,
                    end_epoch: EpochNumber(u64::MAX),
                    next_track_number: TrackNumber(track_number.0 + 1),
                    authority: Address::default(),
                    delegate: Address::default(),
                    share_nonce: Hash::default(),
                },
            )
            .expect("seed snapshot tape");

        let track = CompressedTrack {
            tape: snapshot_tape,
            key: snapshot_chunk_key(epoch, group, ChunkNumber(0)),
            track_number,
            kind: TrackKind::Coded as u64,
            state: TrackState::Certified as u64,
            flags: TrackFlags::NONE,
            size: blob.size,
            group,
            value_hash: blob.get_hash(),
        };
        let track_address = track.address();

        ctx.store
            .put_track(track_address, track)
            .expect("seed track");

        ctx.store
            .put_track_data(track_address, track_number, BlobData::Coded(blob))
            .expect("seed track data");

        ctx.store
            .put_object_info(
                track_address,
                track_number,
                ObjectInfo::System {
                    kind: SystemObjectKind::Snapshot {
                        epoch,
                    },
                    track_address,
                    registered_epoch: epoch,
                    certified_epoch: Some(epoch),
                    slot: SlotNumber(epoch.0),
                },
            )
            .expect("seed object info");
        ctx.store
            .put_slice(owned_spool, track_address, slice_bytes.clone())
            .expect("seed slice");
        ctx.store
            .set_spool_state(
                owned_spool,
                SpoolState::new(SpoolStatus::Active, EpochNumber(0)),
            )
            .expect("set spool state");

        (track_address, owned_spool, slice_bytes)
    }


    const HIDDEN_SLICE: &[u8] = &[0xCD; 96];

    fn signing_key() -> Keypair {
        Keypair::from_secret(SecretKey::from_bytes([0x31; 32]))
    }

    fn reader_identity() -> NetworkTlsPubkey {
        NetworkTlsPubkey::new([0x5B; 32])
    }

    fn staked_caller(ctx: &TestContext) -> MaybeStakedPeer {
        MaybeStakedPeer(Some(crate::features::http::auth::StakedPeer {
            node: ctx.node_address(),
            tls_pubkey: ctx.tls_pubkey(),
            stake: TAPE(1),
        }))
    }

    fn seed_hidden_track(ctx: &TestContext, authority: Address) -> (Address, SpoolIndex, Address, Hash) {
        let group = GroupIndex(2);
        let track_number = TrackNumber(3);
        let owned_spool = group.spool_at(4);
        let tape = Address::new_unique();
        let key = Hash::from([0x6C; 32]);

        ctx.store
            .put_tape(
                tape,
                TapeInfo {
                    id: TapeNumber(11),
                    flags: 0,
                    end_epoch: EpochNumber(u64::MAX),
                    next_track_number: TrackNumber(track_number.0 + 1),
                    authority,
                    delegate: Address::default(),
                    share_nonce: Hash::default(),
                },
            )
            .expect("seed tape");

        let leaves = [Hash::from([0x44; 32]); MAX_GROUP_SIZE];
        let commitment = root_from_leaf_hashes::<SLICE_TREE_HEIGHT>(&leaves);
        let blob = BlobEncoding {
            size: StorageUnits::from_bytes(1_537),
            commitment,
            stripe_size: StorageUnits::from_bytes(512),
            stripe_count: StripeCount(4),
            leaves,
        };

        let track = CompressedTrack {
            tape,
            key,
            track_number,
            kind: TrackKind::Coded as u64,
            state: TrackState::Certified as u64,
            flags: TrackFlags::HIDDEN,
            size: blob.size,
            group,
            value_hash: blob.get_hash(),
        };
        let track_address = track.address();

        ctx.store
            .put_track(track_address, track)
            .expect("seed track");
        ctx.store
            .put_track_data(track_address, track_number, BlobData::Coded(blob))
            .expect("seed track data");
        ctx.store
            .put_slice(owned_spool, track_address, HIDDEN_SLICE.to_vec())
            .expect("seed slice");
        ctx.store
            .set_spool_state(
                owned_spool,
                SpoolState::new(SpoolStatus::Active, EpochNumber(0)),
            )
            .expect("set spool state");

        (track_address, owned_spool, tape, key)
    }

    fn token_headers(token: &ShareToken) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(
            SHARE_TOKEN_HEADER,
            encode_share_token(token).parse().expect("header value"),
        );

        headers
    }

    async fn hidden_read(
        ctx: &TestContext,
        track_address: Address,
        spool: SpoolIndex,
        identity: PeerIdentity,
        headers: HeaderMap,
    ) -> Result<axum::response::Response, RouteError> {
        get_slice(
            State(AppState {
                context: ctx.clone(),
            }),
            staked_caller(ctx),
            MaybeActivePeer(None),
            identity,
            Path((track_address.to_string(), spool)),
            headers,
        )
        .await
        .map(IntoResponse::into_response)
    }

    // a hidden track is refused when the caller presents no share token
    #[tokio::test]
    async fn needs_token() {
        let ctx = test_context().await;
        let (track_address, spool, _tape, _key) =
            seed_hidden_track(&ctx, signing_key().pubkey().address());

        let result = hidden_read(
            &ctx,
            track_address,
            spool,
            PeerIdentity::authenticated(reader_identity()),
            HeaderMap::new(),
        )
        .await;

        assert!(matches!(result, Err(RouteError::Forbidden(_))));
    }

    // a hidden track serves to the reader of an unexpired share token
    #[tokio::test]
    async fn valid_token() {
        let ctx = test_context().await;
        let signer = signing_key();
        let (track_address, spool, tape, key) = seed_hidden_track(&ctx, signer.pubkey().address());
        let token = ShareToken::issue(
            &signer,
            tape,
            key,
            reader_identity(),
            Hash::default(),
            unix_now() + 3_600,
        );

        let response = hidden_read(
            &ctx,
            track_address,
            spool,
            PeerIdentity::authenticated(reader_identity()),
            token_headers(&token),
        )
        .await
        .expect("shared read");

        assert_eq!(response.status(), StatusCode::OK);

        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("read response body");
        assert_eq!(body.as_ref(), HIDDEN_SLICE);
    }

    // an expired share token is refused
    #[tokio::test]
    async fn expired_token() {
        let ctx = test_context().await;
        let signer = signing_key();
        let (track_address, spool, tape, key) = seed_hidden_track(&ctx, signer.pubkey().address());
        let token = ShareToken::issue(&signer, tape, key, reader_identity(), Hash::default(), 1);

        let result = hidden_read(
            &ctx,
            track_address,
            spool,
            PeerIdentity::authenticated(reader_identity()),
            token_headers(&token),
        )
        .await;

        assert!(matches!(result, Err(RouteError::Forbidden(_))));
    }

    // advancing the tape's share nonce revokes a token already issued
    #[tokio::test]
    async fn revoked_token() {
        let ctx = test_context().await;
        let signer = signing_key();
        let (track_address, spool, tape, key) = seed_hidden_track(&ctx, signer.pubkey().address());
        let token = ShareToken::issue(
            &signer,
            tape,
            key,
            reader_identity(),
            Hash::default(),
            unix_now() + 3_600,
        );

        let mut info = ctx.store.get_tape(tape).expect("tape").expect("tape");
        info.share_nonce = Hash::new_unique();
        ctx.store.put_tape(tape, info).expect("advance share nonce");

        let result = hidden_read(
            &ctx,
            track_address,
            spool,
            PeerIdentity::authenticated(reader_identity()),
            token_headers(&token),
        )
        .await;

        assert!(matches!(result, Err(RouteError::Forbidden(_))));
    }

    // a share token is refused for any other reader
    #[tokio::test]
    async fn wrong_reader() {
        let ctx = test_context().await;
        let signer = signing_key();
        let (track_address, spool, tape, key) = seed_hidden_track(&ctx, signer.pubkey().address());
        let token = ShareToken::issue(
            &signer,
            tape,
            key,
            reader_identity(),
            Hash::default(),
            unix_now() + 3_600,
        );

        let result = hidden_read(
            &ctx,
            track_address,
            spool,
            PeerIdentity::authenticated(NetworkTlsPubkey::new([0x77; 32])),
            token_headers(&token),
        )
        .await;

        assert!(matches!(result, Err(RouteError::Forbidden(_))));
    }

    #[tokio::test]
    async fn serves_slice() {
        let ctx = test_context().await;
        let (track_address, owned_spool, slice_bytes) = seed_projected_snapshot_track(&ctx);

        let result = get_slice(
            State(AppState {
                context: ctx.clone(),
            }),
            MaybeStakedPeer(Some(crate::features::http::auth::StakedPeer {
                node: ctx.node_address(),
                tls_pubkey: ctx.tls_pubkey(),
                stake: TAPE(1),
            })),
            MaybeActivePeer(None),
            PeerIdentity::anonymous(),
            Path((track_address.to_string(), owned_spool)),
            HeaderMap::new(),
        )
        .await;
        let response = match result {
            Ok(response) => response.into_response(),
            Err(_) => panic!("get_slice failed for projected snapshot track"),
        };

        assert_eq!(response.status(), StatusCode::OK);

        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("read response body");

        assert_eq!(body.as_ref(), slice_bytes.as_slice());
    }

    fn seed_pending_slice(ctx: &TestContext) -> (Address, SpoolIndex, Vec<u8>) {
        let tape = Address::new_unique();
        let group = GroupIndex(2);
        let track_number = TrackNumber(3);
        let owned_spool = group.spool_at(5);
        let slice_bytes = vec![0xCD; 96];

        // The blob contents do not matter here: get_slice serves the raw bytes and
        // never decodes or verifies, so a zeroed placeholder is enough.
        let blob = BlobEncoding::zeroed();

        let track = CompressedTrack {
            tape,
            key: Hash::new_unique(),
            track_number,
            kind: TrackKind::Coded as u64,
            state: TrackState::Registered as u64,
            flags: TrackFlags::NONE,
            size: blob.size,
            group,
            value_hash: blob.get_hash(),
        };
        let track_address = track.address();

        // Slice bytes and spool ownership land in the store at put time.
        ctx.store
            .put_slice(owned_spool, track_address, slice_bytes.clone())
            .expect("seed slice");
        ctx.store
            .set_spool_state(
                owned_spool,
                SpoolState::new(SpoolStatus::Active, EpochNumber(0)),
            )
            .expect("set spool state");

        // The record lives only in pending, as it would between confirmation and
        // finalization, and is never written to disk.
        ctx.pending
            .apply_register(SlotNumber(100), track_address, track, BlobData::Coded(blob));

        (track_address, owned_spool, slice_bytes)
    }

    // a slice serves while its record is still pending, not yet finalized to disk
    #[tokio::test]
    async fn serves_pending() {
        let ctx = test_context().await;
        let (track_address, owned_spool, slice_bytes) = seed_pending_slice(&ctx);

        assert!(ctx.store.latest_track(track_address).unwrap().is_none());

        let result = get_slice(
            State(AppState {
                context: ctx.clone(),
            }),
            MaybeStakedPeer(Some(crate::features::http::auth::StakedPeer {
                node: ctx.node_address(),
                tls_pubkey: ctx.tls_pubkey(),
                stake: TAPE(1),
            })),
            MaybeActivePeer(None),
            PeerIdentity::anonymous(),
            Path((track_address.to_string(), owned_spool)),
            HeaderMap::new(),
        )
        .await;
        let response = match result {
            Ok(response) => response.into_response(),
            Err(_) => panic!("get_slice should serve a pending-only track"),
        };

        assert_eq!(response.status(), StatusCode::OK);

        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("read response body");

        assert_eq!(body.as_ref(), slice_bytes.as_slice());
    }

    // A coded track whose commitment matches a real slice, so put_slice accepts
    // it and answers with the certify signature.
    fn seed_writable_track(ctx: &TestContext) -> (Address, SpoolIndex, SlicePayload) {
        let epoch = ctx.state().epoch();
        let group = GroupIndex(2);
        let track_number = TrackNumber(11);
        let owned_spool = group.spool_at(5);
        let leaf_pos = group.position_of(owned_spool).expect("spool in group");

        let slice_bytes = vec![0x5C; 128];
        let leaf = slice_root(&slice_bytes).expect("slice root");

        let mut leaves = [Hash::from([0x11; 32]); MAX_GROUP_SIZE];
        leaves[leaf_pos] = leaf;
        let commitment = root_from_leaf_hashes::<SLICE_TREE_HEIGHT>(&leaves);
        let merkle_proof = create_proof_from_leaf_hashes::<SLICE_TREE_HEIGHT>(&leaves, leaf_pos)
            .expect("slice proof");

        let blob = BlobEncoding {
            size: StorageUnits::from_bytes(slice_bytes.len() as u64),
            commitment,
            stripe_size: StorageUnits::from_bytes(512),
            stripe_count: StripeCount(1),
            leaves,
        };

        let tape = Address::new_unique();
        let track = CompressedTrack {
            tape,
            key: Hash::from([0x22; 32]),
            track_number,
            kind: TrackKind::Coded as u64,
            state: TrackState::Registered as u64,
            flags: TrackFlags::NONE,
            size: blob.size,
            group,
            value_hash: blob.get_hash(),
        };
        let track_address = track.address();

        ctx.store
            .put_tape(
                tape,
                TapeInfo {
                    id: TapeNumber(7),
                    flags: 0,
                    end_epoch: EpochNumber(u64::MAX),
                    next_track_number: TrackNumber(track_number.0 + 1),
                    authority: Address::default(),
                    delegate: Address::default(),
                    share_nonce: Hash::default(),
                },
            )
            .expect("seed tape");

        ctx.store
            .put_track(track_address, track)
            .expect("seed track");

        ctx.store
            .put_track_data(track_address, track_number, BlobData::Coded(blob))
            .expect("seed track data");

        ctx.store
            .set_spool_state(owned_spool, SpoolState::new(SpoolStatus::Active, epoch))
            .expect("set spool state");

        (
            track_address,
            owned_spool,
            SlicePayload::new(slice_bytes, leaf, merkle_proof),
        )
    }

    #[tokio::test]
    async fn stored_slice_answers_with_a_certify_receipt() {
        let ctx = test_context().await;
        let (track_address, owned_spool, payload) = seed_writable_track(&ctx);
        let track = ctx.store.latest_track(track_address).expect("track").expect("track present");

        let body = wincode::serialize(&payload).expect("serialize payload");
        let response = put_slice(
            State(AppState { context: ctx.clone() }),
            Path((track.tape.to_string(), track.track_number.0, owned_spool)),
            body.into(),
        )
        .await
        .expect("put_slice")
        .into_response();

        assert_eq!(response.status(), StatusCode::OK);

        let bytes = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("read body");
        let receipt: BlsSignResponse = wincode::deserialize(&bytes).expect("receipt");

        assert_eq!(receipt.node, ctx.node_address());
        assert_eq!(receipt.epoch, ctx.state().epoch());

        // The signature has to be the one the certify route would have issued.
        let message = TrackWriteMessage::new(receipt.epoch, track.get_hash());
        let pubkey = ctx.bls_pubkey().expect("bls pubkey");
        receipt
            .signature
            .verify_aggregate(message.to_bytes(), &[pubkey])
            .expect("receipt signs the track write message");
    }

    #[tokio::test]
    async fn a_receipt_does_not_verify_against_another_epoch() {
        let ctx = test_context().await;
        let (track_address, owned_spool, payload) = seed_writable_track(&ctx);
        let track = ctx.store.latest_track(track_address).expect("track").expect("track present");

        let body = wincode::serialize(&payload).expect("serialize payload");
        let response = put_slice(
            State(AppState { context: ctx.clone() }),
            Path((track.tape.to_string(), track.track_number.0, owned_spool)),
            body.into(),
        )
        .await
        .expect("put_slice")
        .into_response();

        let bytes = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("read body");
        let receipt: BlsSignResponse = wincode::deserialize(&bytes).expect("receipt");

        let wrong = TrackWriteMessage::new(EpochNumber(receipt.epoch.0 + 1), track.get_hash());
        let pubkey = ctx.bls_pubkey().expect("bls pubkey");
        assert!(
            receipt
                .signature
                .verify_aggregate(wrong.to_bytes(), &[pubkey])
                .is_err()
        );
    }
}

