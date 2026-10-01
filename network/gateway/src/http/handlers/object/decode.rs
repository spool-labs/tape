use std::collections::HashMap;

use bytes::Bytes;
use tape_rpc::Rpc;
use tape_store_api::Store;
use tape_core::object::object_etag;
use tape_core::track::blob::BlobEncoding;
use tape_core::track::data::BlobData;
use tape_core::track::types::CompressedTrack;
use tape_core::types::SpoolIndex;
use tape_crypto::Hash;
use tape_crypto::address::Address;
use tape_crypto::hash::hash;
use tape_protocol::inline::{fetch_inline_from_peers, InlineFetchError};
use tape_protocol::Api;
use tape_sdk::codec::decoder::BlobDecoder;
use tape_store::ops::TrackDataOps;
use tokio::task::spawn_blocking;
use tracing::{debug, warn};

use super::hedge::fetch_hedged;
use crate::http::error::RouteError;
use crate::http::handlers::store_error;
use crate::http::handlers::track::slice::{SliceOrigin, read_resolved_slice};
use crate::http::handlers::track::track_data_with_pending;
use crate::http::state::AppState;

pub struct DecodedObject {
    pub bytes: Vec<u8>,
    pub etag: Hash,
}

pub async fn decode_track_bytes<
    Db: Store,
    Cluster: Api,
    Blockchain: Rpc,
>(
    state: &AppState<Db, Cluster, Blockchain>,
    track_addr: Address,
    track: CompressedTrack,
) -> Result<DecodedObject, RouteError> {
    if track.is_inline() {
        return decode_inline(state, track_addr, track).await;
    }

    if !track.is_coded() {
        return Err(RouteError::BadRequest("unsupported track kind".into()));
    }

    let blob = coded_blob(state, track_addr, &track)?;
    let slices = fetch_decoding_slices(state, track_addr, track, blob).await?;
    let profile = state.context.params().profile;
    let start = std::time::Instant::now();
    let decoded = spawn_blocking(move || {
        let refs: Vec<(SpoolIndex, &[u8])> = slices.iter().map(|(position, data)| (*position, data.as_ref())).collect();
        BlobDecoder::try_new(profile)?.decode(&refs)
    })
    .await
    .map_err(|error| RouteError::Internal(format!("decode task: {error}")))?;
    let mut bytes = decoded.map_err(|error| {
        crate::metrics::inc_decode_result("decode_error");
        RouteError::BadGateway(format!("decode object: {error}"))
    })?;
    let logical_size = usize::try_from(blob.size.to_bytes())
        .map_err(|_| RouteError::Internal("object is too large for this platform".into()))?;
    if bytes.len() < logical_size {
        crate::metrics::inc_decode_result("truncated");
        return Err(RouteError::BadGateway("decoded object is truncated".into()));
    }
    bytes.truncate(logical_size);

    crate::metrics::observe_decode("coded", start.elapsed().as_secs_f64(), bytes.len());
    crate::metrics::inc_decode_result("ok");
    Ok(DecodedObject {
        bytes,
        etag: object_etag(&track, Some(&blob)),
    })
}

/// The blob encoding behind a coded track, checked against the track's commitment
pub fn coded_blob<Db: Store, Cluster: Api, Blockchain: Rpc>(
    state: &AppState<Db, Cluster, Blockchain>,
    track_addr: Address,
    track: &CompressedTrack,
) -> Result<BlobEncoding, RouteError> {
    let track_data = track_data_with_pending(state, track_addr, track.tape)?.ok_or(RouteError::NotFound)?;
    let BlobData::Coded(blob) = track_data else {
        crate::metrics::inc_decode_result("data_mismatch");
        return Err(RouteError::BadRequest("track data is not blob metadata".into()));
    };
    let width = state.context.params().n();
    if blob.get_hash() != track.value_hash || blob.commitment_root(width) != Some(blob.commitment) {
        crate::metrics::inc_decode_result("commitment_mismatch");
        return Err(RouteError::Internal("blob commitment mismatch".into()));
    }
    Ok(blob)
}

/// Serve an inline object, falling back to a peer fetch when the bytes are not
/// held locally.
///
/// Snapshots carry the track record but never its content, so a mirror rebuilt
/// from them holds inline records with no bytes. The group owners keep the
/// payload; fetch it from one of them, verify, and cache for later reads. This
/// mirrors the slice-fetch fallback for coded tracks.
async fn decode_inline<Db: Store, Cluster: Api, Blockchain: Rpc>(
    state: &AppState<Db, Cluster, Blockchain>,
    track_addr: Address,
    track: CompressedTrack,
) -> Result<DecodedObject, RouteError> {
    let bytes = match track_data_with_pending(state, track_addr, track.tape)? {
        Some(BlobData::Inline(bytes)) => {
            // The peer-fetch path verifies before caching, so only the local copy
            // needs checking here; either way the bytes are hashed exactly once.
            if hash(&bytes) != track.value_hash {
                crate::metrics::inc_decode_result("inline_hash_mismatch");
                return Err(RouteError::Internal("inline track hash mismatch".into()));
            }
            bytes
        }
        Some(BlobData::Coded(_)) => {
            crate::metrics::inc_decode_result("data_mismatch");
            return Err(RouteError::BadRequest("track data is not inline".into()));
        }
        None => fetch_and_cache_inline(state, track_addr, track).await?,
    };

    crate::metrics::inc_decode_result("ok");
    crate::metrics::add_output_bytes(bytes.len());
    Ok(DecodedObject {
        bytes,
        etag: object_etag(&track, None),
    })
}

async fn fetch_and_cache_inline<Db: Store, Cluster: Api, Blockchain: Rpc>(
    state: &AppState<Db, Cluster, Blockchain>,
    track_addr: Address,
    track: CompressedTrack,
) -> Result<Vec<u8>, RouteError> {
    let peers = state.context.state().group_peers(track.group);
    let fetched =
        fetch_inline_from_peers(state.context.api.as_ref(), &peers, track_addr, track.value_hash).await;
    let payload = match fetched {
        Ok(payload) => payload,
        Err(InlineFetchError::NoPeers { .. }) => return Err(RouteError::NotFound),
        Err(InlineFetchError::Unavailable { .. }) => {
            crate::metrics::inc_decode_result("inline_unavailable");
            return Err(RouteError::NotFound);
        }
    };

    // Cache so later reads serve locally, the same state live tailing leaves,
    // unless the track was removed while its bytes were in flight.
    let cached = state.blacklists.write_unless_denied(track_addr, track.tape, || {
        state
            .context
            .store
            .put_track_data(track_addr, BlobData::Inline(payload.bytes.clone()))
    });
    match cached {
        Some(result) => result.map_err(store_error)?,
        None => return Err(RouteError::Gone),
    }

    debug!(track = %track_addr, node = %payload.node, bytes = payload.bytes.len(), "gateway fetched inline payload from peer");
    Ok(payload.bytes)
}

async fn fetch_decoding_slices<Db: Store, Cluster: Api, Blockchain: Rpc>(
    state: &AppState<Db, Cluster, Blockchain>,
    track_addr: Address,
    track: CompressedTrack,
    blob: BlobEncoding,
) -> Result<Vec<(SpoolIndex, Bytes)>, RouteError> {
    let k = state.context.params().k();

    let peers = state.context.state().group_peers(track.group);
    if peers.is_empty() {
        return Err(RouteError::BadGateway("no peers for track group".into()));
    }

    let origin = SliceOrigin {
        track_addr,
        tape: track.tape,
        group: track.group,
        blob,
    };
    let candidates = fetch_order(state, track_addr, peers);
    let (slices, failures) = fetch_hedged(&candidates, k, |(spool_id, owner)| {
        fetch_decoding_slice(state, &origin, spool_id, owner)
    })
    .await;

    // Tally slice outcomes locally and record them once per decode, not per
    // slice, to keep metric label lookups off the read hot path.
    let (mut rejected_group, mut rejected_leaf, mut fetch_failed) = (0u64, 0u64, 0u64);
    for failure in failures {
        match failure {
            SliceFailure::Group => rejected_group += 1,
            SliceFailure::Leaf => rejected_leaf += 1,
            SliceFailure::Fetch => fetch_failed += 1,
        }
    }

    for (label, count) in [
        ("used", slices.len() as u64),
        ("rejected_group", rejected_group),
        ("rejected_leaf", rejected_leaf),
        ("fetch_failed", fetch_failed),
    ] {
        if count > 0 {
            crate::metrics::inc_decode_slices(label, count);
        }
    }

    if slices.len() >= k {
        return Ok(slices);
    }

    crate::metrics::inc_decode_result("insufficient_slices");
    Err(RouteError::BadGateway(
        "insufficient verified slices for object decode".into(),
    ))
}

/// Why one slice did not count toward a decode
#[derive(Clone, Copy, Debug)]
pub enum SliceFailure {
    Group,
    Leaf,
    Fetch,
}

/// Cached slices first since they cost no transfer, then owners best first
pub fn fetch_order<Db: Store, Cluster: Api, Blockchain: Rpc>(
    state: &AppState<Db, Cluster, Blockchain>,
    track_addr: Address,
    mut peers: Vec<(SpoolIndex, Address)>,
) -> Vec<(SpoolIndex, Address)> {
    let owners: Vec<Address> = peers.iter().map(|(_, owner)| *owner).collect();
    let mut ranks = HashMap::with_capacity(owners.len());
    for (rank, owner) in state.reputation.order(&owners).into_iter().enumerate() {
        ranks.entry(owner).or_insert(rank);
    }
    peers.sort_by_cached_key(|(spool_id, owner)| {
        let is_cached = state.slice_cache.holds(*spool_id, track_addr);
        (!is_cached, ranks.get(owner).copied().unwrap_or(usize::MAX), *spool_id)
    });
    peers
}

async fn fetch_decoding_slice<Db: Store, Cluster: Api, Blockchain: Rpc>(
    state: &AppState<Db, Cluster, Blockchain>,
    origin: &SliceOrigin,
    spool_id: SpoolIndex,
    owner: Address,
) -> Result<(SpoolIndex, Bytes), SliceFailure> {
    let track_addr = origin.track_addr;
    let read = match read_resolved_slice(state, origin, spool_id, owner).await {
        Ok(read) => read,
        Err(error) => {
            debug!(track = %track_addr, ?error, "gateway object slice fetch failed");
            return Err(SliceFailure::Fetch);
        }
    };
    let Some(position) = origin.group.position_of(spool_id) else {
        warn!(spool = %spool_id, track = %track_addr, "gateway skipped slice outside track group");
        return Err(SliceFailure::Group);
    };

    // A slice fetched here was checked on the way in, one left by an earlier process is checked once
    if !read.is_verified {
        if !origin.blob.verify_slice(SpoolIndex(position as u64), &read.data) {
            warn!(spool = %spool_id, track = %track_addr, "gateway skipped slice with mismatched leaf hash");
            return Err(SliceFailure::Leaf);
        }
        if let Err(error) = state.slice_cache.mark_verified(spool_id, track_addr) {
            debug!(%error, "gateway could not mark slice verified");
        }
    }

    Ok((SpoolIndex(position as u64), read.data))
}
