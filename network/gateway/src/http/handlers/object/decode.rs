use futures::stream::{FuturesUnordered, StreamExt};
use rpc::Rpc;
use store::Store;
use tape_core::erasure::GROUP_SIZE;
use tape_core::object::object_etag;
use tape_core::track::blob::BlobEncoding;
use tape_core::track::data::BlobData;
use tape_core::track::types::CompressedTrack;
use tape_core::types::SpoolIndex;
use tape_crypto::Hash;
use tape_crypto::address::Address;
use tape_crypto::hash::hash;
use tape_crypto::merkle::hash_leaf;
use tape_protocol::Api;
use tape_sdk::codec::decoder::BlobDecoder;
use tracing::{debug, warn};

use crate::http::error::RouteError;
use crate::http::handlers::track::slice::read_cached_slice;
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
    let track_data = track_data_with_pending(state, track_addr)?.ok_or(RouteError::NotFound)?;

    if track.is_inline() {
        let BlobData::Inline(bytes) = track_data else {
            crate::metrics::inc_decode_result("data_mismatch");
            return Err(RouteError::BadRequest("track data is not inline".into()));
        };
        if hash(&bytes) != track.value_hash {
            crate::metrics::inc_decode_result("inline_hash_mismatch");
            return Err(RouteError::Internal("inline track hash mismatch".into()));
        }

        crate::metrics::inc_decode_result("ok");
        crate::metrics::add_output_bytes(bytes.len());
        return Ok(DecodedObject {
            bytes,
            etag: object_etag(&track, None),
        });
    }

    if !track.is_coded() {
        return Err(RouteError::BadRequest("unsupported track kind".into()));
    }

    let BlobData::Coded(blob) = track_data else {
        crate::metrics::inc_decode_result("data_mismatch");
        return Err(RouteError::BadRequest("track data is not blob metadata".into()));
    };
    if blob.get_hash() != track.value_hash || blob.commitment_root() != blob.commitment {
        crate::metrics::inc_decode_result("commitment_mismatch");
        return Err(RouteError::Internal("blob commitment mismatch".into()));
    }

    let slices = fetch_decoding_slices(state, track_addr, track, blob).await?;
    let mut decoder = BlobDecoder::with_profile(blob.profile);
    let start = std::time::Instant::now();
    let mut bytes = decoder.decode(slices).map_err(|error| {
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

async fn fetch_decoding_slices<Db: Store, Cluster: Api, Blockchain: Rpc>(
    state: &AppState<Db, Cluster, Blockchain>,
    track_addr: Address,
    track: CompressedTrack,
    blob: BlobEncoding,
) -> Result<Vec<(SpoolIndex, Vec<u8>)>, RouteError> {
    let k = blob.profile.k() as usize;
    if k == 0 || k > GROUP_SIZE {
        return Err(RouteError::Internal("invalid blob encoding profile".into()));
    }

    let peers = state.context.state().group_peers(track.group);
    if peers.is_empty() {
        return Err(RouteError::BadGateway("no peers for track group".into()));
    }

    let mut fetches = FuturesUnordered::new();
    for (spool_id, _) in peers {
        fetches.push(async move {
            let read = read_cached_slice(state, track_addr, spool_id).await?;
            Ok::<_, RouteError>((spool_id, read.data))
        });
    }

    // Tally slice outcomes locally and record them once per decode, not per
    // slice, to keep metric label lookups off the read hot path.
    let mut slices = Vec::with_capacity(k);
    let (mut rejected_group, mut rejected_leaf, mut fetch_failed) = (0u64, 0u64, 0u64);
    while let Some(result) = fetches.next().await {
        match result {
            Ok((spool_id, data)) => {
                let Some(position) = track.group.position_of(spool_id) else {
                    rejected_group += 1;
                    warn!(spool = %spool_id, track = %track_addr, "gateway skipped slice outside track group");
                    continue;
                };
                if position >= GROUP_SIZE || hash_leaf(&data) != blob.leaves[position] {
                    rejected_leaf += 1;
                    warn!(spool = %spool_id, track = %track_addr, "gateway skipped slice with mismatched leaf hash");
                    continue;
                }

                slices.push((SpoolIndex(position as u64), data));
                if slices.len() >= k {
                    break;
                }
            }
            Err(error) => {
                fetch_failed += 1;
                debug!(track = %track_addr, ?error, "gateway object slice fetch failed");
            }
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
