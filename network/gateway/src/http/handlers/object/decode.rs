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

pub(in crate::http::handlers::object) struct DecodedObject {
    pub(in crate::http::handlers::object) bytes: Vec<u8>,
    pub(in crate::http::handlers::object) etag: Hash,
}

pub(in crate::http::handlers::object) async fn decode_track_bytes<
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
            return Err(RouteError::BadRequest("track data is not inline".into()));
        };
        if hash(&bytes) != track.value_hash {
            return Err(RouteError::Internal("inline track hash mismatch".into()));
        }

        return Ok(DecodedObject {
            bytes,
            etag: object_etag(&track, None),
        });
    }

    if !track.is_coded() {
        return Err(RouteError::BadRequest("unsupported track kind".into()));
    }

    let BlobData::Coded(blob) = track_data else {
        return Err(RouteError::BadRequest("track data is not blob metadata".into()));
    };
    if blob.get_hash() != track.value_hash || blob.commitment_root() != blob.commitment {
        return Err(RouteError::Internal("blob commitment mismatch".into()));
    }

    let slices = fetch_decoding_slices(state, track_addr, track, blob).await?;
    let mut decoder = BlobDecoder::with_profile(blob.profile);
    let mut bytes = decoder
        .decode(slices)
        .map_err(|error| RouteError::BadGateway(format!("decode object: {error}")))?;
    let logical_size = usize::try_from(blob.size.to_bytes())
        .map_err(|_| RouteError::Internal("object is too large for this platform".into()))?;
    if bytes.len() < logical_size {
        return Err(RouteError::BadGateway("decoded object is truncated".into()));
    }
    bytes.truncate(logical_size);

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

    let mut slices = Vec::with_capacity(k);
    while let Some(result) = fetches.next().await {
        match result {
            Ok((spool_id, data)) => {
                let Some(position) = track.group.position_of(spool_id) else {
                    warn!(spool = %spool_id, track = %track_addr, "gateway skipped slice outside track group");
                    continue;
                };
                if position >= GROUP_SIZE || hash_leaf(&data) != blob.leaves[position] {
                    warn!(spool = %spool_id, track = %track_addr, "gateway skipped slice with mismatched leaf hash");
                    continue;
                }

                slices.push((SpoolIndex(position as u64), data));
                if slices.len() >= k {
                    return Ok(slices);
                }
            }
            Err(error) => {
                debug!(track = %track_addr, ?error, "gateway object slice fetch failed");
            }
        }
    }

    Err(RouteError::BadGateway(
        "insufficient verified slices for object decode".into(),
    ))
}
