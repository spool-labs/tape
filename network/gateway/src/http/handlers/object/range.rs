//! Range reads that decode only the stripes a byte range covers

use std::time::Instant;

use axum::body::Bytes;
use tape_core::track::blob::BlobEncoding;
use tape_core::track::types::CompressedTrack;
use tape_core::types::{SpoolIndex, StorageUnits};
use tape_crypto::address::Address;
use tape_protocol::Api;
use tape_protocol::api::GetSliceWindowReq;
use tape_rpc::Rpc;
use tape_sdk::bootstrap::counts_against_peer;
use tape_sdk::stream::manifest::{ChunkEntry, ChunkManifest, MANIFEST_VERSION};
use tape_slicer::{stripe_chunk_len, stripe_window, Slicer, StripeWindow};
use tape_store_api::Store;
use tokio::task::spawn_blocking;
use tracing::debug;

use super::decode::{SliceFailure, fetch_order};
use super::hedge::fetch_hedged;
use super::response::ByteRange;
use crate::http::error::RouteError;
use crate::http::handlers::track::slice::SliceOrigin;
use crate::http::state::AppState;

/// A coded track takes the stripe path only past this many stripes
const MIN_RANGED_STRIPES: u64 = 2;

/// Whether a range of this blob is worth decoding from its stripes alone
pub fn is_rangeable(blob: &BlobEncoding) -> bool {
    let stripe_size = blob.stripe_size.to_bytes();
    stripe_size > 0 && blob.size.to_bytes() > stripe_size.saturating_mul(MIN_RANGED_STRIPES)
}

/// The first stripe of a coded track, which is enough to tell a manifest from plain bytes
pub fn head_range(blob: &BlobEncoding) -> ByteRange {
    ByteRange {
        start: 0,
        end: blob.stripe_size.to_bytes().min(blob.size.to_bytes()),
    }
}

/// Whether the first bytes of an object prove it is plain bytes and never a stream manifest
pub fn is_plain_object(head: &[u8], blob_len: u64) -> bool {
    if ChunkManifest::from_bytes(head).is_ok() {
        return false;
    }
    if head.len() as u64 >= blob_len || head.first() != Some(&MANIFEST_VERSION) {
        return true;
    }
    manifest_len(head).is_some_and(|len| len <= head.len() as u64)
}

/// Read from a manifest header how many bytes the whole encoding takes
fn manifest_len(head: &[u8]) -> Option<u64> {
    let empty = ChunkManifest {
        version: MANIFEST_VERSION,
        total_size: StorageUnits::zero(),
        chunk_count: Default::default(),
        chunk_size: StorageUnits::zero(),
        key: Default::default(),
        chunks: Vec::new(),
    };
    let header = empty.to_bytes().ok()?.len();
    let entry = wincode::serialize(&ChunkEntry::default()).ok()?.len();
    let count_bytes: [u8; 8] = head.get(header - 8..header)?.try_into().ok()?;
    let count = u64::from_le_bytes(count_bytes);
    count.checked_mul(entry as u64)?.checked_add(header as u64)
}

/// Decode one byte range of a coded track from windows of k slices
pub async fn decode_track_range<Db: Store, Cluster: Api, Blockchain: Rpc>(
    state: &AppState<Db, Cluster, Blockchain>,
    track_addr: Address,
    track: &CompressedTrack,
    blob: BlobEncoding,
    range: ByteRange,
) -> Result<Bytes, RouteError> {
    let too_large = || RouteError::Internal("object is too large for this platform".into());
    let blob_len = usize::try_from(blob.size.to_bytes()).map_err(|_| too_large())?;
    let stripe_size = usize::try_from(blob.stripe_size.to_bytes()).map_err(|_| too_large())?;
    let (start, end) = match (usize::try_from(range.start), usize::try_from(range.end)) {
        (Ok(start), Ok(end)) => (start, end),
        _ => return Err(too_large()),
    };
    let profile = state.context.params().profile;
    let chunk_size = stripe_chunk_len(profile, stripe_size, blob_len);
    let window = stripe_window(blob_len, stripe_size, chunk_size, start, end)
        .ok_or_else(|| RouteError::BadRequest("range is outside the object".into()))?;
    let k = state.context.params().k();

    let peers = state.context.state().group_peers(track.group);
    let origin = SliceOrigin {
        track_addr,
        tape: track.tape,
        group: track.group,
        blob,
    };
    let candidates = fetch_order(state, track_addr, peers);
    let (windows, failures) = fetch_hedged(&candidates, k, |(spool_id, owner)| {
        fetch_window(state, &origin, spool_id, owner, window)
    })
    .await;
    if windows.len() < k {
        debug!(track = %track_addr, failed = failures.len(), "gateway range read found too few slice windows");
        return Err(RouteError::BadGateway("insufficient verified slice windows for range".into()));
    }

    let started = Instant::now();
    let slicer = Slicer::try_from_profile(profile)
        .map_err(|error| RouteError::Internal(format!("network profile: {error}")))?;
    let decoded = spawn_blocking(move || {
        let refs: Vec<(usize, &[u8])> = windows.iter().map(|(position, data)| (position.as_usize(), data.as_ref())).collect();
        slicer.decode_window(&refs, blob_len, stripe_size, chunk_size, window)
    })
    .await
    .map_err(|error| RouteError::Internal(format!("range decode task: {error}")))?
    .map_err(|error| RouteError::BadGateway(format!("decode range: {error}")))?;
    crate::metrics::observe_decode("ranged", started.elapsed().as_secs_f64(), window.take);

    Ok(Bytes::from(decoded).slice(window.skip..window.skip + window.take))
}

async fn fetch_window<Db: Store, Cluster: Api, Blockchain: Rpc>(
    state: &AppState<Db, Cluster, Blockchain>,
    origin: &SliceOrigin,
    spool_id: SpoolIndex,
    owner: Address,
    window: StripeWindow,
) -> Result<(SpoolIndex, Bytes), SliceFailure> {
    let Some(position) = origin.group.position_of(spool_id) else {
        return Err(SliceFailure::Group);
    };
    let request = GetSliceWindowReq {
        track: origin.track_addr,
        spool: spool_id,
        offset: StorageUnits::from_bytes(window.slice_offset as u64),
        len: StorageUnits::from_bytes(window.slice_len as u64),
    };

    let started = Instant::now();
    let response = match state.context.api.get_slice_window(owner, &request).await {
        Ok(response) => {
            state.reputation.record_success(owner, started.elapsed());
            response
        }
        Err(error) => {
            if counts_against_peer(&error) {
                state.reputation.record_failure(owner);
            }
            debug!(track = %origin.track_addr, %error, "gateway slice window fetch failed");
            return Err(SliceFailure::Fetch);
        }
    };

    let position = SpoolIndex(position as u64);
    let first_window = window.first_sample_window();
    if !origin.blob.verify_slice_window(position, &response.sidecar, first_window, &response.data) {
        return Err(SliceFailure::Leaf);
    }
    Ok((position, response.data))
}

#[cfg(test)]
mod tests {
    use tape_core::types::TrackNumber;
    use tape_crypto::Hash;

    use super::*;

    fn manifest_bytes(segments: u64) -> Vec<u8> {
        let chunks = (0..segments)
            .map(|index| ChunkEntry {
                track: Address::new_unique(),
                offset: StorageUnits::from_bytes(index * 10),
                size: StorageUnits::from_bytes(10),
            })
            .collect();
        ChunkManifest {
            version: MANIFEST_VERSION,
            total_size: StorageUnits::from_bytes(segments * 10),
            chunk_count: TrackNumber(segments),
            chunk_size: StorageUnits::from_bytes(10),
            key: Hash::default(),
            chunks,
        }
        .to_bytes()
        .expect("encode manifest")
    }

    // a head that parses as a manifest, or might once more bytes arrive, keeps the whole-track path
    #[test]
    fn manifest_heads() {
        let manifest = manifest_bytes(4);
        let long = manifest_bytes(4000);

        assert!(!is_plain_object(&manifest, 1 << 30));
        assert!(!is_plain_object(&long[..200], 1 << 30));
        assert!(is_plain_object(&[7u8; 4096], 1 << 30));
        assert!(is_plain_object(&[MANIFEST_VERSION; 4096][..], 4096));
    }
}
