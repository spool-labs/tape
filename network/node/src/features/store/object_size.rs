//! Correct object sizes in listing results.
//!
//! `tape object ls`, object `head`, and clients that use the node object-list
//! endpoint read from the node's object-list index. That index is a cache keyed
//! by `(bucket, name)`, so a listing can be served with one range scan instead
//! of looking up every object track one by one.
//!
//! The size in that index should be the size a user uploaded. That is usually
//! the same as the size of the named track, but large objects are different:
//! they are split across chunk tracks, and the named track is only a manifest
//! that points at those chunks. If the node records the manifest track's size,
//! a 100 MiB object can show up as a tiny object like `113` bytes.
//!
//! There are three useful meanings of "size" here:
//!
//! - Logical object size: bytes the user intended to store, like S3
//!   `Content-Length`.
//! - Committed chunk payload size: sum of the actual chunk tracks'
//!   `state.size` / `BlobEncoding.size`.
//! - Network/storage footprint: encoded slice bytes stored across nodes,
//!   including erasure overhead.
//!
//! This module is about the first meaning because object listings are a
//! user-facing catalog view. It should not be used as the source of truth for
//! quota, billing, repair accounting, or physical storage usage.
//!
//! There are four object shapes to keep straight:
//!
//! - Inline normal object: the named track contains the object bytes. The track
//!   size is the object size.
//! - Coded normal object: the named track contains erasure-coded object bytes.
//!   `BlobEncoding.size` is the object size.
//! - Chunked object with an inline manifest: the named track contains the
//!   `ChunkManifest` bytes directly. Replay can parse those bytes and use
//!   `manifest.total_size`. We do not write this shape yet, but we probably
//!   will as an optimization.
//! - Chunked object with a coded manifest: the named track contains an
//!   erasure-coded `ChunkManifest`. The track size is only the manifest size,
//!   and the logical object size is inside the decoded manifest.
//!
//! This module handles the last case. When a listed object looks like a coded
//! manifest, the node decodes that track through the normal slice path, parses
//! the result as `ChunkManifest`, verifies that it belongs to the listed object
//! name, and then rewrites the cached list entry with `manifest.total_size`.
//! If the decode fails, the bytes are not a manifest, or the manifest belongs
//! to another name, the cached entry is left alone.

use std::collections::HashMap;

use rpc::Rpc;
use store::Store;
use tape_api::program::tapedrive::track_pda;
use tape_core::erasure::group_start;
use tape_core::track::data::BlobData;
use tape_core::track::types::{CompressedTrack, TrackKind};
use tape_core::types::{StorageUnits, SpoolIndex};
use tape_crypto::address::Address;
use tape_crypto::hash::hash;
use tape_protocol::Api;
use tape_sdk::codec::decoder::BlobDecoder;
use tape_sdk::stream::manifest::ChunkManifest;
use tape_sdk::transfer::downloader::ParallelDownloader;
use tape_store::ops::{ObjectListOps, TrackDataOps, TrackOps};
use tape_store::types::ObjectListEntry;

use crate::context::NodeContext;
use crate::core::error::NodeError;

const MANIFEST_BASE_BYTES: u64 = 65;
const MANIFEST_ENTRY_BYTES: u64 = 24;

// Lazy list-time hydration is deliberately bounded. This still covers roughly
// 2.7 TiB of object data at today's 64 MiB chunk size.
const MAX_LAZY_MANIFEST_BYTES: u64 = 1024 * 1024;

/// Return `entry` with its logical stream size when its coded track decodes to
/// a valid `ChunkManifest`; otherwise leave it unchanged.
pub async fn hydrate_object_list_entry_size<Db, Cluster, Blockchain>(
    context: &NodeContext<Db, Cluster, Blockchain>,
    bucket: Address,
    name: &[u8],
    entry: ObjectListEntry,
) -> Result<ObjectListEntry, NodeError>
where
    Db: Store,
    Cluster: Api,
    Blockchain: Rpc,
{
    if !should_try_manifest_hydration(&entry) {
        return Ok(entry);
    }

    let track_address = track_pda(entry.data_tape, entry.track_number).0.into();
    let Some(track) = context
        .store
        .get_track(track_address)
        .map_err(store_error("get_track"))?
    else {
        return Ok(entry);
    };

    if !track.is_coded() || track.tape != entry.data_tape || track.track_number != entry.track_number
    {
        return Ok(entry);
    }

    let Some(BlobData::Coded(blob)) = context
        .store
        .get_track_data(track_address)
        .map_err(store_error("get_track_data"))?
    else {
        return Ok(entry);
    };

    if blob.get_hash() != track.value_hash {
        return Ok(entry);
    }

    let manifest_bytes = match decode_coded_track(context, track_address, &track, blob).await {
        Ok(bytes) => bytes,
        Err(error) => {
            tracing::debug!(
                track = %track_address,
                name = %String::from_utf8_lossy(name),
                error = %error,
                "coded object did not hydrate as a manifest"
            );
            return Ok(entry);
        }
    };

    let Ok(manifest) = ChunkManifest::from_bytes(&manifest_bytes) else {
        return Ok(entry);
    };

    if manifest.key != hash(name) {
        return Ok(entry);
    }

    let manifest_total_size = manifest.total_size;
    if manifest_total_size == entry.size {
        return Ok(entry);
    }

    let mut hydrated = entry;
    hydrated.size = manifest_total_size;
    context
        .store
        .put_object_entry(bucket, name, hydrated.clone())
        .map_err(store_error("put_object_entry"))?;

    Ok(hydrated)
}

fn should_try_manifest_hydration(entry: &ObjectListEntry) -> bool {
    if entry.kind != TrackKind::Coded as u64 {
        return false;
    }

    looks_like_manifest_size(entry.size)
}

fn looks_like_manifest_size(size: StorageUnits) -> bool {
    let bytes = size.to_bytes();
    if !(MANIFEST_BASE_BYTES + MANIFEST_ENTRY_BYTES..=MAX_LAZY_MANIFEST_BYTES).contains(&bytes) {
        return false;
    }

    (bytes - MANIFEST_BASE_BYTES) % MANIFEST_ENTRY_BYTES == 0
}

async fn decode_coded_track<Db, Cluster, Blockchain>(
    context: &NodeContext<Db, Cluster, Blockchain>,
    track_address: Address,
    track: &CompressedTrack,
    blob: tape_core::track::blob::BlobEncoding,
) -> Result<Vec<u8>, String>
where
    Db: Store,
    Cluster: Api,
    Blockchain: Rpc,
{
    let peers: HashMap<SpoolIndex, Address> =
        context.state().group_peers(track.group).into_iter().collect();
    let downloader = ParallelDownloader::new(track_address, peers, blob.profile.k() as usize);
    let slices = downloader
        .download_enough_slices(context.api.as_ref())
        .await
        .map_err(|error| error.to_string())?;

    let base = group_start(track.group);
    let local_slices = slices
        .into_iter()
        .map(|(spool, data)| (spool - base, data))
        .collect();

    let mut decoder = BlobDecoder::with_profile(blob.profile);
    let mut bytes = decoder
        .decode(local_slices)
        .map_err(|error| error.to_string())?;
    let expected_len = usize::try_from(blob.size.to_bytes())
        .map_err(|_| "manifest blob is too large for this platform".to_string())?;
    if bytes.len() < expected_len {
        return Err("decoded manifest shorter than blob metadata".into());
    }
    bytes.truncate(expected_len);
    Ok(bytes)
}

fn store_error(
    operation: &'static str,
) -> impl FnOnce(tape_store::error::TapeStoreError) -> NodeError {
    move |error| NodeError::Store(format!("{operation}: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn coded_entry(size: u64) -> ObjectListEntry {
        ObjectListEntry {
            size: StorageUnits::from_bytes(size),
            etag: tape_crypto::Hash::default(),
            block_time: None,
            slot: tape_core::types::SlotNumber(0),
            data_tape: Address::new_unique(),
            track_number: tape_core::types::TrackNumber(0),
            kind: TrackKind::Coded as u64,
            content_type: tape_core::types::ContentType::Unknown,
        }
    }

    #[test]
    fn recognizes_current_small_manifest_shape() {
        assert!(should_try_manifest_hydration(&coded_entry(113)));
    }

    #[test]
    fn skips_non_manifest_sized_coded_objects() {
        assert!(!should_try_manifest_hydration(&coded_entry(10 * 1024 * 1024)));
    }

    #[test]
    fn skips_inline_objects() {
        let mut entry = coded_entry(113);
        entry.kind = TrackKind::Inline as u64;
        assert!(!should_try_manifest_hydration(&entry));
    }
}
