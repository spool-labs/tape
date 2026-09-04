//! Object-name to backing track resolution
//!
//! Maps a tape address and object name from the finalized per-tape object index
//! plus the confirmed pending overlay to the track the decode/read path consumes.
//! Shared by the S3 listener and the site route.

use rpc::Rpc;
use store::Store;
use tape_api::program::tapedrive::track_pda;
use tape_core::types::ContentType;
use tape_crypto::Hash;
use tape_crypto::address::Address;
use tape_protocol::Api;
use tape_store::error::TapeStoreError;
use tape_store::ops::ObjectListOps;
use tape_store::types::ObjectListEntry;
use tape_node::features::block::pending_tracks::PendingNamedObject;

use crate::http::state::AppState;

/// A resolved object location plus the metadata needed to build response
/// headers without re-reading the object body
pub struct ResolvedObject {
    /// Address of the object-representing track the decode/read path consumes
    pub track_address: Address,
    /// Object size in bytes
    pub size: u64,
    /// ETag, currently the object commitment hash
    pub etag: Hash,
    /// Last-modified time in unix seconds, when known
    pub block_time: Option<i64>,
    /// Object content type recorded in the listing index
    pub content_type: ContentType,
}

/// Resolve a tape address and object name to the backing object track
pub fn resolve_object<Db: Store, Cluster: Api, Blockchain: Rpc>(
    state: &AppState<Db, Cluster, Blockchain>,
    tape: Address,
    name: &str,
) -> Result<Option<ResolvedObject>, TapeStoreError> {
    let stored = state
        .context
        .store
        .get_object_entry(tape, name.as_bytes())?;
    let pending = state.context.pending.named_object(tape, name.as_bytes());

    if let Some(entry) = pending.filter(|pending| pending_is_newer(stored.as_ref(), pending)) {
        return Ok(Some(ResolvedObject {
            track_address: entry.track_address,
            size: entry.size,
            etag: entry.etag,
            block_time: entry.block_time,
            content_type: entry.content_type,
        }));
    }

    let Some(entry) = stored else {
        return Ok(None);
    };

    let track_address = track_pda(entry.data_tape, entry.track_number).0;

    Ok(Some(ResolvedObject {
        track_address,
        size: entry.size.to_bytes(),
        etag: entry.etag,
        block_time: entry.block_time,
        content_type: entry.content_type,
    }))
}

/// Pending state overrides finalized state only for a strictly newer tape
/// track. Equal entries are the same registration during promotion, while an
/// older pending event must never roll a finalized replacement backward.
fn pending_is_newer(
    stored: Option<&ObjectListEntry>,
    pending: &PendingNamedObject,
) -> bool {
    stored.is_none_or(|stored| pending.track_number.0 > stored.track_number.0)
}

#[cfg(test)]
mod tests {
    use tape_core::track::types::TrackKind;
    use tape_core::types::{SlotNumber, StorageUnits, TrackNumber};

    use super::*;

    fn stored(track_number: u64) -> ObjectListEntry {
        ObjectListEntry {
            size: StorageUnits::from_bytes(1),
            etag: Hash::default(),
            block_time: None,
            slot: SlotNumber(1),
            data_tape: Address::default(),
            track_number: TrackNumber(track_number),
            kind: TrackKind::Inline as u64,
            content_type: ContentType::TextHtml,
        }
    }

    fn pending(track_number: u64) -> PendingNamedObject {
        PendingNamedObject {
            track_address: Address::new_unique(),
            track_number: TrackNumber(track_number),
            size: 1,
            etag: Hash::default(),
            block_time: None,
            content_type: ContentType::TextHtml,
        }
    }

    #[test]
    fn pending_name_resolution_requires_a_newer_track() {
        assert!(pending_is_newer(None, &pending(4)));
        assert!(pending_is_newer(Some(&stored(3)), &pending(4)));
        assert!(!pending_is_newer(Some(&stored(4)), &pending(4)));
        assert!(!pending_is_newer(Some(&stored(5)), &pending(4)));
    }
}
