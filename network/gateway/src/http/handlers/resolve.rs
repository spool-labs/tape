//! Object-name to backing track resolution
//!
//! Maps a tape address and object name from the finalized per-tape object index
//! plus the confirmed pending overlay to the track the decode/read path consumes.
//! Shared by the S3 listener and the site route.

use store::Store;
use tape_api::program::tapedrive::track_pda;
use tape_core::types::ContentType;
use tape_crypto::Hash;
use tape_crypto::address::Address;
use tape_node::features::block::pending_tracks::{PendingNamedObject, PendingTracks};
use tape_store::TapeStore;
use tape_store::error::TapeStoreError;
use tape_store::ops::ObjectListOps;
use tape_store::types::{ObjectListEntry, PendingOp, PendingState};

use crate::staging::StagingStore;

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
pub fn resolve_object<Db: Store>(
    store: &TapeStore<Db>,
    tape: Address,
    name: &[u8],
) -> Result<Option<ResolvedObject>, TapeStoreError> {
    let Some(entry) = store.get_object_entry(tape, name)? else {
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

/// Resolve an object from the finalized index plus confirmed pending blocks.
pub fn resolve_object_with_pending<Db: Store>(
    store: &TapeStore<Db>,
    pending: &PendingTracks,
    tape: Address,
    name: &[u8],
) -> Result<Option<ResolvedObject>, TapeStoreError> {
    let stored = store.get_object_entry(tape, name)?;
    let pending = pending.named_object(tape, name);

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

/// The metadata a queued write answers a read with, before the index has it.
pub struct QueuedObject {
    /// Object size in bytes
    pub size: u64,
    /// Object content type recorded when the write was accepted
    pub content_type: ContentType,
    /// ETag the client was already given
    pub etag: Hash,
    /// Last-modified time in unix seconds
    pub block_time: i64,
}

/// What a read serves for a name.
pub enum Readable {
    /// The write queue still holds the bytes; serve them directly
    Queued(QueuedObject),
    /// Decode the object from its track
    Track(ResolvedObject),
}

impl Readable {
    /// The ETag to answer with, whichever copy serves.
    pub fn etag(&self) -> Hash {
        match self {
            Self::Queued(object) => object.etag,
            Self::Track(resolved) => resolved.etag,
        }
    }

    /// The last-modified second to answer with, when the copy knows one.
    pub fn last_modified(&self) -> Option<i64> {
        match self {
            Self::Queued(object) => Some(object.block_time),
            Self::Track(resolved) => resolved.block_time,
        }
    }

    /// The content type to answer with, whichever copy serves.
    pub fn content_type(&self) -> ContentType {
        match self {
            Self::Queued(object) => object.content_type,
            Self::Track(resolved) => resolved.content_type,
        }
    }
}

/// Resolve a name to what a read serves, the write queue first; an entry is never staler than the index.
pub fn resolve_readable<Db: Store>(
    store: &TapeStore<Db>,
    staging: &StagingStore<Db>,
    tape: Address,
    name: &[u8],
) -> Result<Option<Readable>, TapeStoreError> {
    resolve_readable_or(staging, tape, name, || resolve_object(store, tape, name))
}

/// Resolve a read through the durable write queue, confirmed blocks, then the finalized index.
pub fn resolve_readable_with_pending<Db: Store>(
    store: &TapeStore<Db>,
    staging: &StagingStore<Db>,
    pending: &PendingTracks,
    tape: Address,
    name: &[u8],
) -> Result<Option<Readable>, TapeStoreError> {
    resolve_readable_or(staging, tape, name, || {
        resolve_object_with_pending(store, pending, tape, name)
    })
}

fn resolve_readable_or<Db: Store>(
    staging: &StagingStore<Db>,
    tape: Address,
    name: &[u8],
    fallback: impl FnOnce() -> Result<Option<ResolvedObject>, TapeStoreError>,
) -> Result<Option<Readable>, TapeStoreError> {
    let Some(entry) = staging.entry(tape, name)? else {
        return Ok(fallback()?.map(Readable::Track));
    };

    // A queued delete hides the name even while the index still lists it.
    let PendingOp::Put { content_type, etag, size, block_time, .. } = entry.op else {
        return Ok(None);
    };
    if staging.has_bytes(tape, name)? {
        return Ok(Some(Readable::Queued(QueuedObject {
            size,
            content_type,
            etag,
            block_time,
        })));
    }

    // A streamed write's bytes went straight to their tracks; anything else without bytes is corrupt.
    match entry.state {
        PendingState::Landed { track } => Ok(Some(Readable::Track(ResolvedObject {
            track_address: track,
            size,
            etag,
            block_time: Some(block_time),
            content_type,
        }))),
        PendingState::Queued => Ok(None),
        PendingState::Failed { .. } => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use store_memory::MemoryStore;
    use tape_core::track::types::TrackKind;
    use tape_core::types::{SlotNumber, StorageUnits, TrackNumber};
    use tape_store::types::ObjectListEntry;

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

    fn parts() -> (Arc<TapeStore<MemoryStore>>, StagingStore<MemoryStore>) {
        let store = Arc::new(TapeStore::new(MemoryStore::new()));
        let staging = StagingStore::try_new(store.clone()).expect("open queue");
        (store, staging)
    }

    fn index(store: &TapeStore<MemoryStore>, tape: Address, name: &[u8], size: u64) {
        let entry = ObjectListEntry {
            size: StorageUnits::from_bytes(size),
            etag: Hash([1u8; 32]),
            block_time: Some(1),
            slot: Default::default(),
            data_tape: tape,
            track_number: TrackNumber(1),
            kind: 0,
            content_type: ContentType::TextHtml,
        };
        store.put_object_entry(tape, name, entry).expect("index entry");
    }

    async fn queue(staging: &StagingStore<MemoryStore>, tape: Address, name: &[u8], body: &[u8]) {
        staging
            .enqueue_put(
                tape,
                name,
                body.to_vec(),
                ContentType::TextHtml,
                Hash([9u8; 32]),
                1_700_000_000,
            )
            .await
            .expect("enqueue put");
    }

    // an overwrite serves its queued size and etag, not the older indexed row
    #[tokio::test]
    async fn queue_wins() {
        let (store, staging) = parts();
        let tape = Address::new([1u8; 32]);
        index(&store, tape, b"index.html", 140_291);
        queue(&staging, tape, b"index.html", &vec![0u8; 13_180]).await;

        let readable = resolve_readable(&store, &staging, tape, b"index.html")
            .expect("resolve")
            .expect("readable");

        let Readable::Queued(object) = readable else {
            panic!("the queued copy should win over the index row");
        };
        assert_eq!(object.size, 13_180);
        assert_eq!(object.etag, Hash([9u8; 32]));
    }

    // a directory's index page resolves from the queue the same way
    #[tokio::test]
    async fn queued_directory_index() {
        let (store, staging) = parts();
        let tape = Address::new([2u8; 32]);
        queue(&staging, tape, b"about/index.html", b"<html>").await;

        let readable = resolve_readable(&store, &staging, tape, b"about/index.html")
            .expect("resolve")
            .expect("readable");

        assert!(matches!(readable, Readable::Queued(_)));
    }

    // a queued delete hides a key the index still lists
    #[tokio::test]
    async fn queued_delete_hides() {
        let (store, staging) = parts();
        let tape = Address::new([3u8; 32]);
        index(&store, tape, b"gone.html", 10);
        staging.enqueue_delete(tape, b"gone.html").await.expect("enqueue delete");

        assert!(resolve_readable(&store, &staging, tape, b"gone.html")
            .expect("resolve")
            .is_none());
    }

    // with nothing queued the index answers
    #[tokio::test]
    async fn index_answers() {
        let (store, staging) = parts();
        let tape = Address::new([4u8; 32]);
        index(&store, tape, b"index.html", 42);

        let readable = resolve_readable(&store, &staging, tape, b"index.html")
            .expect("resolve")
            .expect("readable");

        let Readable::Track(resolved) = readable else {
            panic!("the index row should answer");
        };
        assert_eq!(resolved.size, 42);
    }
}
