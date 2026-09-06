//! Object-name to backing track resolution
//!
//! Maps a tape address and object name from the store's per-tape,
//! name-ordered object index to the track the decode/read path consumes.
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
    let Some(entry) = state
        .context
        .store
        .get_object_entry(tape, name.as_bytes())?
    else {
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
