//! S3 `(bucket, key)` to backing object-track resolution
//!
//! Maps an S3 bucket (a base58 tape address) and object key (a name in the
//! store's per-bucket, name-ordered object index) to the backing data tape and
//! track number. The existing decode/read path under `handlers/object/` then
//! turns that into bytes.

// `resolve_object` and most `ResolvedObject` fields are consumed by the
// GET/HEAD read pass; `parse_bucket` is already used by the listing handler.
#![allow(dead_code)]

use rpc::Rpc;
use store::Store;
use tape_api::program::tapedrive::track_pda;
use tape_core::types::{ContentType, TrackNumber};
use tape_crypto::Hash;
use tape_crypto::address::Address;
use tape_protocol::Api;
use tape_store::ops::ObjectListOps;

use super::error::S3Error;
use crate::http::state::AppState;

/// A resolved S3 object location plus the metadata needed to build response
/// headers without re-reading the object body
pub struct ResolvedObject {
    /// Tape that physically holds the object's track
    pub data_tape: Address,
    /// Track number of the object on `data_tape`
    pub track_number: TrackNumber,
    /// Address of the object-representing track (the `track_pda` of
    /// `(data_tape, track_number)`) the existing decode/read path consumes
    pub track_address: Address,
    /// Object size in bytes
    pub size: u64,
    /// S3 ETag (currently the object commitment hash; see `object_etag`)
    pub etag: Hash,
    /// Last-modified time in unix seconds, when known (from `block_time`)
    pub block_time: Option<i64>,
    /// Object content type recorded in the listing index
    pub content_type: ContentType,
}

/// Parse an S3 bucket label as a base58 tape Address.
pub fn parse_bucket(bucket: &str) -> Result<Address, S3Error> {
    bucket.parse().map_err(|_| S3Error::NoSuchBucket)
}

/// Resolve an S3 `(bucket, key)` pair to its backing object track
pub fn resolve_object<Db: Store, Cluster: Api, Blockchain: Rpc>(
    state: &AppState<Db, Cluster, Blockchain>,
    bucket: Address,
    key: &str,
) -> Result<Option<ResolvedObject>, S3Error> {
    let Some(entry) = state
        .context
        .store
        .get_object_entry(bucket, key.as_bytes())
        .map_err(|error| S3Error::Internal(format!("object index lookup: {error}")))?
    else {
        return Ok(None);
    };

    let track_address = track_pda(entry.data_tape, entry.track_number).0;

    Ok(Some(ResolvedObject {
        data_tape: entry.data_tape,
        track_number: entry.track_number,
        track_address,
        size: entry.size.to_bytes(),
        etag: entry.etag,
        block_time: entry.block_time,
        content_type: entry.content_type,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    // a base58 address parses to a bucket
    #[test]
    fn valid_bucket() {
        // 32 base58 '1' digits decode to the 32-zero (default) address.
        assert!(parse_bucket("11111111111111111111111111111111").is_ok());
    }

    // a non-address bucket label maps to NoSuchBucket
    #[test]
    fn invalid_bucket() {
        assert!(matches!(
            parse_bucket("not a valid address!"),
            Err(S3Error::NoSuchBucket)
        ));
    }
}
