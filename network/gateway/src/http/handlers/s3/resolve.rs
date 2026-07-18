//! S3 `(bucket, key)` to backing object-track resolution
//!
//! An S3 bucket is a base58 tape address and an object key is a name in the
//! store's object index; the shared resolver does the lookup and this module
//! maps its errors into the S3 error domain.

use rpc::Rpc;
use store::Store;
use tape_crypto::address::Address;
use tape_protocol::Api;

use super::error::S3Error;
use crate::http::handlers::resolve::{self, ResolvedObject};
use crate::http::state::AppState;

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
    resolve::resolve_object(state, bucket, key)
        .map_err(|error| S3Error::Internal(format!("object index lookup: {error}")))
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
