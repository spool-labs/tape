//! S3 `(bucket, key)` to backing object-track resolution
//!
//! An S3 bucket is a tape address, written either in base58 or as the lowercase
//! label S3 clients accept as a bucket name; an object key is a name in the
//! store's object index. The shared resolver does the lookup and this module
//! maps its errors into the S3 error domain.

use rpc::Rpc;
use store::Store;
use tape_crypto::address::Address;
use tape_protocol::Api;

use super::error::S3Error;
use crate::http::handlers::resolve::{self, ResolvedObject};
use crate::http::state::AppState;

/// Parse an S3 bucket label as a tape Address, base58 or the lowercase label
pub fn parse_bucket(bucket: &str) -> Result<Address, S3Error> {
    bucket
        .parse()
        .ok()
        .or_else(|| Address::try_from_subdomain_label(bucket))
        .ok_or(S3Error::NoSuchBucket)
}

/// The bucket name a client is handed: lowercase, so every S3 client's name check passes
pub fn bucket_name(tape: Address) -> String {
    tape.to_subdomain_label()
}

/// Resolve an S3 `(bucket, key)` pair to its backing object track
pub fn resolve_object<Db: Store, Cluster: Api, Blockchain: Rpc>(
    state: &AppState<Db, Cluster, Blockchain>,
    bucket: Address,
    key: &str,
) -> Result<Option<ResolvedObject>, S3Error> {
    resolve::resolve_object(state.context.store.as_ref(), bucket, key.as_bytes())
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

    // the lowercase label names the same tape as its base58 form
    #[test]
    fn label_bucket() {
        let tape = Address::new([9u8; 32]);
        assert_eq!(parse_bucket(&bucket_name(tape)).expect("label parses"), tape);
        assert_eq!(parse_bucket(&tape.to_string()).expect("base58 parses"), tape);
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
