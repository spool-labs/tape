//! S3 write-authorization credential column family.

use store::Column;

use crate::types::Credential;

/// Durable S3 write credentials, keyed by access key id.
pub struct CredentialCol;

impl Column for CredentialCol {
    const CF_NAME: &'static str = "credential";
    type Key = String;
    type Value = Credential;
}
