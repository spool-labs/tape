//! Per-request read grant, shared between the peer client and serving listeners.
//!
//! A reader scopes a grant around its fetches; the peer HTTP client stamps it on
//! outbound calls when one is in scope, and a storage node checks it before
//! serving a restricted track. On the gateway, which sees no client certificate,
//! the reader signs the call itself and the three headers travel together.

use std::future::Future;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tape_core::grant::{GrantError, ReadGrant, ReadRequest, GRANT_SIGNATURE_BYTES};
use tape_crypto::ed25519::{Keypair, Signature};

tokio::task_local! {
    static READ_GRANT: String;
}

/// Header carrying the base64 grant to the node that serves the bytes.
pub const READ_GRANT_HEADER: &str = "x-tape-read-grant";

/// Header carrying the unix second the reader's signature stops being good for.
pub const READ_EXPIRY_HEADER: &str = "x-tape-read-expiry";

/// Header carrying the base64 signature the grant's reader put on this call.
pub const READ_SIGNATURE_HEADER: &str = "x-tape-read-signature";

/// Run a future with a read grant in scope.
pub async fn with_read_grant<F: Future>(grant: String, future: F) -> F::Output {
    READ_GRANT.scope(grant, future).await
}

/// The grant in scope, when the caller runs inside a restricted read.
pub fn current_read_grant() -> Option<String> {
    READ_GRANT.try_with(|grant| grant.clone()).ok()
}

/// Encode a grant for the header.
pub fn encode_read_grant(grant: &ReadGrant) -> String {
    base64::encode(grant.pack())
}

/// Decode a grant from the header.
pub fn decode_read_grant(value: &str) -> Result<ReadGrant, GrantError> {
    let bytes = base64::decode(value).map_err(|_| GrantError::Malformed)?;

    ReadGrant::unpack(&bytes)
}

/// Encode the expiry for the header.
pub fn encode_read_expiry(expiry: i64) -> String {
    expiry.to_string()
}

/// Decode the expiry from the header.
pub fn decode_read_expiry(value: &str) -> Result<i64, GrantError> {
    value.parse().map_err(|_| GrantError::Malformed)
}

/// Encode a reader's request signature for the header.
pub fn encode_read_signature(signature: &Signature) -> String {
    base64::encode(signature.to_bytes())
}

/// Decode a reader's request signature from the header.
pub fn decode_read_signature(value: &str) -> Result<Signature, GrantError> {
    let bytes = base64::decode(value).map_err(|_| GrantError::Malformed)?;
    let bytes: [u8; GRANT_SIGNATURE_BYTES] =
        bytes.try_into().map_err(|_| GrantError::Malformed)?;

    Signature::from_bytes(bytes).map_err(|_| GrantError::Malformed)
}

/// The three headers a reader presents on one restricted call to the gateway.
///
/// The signature covers the method and path, so each set opens exactly the call
/// it was made for, and expires within `ttl`.
pub fn read_request_headers(
    grant: &ReadGrant,
    reader: &Keypair,
    method: &str,
    path: &str,
    ttl: Duration,
) -> [(&'static str, String); 3] {
    let expiry = now_secs().saturating_add(ttl.as_secs() as i64);
    let signature = ReadRequest::sign(reader, method, path, expiry);

    [
        (READ_GRANT_HEADER, encode_read_grant(grant)),
        (READ_EXPIRY_HEADER, encode_read_expiry(expiry)),
        (READ_SIGNATURE_HEADER, encode_read_signature(&signature)),
    ]
}

fn now_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_secs() as i64)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    use bytemuck::Zeroable;
    use tape_core::types::tls::NetworkTlsPubkey;
    use tape_crypto::address::Address;
    use tape_crypto::ed25519::SecretKey;
    use tape_crypto::hash::Hash;

    // a grant round-trips through its header encoding
    #[test]
    fn header_round_trip() {
        let grant = ReadGrant::zeroed();

        let encoded = encode_read_grant(&grant);

        assert_eq!(decode_read_grant(&encoded), Ok(grant));
    }

    // junk in the header is refused rather than parsed
    #[test]
    fn rejects_junk() {
        assert_eq!(decode_read_grant("not base64!"), Err(GrantError::Malformed));
        assert_eq!(decode_read_grant("YWJj"), Err(GrantError::Malformed));
    }

    // the grant round-trips inside its scope and is absent outside one
    #[tokio::test]
    async fn scope_round_trip() {
        assert_eq!(current_read_grant(), None);
        let seen = with_read_grant("abc".into(), async { current_read_grant() }).await;
        assert_eq!(seen.as_deref(), Some("abc"));
        assert_eq!(current_read_grant(), None);
    }
    // the reader's headers verify against the grant they travel with
    #[test]
    fn request_headers() {
        let reader = Keypair::from_secret(SecretKey::from_bytes([0x0b; 32]));
        let grant = ReadGrant::issue(
            &Keypair::from_secret(SecretKey::from_bytes([0x09; 32])),
            Address::new_unique(),
            Hash([0x51; 32]),
            NetworkTlsPubkey::new(reader.pubkey().to_bytes()),
            0,
            1_900_000_000,
        );

        let headers = read_request_headers(
            &grant,
            &reader,
            "GET",
            "/object/abc",
            Duration::from_secs(60),
        );

        assert_eq!(headers[0].0, READ_GRANT_HEADER);
        assert_eq!(decode_read_grant(&headers[0].1), Ok(grant));

        let expiry = decode_read_expiry(&headers[1].1).expect("expiry decodes");
        let signature = decode_read_signature(&headers[2].1).expect("signature decodes");
        let checked = ReadRequest::verify(
            &grant.reader,
            "GET",
            "/object/abc",
            expiry,
            &signature,
            now_secs(),
        );

        assert_eq!(checked, Ok(()));
    }

    // junk in the signature and expiry headers is refused rather than parsed
    #[test]
    fn rejects_bad_request() {
        assert_eq!(decode_read_expiry("soon"), Err(GrantError::Malformed));
        assert_eq!(decode_read_signature("YWJj"), Err(GrantError::Malformed));
    }
}
