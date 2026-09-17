//! Share token header, which the peer client stamps from the token in scope, and the
//! signed headers for gateway calls

use std::future::Future;
use std::time::Duration;

use tape_core::share::{unix_now, ReadRequest, ShareError, ShareToken};
use tape_crypto::ed25519::consts::ED25519_SIG_LEN;
use tape_crypto::ed25519::{Keypair, Signature};

tokio::task_local! {
    static SHARE_TOKEN: ShareToken;
}

/// Header carrying the base64 share token to the node that serves the bytes
pub const SHARE_TOKEN_HEADER: &str = "x-tape-share-token";

/// Header carrying the unix second when the reader's signature expires
pub const READ_EXPIRY_HEADER: &str = "x-tape-read-expiry";

/// Header carrying the reader's base64 signature over this call
pub const READ_SIGNATURE_HEADER: &str = "x-tape-read-signature";

/// Run a future with a share token in scope, so peer requests inside it carry the token
pub async fn with_share_token<F: Future>(token: Option<ShareToken>, future: F) -> F::Output {
    match token {
        Some(token) => SHARE_TOKEN.scope(token, future).await,
        None => future.await,
    }
}

/// The share token in scope, if any
pub fn current_share_token() -> Option<ShareToken> {
    SHARE_TOKEN.try_with(|token| *token).ok()
}

/// Encode a share token for the header
pub fn encode_share_token(token: &ShareToken) -> String {
    base64::encode(token.pack())
}

/// Decode a share token from the header
pub fn decode_share_token(value: &str) -> Result<ShareToken, ShareError> {
    let bytes = base64::decode(value).map_err(|_| ShareError::Malformed)?;

    ShareToken::unpack(&bytes)
}

/// Encode the expiry for the header
pub fn encode_read_expiry(expiry: i64) -> String {
    expiry.to_string()
}

/// Decode the expiry from the header
pub fn decode_read_expiry(value: &str) -> Result<i64, ShareError> {
    value.parse().map_err(|_| ShareError::Malformed)
}

/// Encode a reader's request signature for the header
pub fn encode_read_signature(signature: &Signature) -> String {
    base64::encode(signature.to_bytes())
}

/// Decode a reader's request signature from the header
pub fn decode_read_signature(value: &str) -> Result<Signature, ShareError> {
    let bytes = base64::decode(value).map_err(|_| ShareError::Malformed)?;
    let bytes: [u8; ED25519_SIG_LEN] = bytes.try_into().map_err(|_| ShareError::Malformed)?;

    Signature::from_bytes(bytes).map_err(|_| ShareError::Malformed)
}

/// Build the three headers for one hidden-track call to the gateway
///
/// The signature covers the method and path, so each set opens only the call it was made for, and expires within `ttl`.
pub fn read_request_headers(
    token: &ShareToken,
    reader: &Keypair,
    method: &str,
    path: &str,
    ttl: Duration,
) -> [(&'static str, String); 3] {
    let expiry = unix_now().saturating_add(ttl.as_secs() as i64);
    let signature = ReadRequest::sign(reader, method, path, expiry);

    [
        (SHARE_TOKEN_HEADER, encode_share_token(token)),
        (READ_EXPIRY_HEADER, encode_read_expiry(expiry)),
        (READ_SIGNATURE_HEADER, encode_read_signature(&signature)),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    use bytemuck::Zeroable;
    use tape_core::types::tls::NetworkTlsPubkey;
    use tape_crypto::address::Address;
    use tape_crypto::ed25519::SecretKey;
    use tape_crypto::hash::Hash;

    // a token round-trips through its header encoding
    #[test]
    fn header_roundtrip() {
        let token = ShareToken::zeroed();

        assert_eq!(decode_share_token(&encode_share_token(&token)), Ok(token));
    }

    // junk in the header is refused
    #[test]
    fn rejects_junk() {
        assert_eq!(decode_share_token("not base64!"), Err(ShareError::Malformed));
        assert_eq!(decode_share_token("YWJj"), Err(ShareError::Malformed));
    }

    // the token is visible inside its scope and absent outside one
    #[tokio::test]
    async fn scope() {
        let token = ShareToken::zeroed();

        assert_eq!(current_share_token(), None);
        let seen = with_share_token(Some(token), async { current_share_token() }).await;
        assert_eq!(seen, Some(token));
        assert_eq!(current_share_token(), None);
    }

    // a reader's headers verify against their token
    #[test]
    fn request_headers() {
        let reader = Keypair::from_secret(SecretKey::from_bytes([0x0b; 32]));
        let token = ShareToken::issue(
            &Keypair::from_secret(SecretKey::from_bytes([0x09; 32])),
            Address::new_unique(),
            Hash([0x51; 32]),
            NetworkTlsPubkey::new(reader.pubkey().to_bytes()),
            Hash::default(),
            1_900_000_000,
        );

        let headers = read_request_headers(&token, &reader, "GET", "/object/abc", Duration::from_secs(60));

        assert_eq!(headers[0].0, SHARE_TOKEN_HEADER);
        assert_eq!(decode_share_token(&headers[0].1), Ok(token));

        let expiry = decode_read_expiry(&headers[1].1).expect("expiry decodes");
        let signature = decode_read_signature(&headers[2].1).expect("signature decodes");
        let checked = ReadRequest::verify(&token.reader, "GET", "/object/abc", expiry, &signature, unix_now());

        assert_eq!(checked, Ok(()));
    }

    // junk in the signature and expiry headers is refused
    #[test]
    fn rejects_bad_request() {
        assert_eq!(decode_read_expiry("soon"), Err(ShareError::Malformed));
        assert_eq!(decode_read_signature("YWJj"), Err(ShareError::Malformed));
    }
}
