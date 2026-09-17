//! Share token header, stamped by the peer client from the token in scope

use std::future::Future;

use tape_core::share::{ShareError, ShareToken};

tokio::task_local! {
    static SHARE_TOKEN: String;
}

/// Header carrying the base64 share token to the node that serves the bytes
pub const SHARE_TOKEN_HEADER: &str = "x-tape-share-token";

/// Run a future with a share token in scope, so peer requests inside it carry the token
pub async fn with_share_token<F: Future>(token: Option<ShareToken>, future: F) -> F::Output {
    match token {
        Some(token) => SHARE_TOKEN.scope(encode_share_token(&token), future).await,
        None => future.await,
    }
}

/// The encoded share token in scope, if any
pub fn current_share_token() -> Option<String> {
    SHARE_TOKEN.try_with(Clone::clone).ok()
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

#[cfg(test)]
mod tests {
    use super::*;

    use bytemuck::Zeroable;

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
        assert_eq!(seen, Some(encode_share_token(&token)));
        assert_eq!(current_share_token(), None);
    }
}
