//! Reader-signed authorization for hidden tracks on the gateway's own routes
//!
//! Gateway clients have no client certificate, so the reader signs method, path,
//! and expiry with the share token's reader key.

use std::time::Duration;

use axum::http::{HeaderMap, Method};
use tape_rpc::Rpc;
use tape_store_api::Store;
use tape_core::share::{unix_now, ReadRequest, ShareContext, ShareError, ShareToken, ALL_TRACKS};
use tape_core::tape::TapeFlags;
use tape_core::track::types::CompressedTrack;
use tape_crypto::Hash;
use tape_crypto::address::Address;
use tape_crypto::ed25519::Signature;
use tape_crypto::hash::hash;
use tape_protocol::Api;
use tape_protocol::api::{
    decode_read_expiry, decode_read_signature, decode_share_token, READ_EXPIRY_HEADER,
    READ_SIGNATURE_HEADER, SHARE_TOKEN_HEADER,
};
use tape_store::ops::TapeOps;
use tape_store::types::TapeInfo;

use crate::http::error::RouteError;
use crate::http::state::AppState;

/// A share token the gateway signs for itself stays valid this long, enough for a multi-chunk stream read
const SELF_SHARE_TTL: Duration = Duration::from_secs(300);

/// Admit a read of one track and return the share token for its upstream fetch
///
/// A public track passes with no token. A hidden one needs a valid share token plus the
/// reader's signature over this call, and then fetches under the gateway's own token.
pub fn authorize_signed_read<Db: Store, Cluster: Api, Blockchain: Rpc>(
    state: &AppState<Db, Cluster, Blockchain>,
    headers: &HeaderMap,
    method: &Method,
    path: &str,
    track: &CompressedTrack,
) -> Result<Option<ShareToken>, RouteError> {
    if !track.is_hidden() {
        return Ok(None);
    }

    let tape = tape_for_track(state, track)?;
    verify_signed_read(headers, method, path, track.tape, &tape, track.key)?;

    Ok(node_share_token(state, track, &tape))
}

/// Admit a read of an object the write queue still holds
///
/// The object lands with its tape's flags under the key `hash(name)`, so on a hidden tape
/// it takes the same signed read its track will.
pub fn authorize_signed_queued_read<Db: Store, Cluster: Api, Blockchain: Rpc>(
    state: &AppState<Db, Cluster, Blockchain>,
    headers: &HeaderMap,
    method: &Method,
    path: &str,
    tape: Address,
    name: &[u8],
) -> Result<(), RouteError> {
    admit_queued_read(tape_info(state, tape)?.as_ref(), headers, method, path, tape, name)
}

/// Whether a tape's writes land hidden. A tape this gateway hasn't seen counts as hidden
pub fn is_hidden_tape(info: Option<&TapeInfo>) -> bool {
    info.is_none_or(|info| TapeFlags::is_hidden(info.flags))
}

/// Load a tape with pending writes applied
pub fn tape_info<Db: Store, Cluster: Api, Blockchain: Rpc>(
    state: &AppState<Db, Cluster, Blockchain>,
    tape: Address,
) -> Result<Option<TapeInfo>, RouteError> {
    let in_store = state
        .context
        .store
        .get_tape(tape)
        .map_err(|error| RouteError::Internal(format!("get tape: {error}")))?;

    Ok(state.context.pending.apply_to_tape(tape, in_store))
}

/// Load a track's tape with pending writes applied
pub fn tape_for_track<Db: Store, Cluster: Api, Blockchain: Rpc>(
    state: &AppState<Db, Cluster, Blockchain>,
    track: &CompressedTrack,
) -> Result<TapeInfo, RouteError> {
    tape_info(state, track.tape)?.ok_or_else(unknown_tape)
}

/// Pick the share token for the gateway's fetches from nodes
///
/// A configured token answers first. Failing that, a tape that delegates to this gateway
/// lets it sign an all-tracks token for its own mTLS identity, since each chunk of a
/// streamed object is a track with its own key.
pub fn node_share_token<Db: Store, Cluster: Api, Blockchain: Rpc>(
    state: &AppState<Db, Cluster, Blockchain>,
    track: &CompressedTrack,
    tape: &TapeInfo,
) -> Option<ShareToken> {
    if let Some(token) = state.share_tokens.token_for(track.tape, track.key) {
        return Some(token);
    }

    let write_ctx = state.write_ctx.as_ref()?;
    if tape.delegate != write_ctx.delegate_address() {
        return None;
    }

    write_ctx
        .issue_share_token(
            track.tape,
            ALL_TRACKS,
            state.context.tls_pubkey(),
            tape.nonce,
            unix_now().saturating_add(SELF_SHARE_TTL.as_secs() as i64),
        )
        .ok()
}

fn admit_queued_read(
    info: Option<&TapeInfo>,
    headers: &HeaderMap,
    method: &Method,
    path: &str,
    tape: Address,
    name: &[u8],
) -> Result<(), RouteError> {
    if !is_hidden_tape(info) {
        return Ok(());
    }

    let info = info.ok_or_else(unknown_tape)?;
    verify_signed_read(headers, method, path, tape, info, hash(name))
}

/// Check the reader's share token for one track key and the reader's signature over this call
fn verify_signed_read(
    headers: &HeaderMap,
    method: &Method,
    path: &str,
    tape: Address,
    info: &TapeInfo,
    track_key: Hash,
) -> Result<(), RouteError> {
    let (token, expiry, signature) = read_headers(headers)?;
    let now = unix_now();

    // The request signature below proves the caller holds the token's reader key.
    let context = ShareContext {
        tape,
        tape_authority: info.authority,
        tape_delegate: info.delegate,
        nonce: info.nonce,
        track_key,
        reader: token.reader,
        now,
    };
    token.verify(&context).map_err(forbidden)?;

    ReadRequest::verify(&token.reader, method.as_str(), path, expiry, &signature, now)
        .map_err(forbidden)
}

fn unknown_tape() -> RouteError {
    RouteError::Forbidden("tape is not known to this gateway".into())
}

/// Parse the three required headers of a hidden-track read
fn read_headers(headers: &HeaderMap) -> Result<(ShareToken, i64, Signature), RouteError> {
    let token = decode_share_token(header(headers, SHARE_TOKEN_HEADER)?).map_err(forbidden)?;
    let expiry = decode_read_expiry(header(headers, READ_EXPIRY_HEADER)?).map_err(forbidden)?;
    let signature =
        decode_read_signature(header(headers, READ_SIGNATURE_HEADER)?).map_err(forbidden)?;

    Ok((token, expiry, signature))
}

/// One header, refused as a whole when the caller left it out
fn header<'headers>(
    headers: &'headers HeaderMap,
    name: &'static str,
) -> Result<&'headers str, RouteError> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .ok_or_else(|| RouteError::Forbidden(format!("hidden track requires {name}")))
}

fn forbidden(error: ShareError) -> RouteError {
    RouteError::Forbidden(error.reason().into())
}

#[cfg(test)]
mod tests {
    use super::*;

    use tape_core::types::tls::NetworkTlsPubkey;
    use tape_core::types::{EpochNumber, TapeNumber, TrackNumber};
    use tape_crypto::ed25519::{Keypair, SecretKey};
    use tape_protocol::api::read_request_headers;

    const QUEUED_PATH: &str = "/bucket/tape/notes.txt";

    fn authority() -> Keypair {
        Keypair::from_secret(SecretKey::from_bytes([0x09; 32]))
    }

    fn tape_row(flags: u64) -> TapeInfo {
        TapeInfo {
            authority: authority().pubkey().address(),
            ..TapeInfo::new(TapeNumber(1), flags, EpochNumber(10), TrackNumber(1))
        }
    }

    fn queued_headers(tape: Address, track_key: Hash) -> HeaderMap {
        let reader = Keypair::from_secret(SecretKey::from_bytes([0x0b; 32]));
        let token = ShareToken::issue(
            &authority(),
            tape,
            track_key,
            NetworkTlsPubkey::new(reader.pubkey().to_bytes()),
            Hash::default(),
            1_900_000_000,
        );

        let mut headers = HeaderMap::new();
        for (name, value) in read_request_headers(&token, &reader, "GET", QUEUED_PATH, Duration::from_secs(60)) {
            headers.insert(name, value.parse().expect("header value"));
        }

        headers
    }

    fn admit(info: Option<&TapeInfo>, headers: &HeaderMap, tape: Address) -> Result<(), RouteError> {
        admit_queued_read(info, headers, &Method::GET, QUEUED_PATH, tape, b"notes.txt")
    }

    fn reader_headers() -> HeaderMap {
        let reader = Keypair::from_secret(SecretKey::from_bytes([0x0b; 32]));
        let token = ShareToken::issue(
            &Keypair::from_secret(SecretKey::from_bytes([0x09; 32])),
            Address::new_unique(),
            Hash([0x04; 32]),
            NetworkTlsPubkey::new(reader.pubkey().to_bytes()),
            Hash::default(),
            1_900_000_000,
        );

        let mut headers = HeaderMap::new();
        for (name, value) in read_request_headers(&token, &reader, "GET", "/object/abc", Duration::from_secs(60)) {
            headers.insert(name, value.parse().expect("header value"));
        }

        headers
    }

    // a reader's three headers parse back into the signed values
    #[test]
    fn parses_headers() {
        let headers = reader_headers();

        assert!(read_headers(&headers).is_ok());
    }

    // a refusal for a missing header includes the header
    #[test]
    fn missing_header() {
        let mut headers = reader_headers();
        headers.remove(READ_SIGNATURE_HEADER);

        let refused = read_headers(&headers).expect_err("missing signature is refused");

        assert!(refused.to_string().contains(READ_SIGNATURE_HEADER));
    }

    // junk in a header is refused
    #[test]
    fn junk_header() {
        let mut headers = reader_headers();
        headers.insert(READ_EXPIRY_HEADER, "soon".parse().expect("header value"));

        assert!(read_headers(&headers).is_err());
    }

    // a queued object on a public tape needs no token
    #[test]
    fn queued_public() {
        let info = tape_row(TapeFlags::NONE);

        assert!(admit(Some(&info), &HeaderMap::new(), Address::new_unique()).is_ok());
    }

    // a queued object on a hidden tape is refused to an unsigned caller
    #[test]
    fn queued_hidden_unsigned() {
        let info = tape_row(TapeFlags::HIDDEN);

        let refused = admit(Some(&info), &HeaderMap::new(), Address::new_unique());

        assert!(matches!(refused, Err(RouteError::Forbidden(_))));
    }

    // a reader with a token for the object's name and a signed call reads a queued hidden object
    #[test]
    fn queued_hidden_signed() {
        let tape = Address::new_unique();
        let headers = queued_headers(tape, hash(b"notes.txt"));

        assert!(admit(Some(&tape_row(TapeFlags::HIDDEN)), &headers, tape).is_ok());
    }

    // a token for another name doesn't open a queued hidden object
    #[test]
    fn queued_hidden_other_key() {
        let tape = Address::new_unique();
        let headers = queued_headers(tape, hash(b"other.txt"));

        let refused = admit(Some(&tape_row(TapeFlags::HIDDEN)), &headers, tape);

        assert!(matches!(refused, Err(RouteError::Forbidden(_))));
    }

    // a queued object on a tape the gateway hasn't seen is refused, even to a signed caller
    #[test]
    fn queued_unknown_tape() {
        let tape = Address::new_unique();
        let headers = queued_headers(tape, hash(b"notes.txt"));

        assert!(matches!(admit(None, &headers, tape), Err(RouteError::Forbidden(_))));
    }
}
