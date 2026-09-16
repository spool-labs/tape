//! Reader-signed authorization for restricted tracks on the gateway's own routes.
//!
//! A node checks a grant against the client certificate it terminated. The
//! gateway sees no certificate, so the reader signs the call instead: the grant
//! names a reader key, and the signature over method, path, and expiry proves
//! the caller holds it. Any client with an ed25519 key can walk through, which
//! is the whole point of the door.

use std::time::{SystemTime, UNIX_EPOCH};

use axum::http::{HeaderMap, Method};
use rpc::Rpc;
use store::Store;
use tape_core::grant::{GrantContext, ReadGrant, ReadRequest, WILDCARD_TRACK_KEY};
use tape_core::track::types::CompressedTrack;
use tape_crypto::ed25519::Signature;
use tape_protocol::Api;
use tape_protocol::api::{
    decode_read_expiry, decode_read_grant, decode_read_signature, READ_EXPIRY_HEADER,
    READ_GRANT_HEADER, READ_SIGNATURE_HEADER,
};
use tape_store::ops::TapeOps;
use tape_store::types::TapeInfo;

use crate::http::error::RouteError;
use crate::http::state::AppState;

/// Seconds a grant the gateway mints for itself is good for, long enough to
/// cover a multi-chunk stream read and no longer.
const MINTED_GRANT_TTL_SECS: i64 = 300;

/// Admit a read of one track and return the grant its upstream fetch presents
///
/// A public track passes untouched with no grant. A restricted one is admitted
/// only on a valid owner-signed grant plus the reader's signature over this
/// exact call, and then fetches under whatever grant the gateway itself holds.
pub fn authorize_reader<Db: Store, Cluster: Api, Blockchain: Rpc>(
    state: &AppState<Db, Cluster, Blockchain>,
    headers: &HeaderMap,
    method: &Method,
    path: &str,
    track: &CompressedTrack,
) -> Result<Option<ReadGrant>, RouteError> {
    if !track.is_restricted() {
        return Ok(None);
    }

    let (grant, expiry, signature) = presented_request(headers)?;

    let tape = tape_for_track(state, track)?;
    let now = wall_now();

    // The reader the grant names stands in for the mTLS pubkey a node compares
    // against; the request signature is what ties this caller to that key.
    let context = GrantContext {
        tape: track.tape,
        tape_authority: tape.authority,
        tape_delegate: tape.delegate,
        grant_generation: tape.grant_generation,
        track_key: track.key,
        reader: grant.reader,
        now,
    };
    grant
        .verify(&context)
        .map_err(|error| RouteError::Forbidden(error.reason().into()))?;

    ReadRequest::verify(
        &grant.reader,
        method.as_str(),
        path,
        expiry,
        &signature,
        now,
    )
    .map_err(|error| RouteError::Forbidden(error.reason().into()))?;

    Ok(upstream_grant(state, track, &tape))
}

/// The three headers a restricted read carries, none of them optional
fn presented_request(headers: &HeaderMap) -> Result<(ReadGrant, i64, Signature), RouteError> {
    let grant = decode_read_grant(header(headers, READ_GRANT_HEADER)?)
        .map_err(|error| RouteError::Forbidden(error.reason().into()))?;
    let expiry = decode_read_expiry(header(headers, READ_EXPIRY_HEADER)?)
        .map_err(|error| RouteError::Forbidden(error.reason().into()))?;
    let signature = decode_read_signature(header(headers, READ_SIGNATURE_HEADER)?)
        .map_err(|error| RouteError::Forbidden(error.reason().into()))?;

    Ok((grant, expiry, signature))
}

/// One header, refused as a whole when the caller left it out
fn header<'headers>(
    headers: &'headers HeaderMap,
    name: &'static str,
) -> Result<&'headers str, RouteError> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .ok_or_else(|| RouteError::Forbidden(format!("restricted track requires {name}")))
}

/// The tape a track sits on, pending writes applied
fn tape_for_track<Db: Store, Cluster: Api, Blockchain: Rpc>(
    state: &AppState<Db, Cluster, Blockchain>,
    track: &CompressedTrack,
) -> Result<TapeInfo, RouteError> {
    let in_store = state
        .context
        .store
        .get_tape(track.tape)
        .map_err(|error| RouteError::Internal(format!("get tape: {error}")))?;

    state
        .context
        .pending
        .apply_to_tape(track.tape, in_store)
        .ok_or_else(|| RouteError::Forbidden("tape is not known to this gateway".into()))
}

/// The grant the gateway presents to the nodes it fetches slices from
///
/// A configured grant answers first. Failing that, a tape that delegates to
/// this gateway lets it sign one for its own mTLS identity, so an operator who
/// already runs the tape's writes configures nothing. What it signs is the
/// tape's wildcard, since a streamed object's chunks are tracks of their own
/// and each is checked against the grant's key.
fn upstream_grant<Db: Store, Cluster: Api, Blockchain: Rpc>(
    state: &AppState<Db, Cluster, Blockchain>,
    track: &CompressedTrack,
    tape: &TapeInfo,
) -> Option<ReadGrant> {
    if let Some(grant) = state.grants.grant_for(track.tape, track.key) {
        return Some(grant);
    }

    let write_ctx = state.write_ctx.as_ref()?;
    if tape.delegate != write_ctx.delegate_address() {
        return None;
    }

    write_ctx
        .issue_read_grant(
            track.tape,
            WILDCARD_TRACK_KEY,
            state.context.tls_pubkey(),
            tape.grant_generation,
            wall_now().saturating_add(MINTED_GRANT_TTL_SECS),
        )
        .ok()
}

/// Grant and request expiry are judged by the local clock, as a node judges them.
fn wall_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_secs() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    use tape_core::types::tls::NetworkTlsPubkey;
    use tape_crypto::address::Address;
    use tape_crypto::ed25519::{Keypair, SecretKey};
    use tape_crypto::hash::Hash;
    use tape_protocol::api::read_request_headers;

    fn reader_headers() -> HeaderMap {
        let reader = Keypair::from_secret(SecretKey::from_bytes([0x0b; 32]));
        let grant = ReadGrant::issue(
            &Keypair::from_secret(SecretKey::from_bytes([0x09; 32])),
            Address::new_unique(),
            Hash([0x04; 32]),
            NetworkTlsPubkey::new(reader.pubkey().to_bytes()),
            0,
            1_900_000_000,
        );

        let mut headers = HeaderMap::new();
        for (name, value) in read_request_headers(
            &grant,
            &reader,
            "GET",
            "/object/abc",
            std::time::Duration::from_secs(60),
        ) {
            headers.insert(name, value.parse().expect("header value"));
        }

        headers
    }

    // the three headers a reader sends parse back into what it signed
    #[test]
    fn presented_headers() {
        let headers = reader_headers();

        assert!(presented_request(&headers).is_ok());
    }

    // a missing header names itself in the refusal
    #[test]
    fn missing_header() {
        let mut headers = reader_headers();
        headers.remove(READ_SIGNATURE_HEADER);

        let refused = presented_request(&headers).expect_err("missing signature is refused");

        assert!(refused.to_string().contains(READ_SIGNATURE_HEADER));
    }

    // junk in a header is refused rather than parsed
    #[test]
    fn junk_header() {
        let mut headers = reader_headers();
        headers.insert(READ_EXPIRY_HEADER, "soon".parse().expect("header value"));

        assert!(presented_request(&headers).is_err());
    }
}
