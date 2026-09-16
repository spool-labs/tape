//! Read-grant enforcement for restricted tracks.

use std::time::{SystemTime, UNIX_EPOCH};

use axum::http::HeaderMap;

use rpc::Rpc;
use store::Store;
use tape_core::grant::GrantContext;
use tape_core::track::types::CompressedTrack;
use tape_protocol::Api;
use tape_protocol::api::{decode_read_grant, READ_GRANT_HEADER};
use tape_store::ops::TapeOps;

use crate::features::http::auth::ActivePeer;
use crate::features::http::error::RouteError;
use crate::features::http::peer_identity::PeerIdentity;
use crate::features::http::state::AppState;

/// Refuse a restricted track's bytes unless the caller is group-exempt or holds a grant
pub fn authorize_restricted_read<Db: Store, Cluster: Api, Blockchain: Rpc>(
    state: &AppState<Db, Cluster, Blockchain>,
    track: &CompressedTrack,
    identity: PeerIdentity,
    active_peer: Option<ActivePeer>,
    headers: &HeaderMap,
) -> Result<(), RouteError> {
    if !track.is_restricted() {
        return Ok(());
    }

    if is_group_exempt(state, track, active_peer) {
        return Ok(());
    }

    let reader = identity
        .pubkey()
        .ok_or_else(|| RouteError::Forbidden("restricted track requires an mTLS identity".into()))?;

    let encoded = headers
        .get(READ_GRANT_HEADER)
        .and_then(|value| value.to_str().ok())
        .ok_or_else(|| RouteError::Forbidden("restricted track requires a read grant".into()))?;

    let grant = decode_read_grant(encoded)
        .map_err(|error| RouteError::Forbidden(error.reason().into()))?;

    let in_store = state
        .context
        .store
        .get_tape(track.tape)
        .map_err(|error| RouteError::Internal(format!("get tape: {error}")))?;
    let tape = state
        .context
        .pending
        .apply_to_tape(track.tape, in_store)
        .ok_or_else(|| RouteError::Forbidden("tape is not known to this node".into()))?;

    let context = GrantContext {
        tape: track.tape,
        tape_authority: tape.authority,
        tape_delegate: tape.delegate,
        grant_generation: tape.grant_generation,
        track_key: track.key,
        reader,
        now: wall_now(),
    };

    grant
        .verify(&context)
        .map_err(|error| RouteError::Forbidden(error.reason().into()))
}

/// Grant expiry is judged like a TLS certificate's validity, by the local clock;
/// chain time lags it by however far ingest is behind.
fn wall_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_secs() as i64)
        .unwrap_or(0)
}

/// A committee peer holding a spool in the track's group needs no grant, so sync,
/// repair, and challenge traffic inside the group is untouched.
fn is_group_exempt<Db: Store, Cluster: Api, Blockchain: Rpc>(
    state: &AppState<Db, Cluster, Blockchain>,
    track: &CompressedTrack,
    active_peer: Option<ActivePeer>,
) -> bool {
    let Some(peer) = active_peer else {
        return false;
    };

    state
        .context
        .state()
        .spool_for_node_in_group(track.group, peer.node)
        .is_some()
}
