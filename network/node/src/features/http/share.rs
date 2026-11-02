//! Share token check for hidden tracks

use axum::http::HeaderMap;

use tape_rpc::Rpc;
use tape_store_api::Store;
use tape_core::share::{unix_now, ShareContext};
use tape_core::track::types::CompressedTrack;
use tape_protocol::Api;
use tape_protocol::api::{decode_share_token, SHARE_TOKEN_HEADER};
use tape_store::ops::TapeOps;

use crate::context::NodeContext;
use crate::features::http::auth::ActivePeer;
use crate::features::http::error::RouteError;
use crate::features::http::peer_identity::PeerIdentity;
use crate::features::http::state::AppState;

/// Refuse a hidden track's bytes unless the caller holds a spool in its group or a valid share token
pub fn check_share_token<Db: Store, Cluster: Api, Blockchain: Rpc>(
    state: &AppState<Db, Cluster, Blockchain>,
    track: &CompressedTrack,
    identity: PeerIdentity,
    active_peer: Option<ActivePeer>,
    headers: &HeaderMap,
) -> Result<(), RouteError> {
    if !track.is_hidden() {
        return Ok(());
    }

    if holds_group_spool(&state.context, track, active_peer) {
        return Ok(());
    }

    let reader = identity
        .pubkey()
        .ok_or_else(|| RouteError::Forbidden("hidden track requires an mTLS identity".into()))?;

    let encoded = headers
        .get(SHARE_TOKEN_HEADER)
        .and_then(|value| value.to_str().ok())
        .ok_or_else(|| RouteError::Forbidden("hidden track requires a share token".into()))?;

    let token = decode_share_token(encoded)
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

    let context = ShareContext {
        tape: track.tape,
        tape_authority: tape.authority,
        tape_delegate: tape.delegate,
        nonce: tape.nonce,
        track_key: track.key,
        reader,
        now: unix_now(),
    };

    token
        .verify(&context)
        .map_err(|error| RouteError::Forbidden(error.reason().into()))
}

/// Whether the caller is an active peer holding a spool in the track's group, so sync and repair pass
pub(crate) fn holds_group_spool<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: &NodeContext<Db, Cluster, Blockchain>,
    track: &CompressedTrack,
    active_peer: Option<ActivePeer>,
) -> bool {
    let Some(peer) = active_peer else {
        return false;
    };

    ctx.state()
        .spool_for_node_in_group(track.group, peer.node)
        .is_some()
}
