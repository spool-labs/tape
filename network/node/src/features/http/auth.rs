//! Application-layer peer authorization.
//!
//! The TLS layer ([`super::peer_identity::PeerIdentityAcceptor`]) captures the
//! client cert's SPKI as a [`PeerIdentity`] and attaches it to every request
//! on that connection. This module turns that raw identity into request-scoped
//! peer capabilities:
//! - [`ActivePeer`]: the cert maps to a node in the current committee.
//! - [`StakedPeer`]: the cert maps to a registered node whose stake satisfies
//!   the local node's access threshold.
//!
//! Committee-only handlers declare `_active_peer: ActivePeer`; hard-gated read
//! handlers declare `_staked_peer: StakedPeer`; threshold-conditioned routes
//! declare `MaybeStakedPeer` and apply the local threshold explicitly. Axum's
//! extractor machinery returns `403 FORBIDDEN` if the relevant hard-gate
//! extension is absent, which happens when:
//! - the request came in on the HTTP listener (no client cert, no mTLS),
//! - the client dialled HTTPS without presenting a cert,
//! - the client cert's SPKI doesn't map to any known node, or
//! - the mapped node lacks the required committee membership or access stake.
//!
//! The HTTPS listener installs [`authorize_peer`] as a middleware layer so the
//! check runs on every request. The HTTP listener omits it, so these extractors
//! are unreachable from the plaintext side regardless of what the caller does.

use axum::extract::{Request, State};
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::Response;

use rpc::Rpc;
use store::Store;
use tape_core::types::coin::{Coin, TAPE};
use tape_core::types::tls::NetworkTlsPubkey;
use tape_crypto::Address;
use tape_protocol::Api;
use tracing::trace;

use super::peer_identity::PeerIdentity;
use super::state::AppState;

/// A request caller authenticated as a current committee peer via mTLS.
///
/// Present in request extensions only when the handshake yielded a client cert
/// whose SPKI maps to a known node account in the local current committee.
#[derive(Clone, Copy, Debug)]
pub struct ActivePeer {
    pub node: Address,
    pub tls_pubkey: NetworkTlsPubkey,
}

/// A request caller authenticated as a stake-qualified read peer via mTLS.
#[derive(Clone, Copy, Debug)]
pub struct StakedPeer {
    pub node: Address,
    pub tls_pubkey: NetworkTlsPubkey,
    pub stake: Coin<TAPE>,
}

/// Optional stake-qualified peer capability.
#[derive(Clone, Copy, Debug)]
pub struct MaybeStakedPeer(pub Option<StakedPeer>);

impl<S> axum::extract::FromRequestParts<S> for ActivePeer
where
    S: Send + Sync,
{
    type Rejection = StatusCode;

    async fn from_request_parts(
        parts: &mut axum::http::request::Parts,
        _state: &S,
    ) -> Result<Self, Self::Rejection> {
        parts
            .extensions
            .get::<ActivePeer>()
            .copied()
            .ok_or(StatusCode::FORBIDDEN)
    }
}

impl<S> axum::extract::FromRequestParts<S> for StakedPeer
where
    S: Send + Sync,
{
    type Rejection = StatusCode;

    async fn from_request_parts(
        parts: &mut axum::http::request::Parts,
        _state: &S,
    ) -> Result<Self, Self::Rejection> {
        parts
            .extensions
            .get::<StakedPeer>()
            .copied()
            .ok_or(StatusCode::FORBIDDEN)
    }
}

impl<S> axum::extract::FromRequestParts<S> for MaybeStakedPeer
where
    S: Send + Sync,
{
    type Rejection = std::convert::Infallible;

    async fn from_request_parts(
        parts: &mut axum::http::request::Parts,
        _state: &S,
    ) -> Result<Self, Self::Rejection> {
        Ok(Self(parts.extensions.get::<StakedPeer>().copied()))
    }
}

/// Middleware: read the connection's `PeerIdentity`, map it to known node
/// accounts, and inject [`ActivePeer`] and/or [`StakedPeer`] capabilities when
/// the node satisfies those classes. Always calls `next` — gated handlers 403
/// via their extractors when the relevant capability is absent.
pub async fn authorize_peer<Db, Cluster, Blockchain>(
    State(state): State<AppState<Db, Cluster, Blockchain>>,
    mut req: Request,
    next: Next,
) -> Response
where
    Db: Store,
    Cluster: Api,
    Blockchain: Rpc,
{
    let identity = req
        .extensions()
        .get::<PeerIdentity>()
        .copied()
        .unwrap_or_default();

    if let Some(tls_pubkey) = identity.pubkey() {
        if let Some(peer) = state.context.peer_manager.peer_for_tls_pubkey(tls_pubkey) {
            let node = peer.node;
            let threshold = local_access_threshold(&state);
            if state.context.state().is_committee_peer(node) {
                req.extensions_mut().insert(ActivePeer { node, tls_pubkey });
            }
            if peer.stake >= threshold {
                req.extensions_mut().insert(StakedPeer {
                    node,
                    tls_pubkey,
                    stake: peer.stake,
                });
            }
            trace!(
                node = %node,
                tls_pubkey = %tls_pubkey,
                stake = peer.stake.0,
                access_threshold = threshold.0,
                active = state.context.state().is_committee_peer(node),
                staked = peer.stake >= threshold,
                "peer auth resolved"
            );
        } else {
            trace!(tls_pubkey = %tls_pubkey, "peer auth rejected unknown tls pubkey");
        }
    } else {
        trace!("peer auth anonymous request");
    }

    next.run(req).await
}

pub(crate) fn local_access_threshold<Db, Cluster, Blockchain>(
    state: &AppState<Db, Cluster, Blockchain>,
) -> Coin<TAPE>
where
    Db: Store,
    Cluster: Api,
    Blockchain: Rpc,
{
    state
        .context
        .peer_manager
        .get(state.context.node_address())
        .map(|peer| peer.preferences.access_threshold)
        .unwrap_or(TAPE(0))
}
