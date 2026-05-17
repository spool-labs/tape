//! Application-layer peer authorization.
//!
//! The TLS layer ([`super::peer_identity::PeerIdentityAcceptor`]) captures the
//! client cert's SPKI as a [`PeerIdentity`] and attaches it to every request
//! on that connection. This module turns that raw identity into an authorized
//! [`PeerAuth`] — a client cert whose on-chain `network_tls` maps to a known
//! peer node authorized for peer-only routes.
//!
//! Handlers that require peer auth declare `_peer: PeerAuth` in their
//! signature. Axum's extractor machinery returns `403 FORBIDDEN` if the
//! extension is absent, which happens when:
//! - the request came in on the HTTP listener (no client cert, no mTLS),
//! - the client dialled HTTPS without presenting a cert,
//! - the client cert's SPKI doesn't map to any known node, or
//! - the mapped node isn't in any committee.
//!
//! The HTTPS listener installs [`authorize_peer`] as a middleware layer so the
//! check runs on every request. The HTTP listener omits it, so `PeerAuth` is
//! unreachable from the plaintext side regardless of what the caller does.

use axum::extract::{Request, State};
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::Response;

use rpc::Rpc;
use store::Store;
use tape_core::types::tls::NetworkTlsPubkey;
use tape_crypto::Address;
use tape_protocol::Api;

use super::peer_identity::PeerIdentity;
use super::state::AppState;

/// A request caller authenticated as a known peer via mTLS.
///
/// Present in request extensions only when the handshake yielded a client cert
/// whose SPKI maps to a known node account currently authorized for peer-only
/// routes.
#[derive(Clone, Copy, Debug)]
pub struct PeerAuth {
    pub node: Address,
    pub tls_pubkey: NetworkTlsPubkey,
}

impl<S> axum::extract::FromRequestParts<S> for PeerAuth
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
            .get::<PeerAuth>()
            .copied()
            .ok_or(StatusCode::FORBIDDEN)
    }
}

/// Middleware: read the connection's `PeerIdentity`, map it to an authorized
/// peer, and inject [`PeerAuth`] into the request extensions when the mapping
/// succeeds. Always calls `next` — requests without an authenticated peer reach
/// handlers; those handlers that require auth 403 via the extractor.
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
        if let Some(node) = state.context.peer_manager.node_for_tls_pubkey(tls_pubkey) {
            if state.context.state().is_committee_peer(node) {
                req.extensions_mut().insert(PeerAuth { node, tls_pubkey });
            }
        }
    }

    next.run(req).await
}
