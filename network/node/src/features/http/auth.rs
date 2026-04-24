//! Application-layer peer authorization.
//!
//! The TLS layer ([`super::peer_identity::PeerIdentityAcceptor`]) captures the
//! client cert's SPKI as a [`PeerIdentity`] and attaches it to every request
//! on that connection. This module turns that raw identity into an authorized
//! [`PeerCommitteeMember`] — a client cert whose on-chain `network_tls` maps
//! to a known node in the current, previous, or next committee.
//!
//! Handlers that require peer auth declare `_peer: PeerCommitteeMember` in
//! their signature. Axum's extractor machinery returns `403 FORBIDDEN` if the
//! extension is absent, which happens when:
//! - the request came in on the HTTP listener (no client cert, no mTLS),
//! - the client dialled HTTPS without presenting a cert,
//! - the client cert's SPKI doesn't map to any known node, or
//! - the mapped node isn't in any committee.
//!
//! The HTTPS listener installs [`resolve_peer_membership`] as a middleware
//! layer so the check runs on every request. The HTTP listener omits it, so
//! `PeerCommitteeMember` is unreachable from the plaintext side regardless of
//! what the caller does.

use axum::extract::{Request, State};
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::Response;

use rpc::Rpc;
use store::Store;
use tape_core::types::NodeId;
use tape_core::types::tls::NetworkTlsPubkey;
use tape_protocol::Api;

use super::peer_identity::PeerIdentity;
use super::state::AppState;

/// A request caller that has been authenticated as a known committee peer
/// via mTLS. Present in request extensions only when the handshake yielded
/// a client cert whose SPKI maps to a current/previous/next committee member.
#[derive(Clone, Copy, Debug)]
pub struct PeerCommitteeMember {
    pub node_id: NodeId,
    pub tls_pubkey: NetworkTlsPubkey,
}

impl<S> axum::extract::FromRequestParts<S> for PeerCommitteeMember
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
            .get::<PeerCommitteeMember>()
            .copied()
            .ok_or(StatusCode::FORBIDDEN)
    }
}

/// Middleware: read the connection's `PeerIdentity`, map it to a committee
/// peer, and inject [`PeerCommitteeMember`] into the request extensions when
/// the mapping succeeds. Always calls `next` — requests without an
/// authenticated peer reach handlers; those handlers that require auth 403
/// via the extractor.
pub async fn resolve_peer_membership<Db, Cluster, Blockchain>(
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
        if let Some(node_id) = state.context.peer_manager.node_for_tls_pubkey(tls_pubkey) {
            if state.context.state().is_committee_peer(node_id) {
                req.extensions_mut().insert(PeerCommitteeMember { node_id, tls_pubkey });
            }
        }
    }

    next.run(req).await
}
