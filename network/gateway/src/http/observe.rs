//! Optional mTLS listener exposing the atlas snapshot to configured
//! observers. The gateway's public listeners sit behind proxies and never see
//! client certificates, so the observer check gets its own small pinned-key
//! server, the same dance nodes use between themselves.

use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use axum::extract::{Query, Request, State};
use axum::middleware::{from_fn_with_state, Next};
use axum::response::Response;
use axum::routing::get;
use axum::{Json, Router};
use axum_server::tls_rustls::{RustlsAcceptor, RustlsConfig};
use axum_server::Handle;
use peer_tls::build_server_config_with_peer_auth;
use rpc::Rpc;
use store::Store;
use tokio_util::sync::CancellationToken;

use tape_node::context::NodeContext;
use tape_node::core::error::NodeError;
use tape_node::features::http::auth::ObserverPeer;
use tape_node::features::http::handlers::atlas::AtlasQuery;
use tape_node::features::http::peer_identity::{PeerIdentity, PeerIdentityAcceptor};
use tape_observe_api::AtlasRecent;
use tape_protocol::api::OBSERVE_ATLAS_PATH;
use tape_protocol::Api;

const SHUTDOWN_GRACE: Duration = Duration::from_secs(5);

/// Serve the atlas snapshot over pinned mTLS until cancelled
pub async fn run<Db, Cluster, Blockchain>(
    context: Arc<NodeContext<Db, Cluster, Blockchain>>,
    listen: SocketAddr,
    cancel: CancellationToken,
) -> Result<(), NodeError>
where
    Db: Store + 'static,
    Cluster: Api + 'static,
    Blockchain: Rpc + 'static,
{
    let san_ips = cert_san_ips(listen.ip());
    let server_config = build_server_config_with_peer_auth(context.tls_keypair(), &san_ips)
        .map_err(|error| NodeError::Config(format!("observe tls config: {error}")))?;
    let acceptor = PeerIdentityAcceptor::new(RustlsAcceptor::new(RustlsConfig::from_config(
        server_config,
    )));

    let router = Router::new()
        .route(OBSERVE_ATLAS_PATH, get(recent::<Db, Cluster, Blockchain>))
        .layer(from_fn_with_state(
            context.clone(),
            authorize_observer::<Db, Cluster, Blockchain>,
        ))
        .with_state(context);

    let handle = Handle::new();
    let shutdown_handle = handle.clone();
    let shutdown = tokio::spawn(async move {
        cancel.cancelled().await;
        shutdown_handle.graceful_shutdown(Some(SHUTDOWN_GRACE));
    });

    let result = axum_server::bind(listen)
        .acceptor(acceptor)
        .handle(handle)
        .serve(router.into_make_service_with_connect_info::<SocketAddr>())
        .await
        .map_err(NodeError::Io);

    shutdown.abort();
    let _ = shutdown.await;
    result
}

/// Grant the observer capability when the client certificate is allowlisted
async fn authorize_observer<Db, Cluster, Blockchain>(
    State(context): State<Arc<NodeContext<Db, Cluster, Blockchain>>>,
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
    if let Some(key) = identity.pubkey() {
        if context.atlas.is_observer(key) {
            req.extensions_mut().insert(ObserverPeer { tls_pubkey: key });
        }
    }
    next.run(req).await
}

async fn recent<Db, Cluster, Blockchain>(
    State(context): State<Arc<NodeContext<Db, Cluster, Blockchain>>>,
    _observer: ObserverPeer,
    Query(query): Query<AtlasQuery>,
) -> Json<AtlasRecent>
where
    Db: Store,
    Cluster: Api,
    Blockchain: Rpc,
{
    Json(context.atlas.recent(query.after))
}

/// Include loopback in the certificate when listening on every interface, so
/// a co-located collector can dial over localhost
fn cert_san_ips(listen_ip: IpAddr) -> Vec<IpAddr> {
    use std::net::{Ipv4Addr, Ipv6Addr};

    let mut sans = vec![listen_ip];
    if listen_ip.is_unspecified() {
        sans.push(IpAddr::V4(Ipv4Addr::LOCALHOST));
        sans.push(IpAddr::V6(Ipv6Addr::LOCALHOST));
    }
    sans
}
