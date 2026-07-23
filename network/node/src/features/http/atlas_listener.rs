//! Dedicated pinned-mTLS listener serving the atlas snapshot to configured
//! observers. The node folds the atlas route into its main peer listener;
//! this standalone form exists for services whose public listeners sit behind
//! proxies and never see client certificates, the gateway foremost.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::{Query, Request, State};
use axum::middleware::{from_fn_with_state, Next};
use axum::response::Response;
use axum::routing::get;
use axum::{Json, Router};
use axum_server::tls_rustls::{RustlsAcceptor, RustlsConfig};
use axum_server::Handle;
use peer_tls::{build_server_config_with_peer_auth, cert_san_ips};
use rpc::Rpc;
use store::Store;
use tape_observe_api::AtlasRecent;
use tape_protocol::api::OBSERVE_ATLAS_PATH;
use tape_protocol::Api;
use tokio_util::sync::CancellationToken;

use crate::context::NodeContext;
use crate::core::error::NodeError;
use crate::features::http::auth::{observer_capability, ObserverPeer};
use crate::features::http::handlers::atlas::AtlasQuery;
use crate::features::http::peer_identity::{PeerIdentity, PeerIdentityAcceptor};

const SHUTDOWN_GRACE: Duration = Duration::from_secs(5);

/// Serve the atlas snapshot over pinned mTLS until cancelled
pub async fn serve<Db, Cluster, Blockchain>(
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
    if let Some(observer) = observer_capability(&context.atlas, identity) {
        req.extensions_mut().insert(observer);
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
