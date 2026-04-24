use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::{Request, State};
use axum::error_handling::HandleErrorLayer;
use axum::extract::DefaultBodyLimit;
use axum::http::StatusCode;
use axum::middleware::{from_fn_with_state, Next};
use axum::routing::{get, post};
use axum::response::Response;
use axum::{BoxError, Router};

use axum_server::tls_rustls::{RustlsAcceptor, RustlsConfig};
use axum_server::Handle;
use peer_tls::{
    build_server_config_from_pem, build_server_config_with_peer_auth, install_default_provider,
};

use crate::features::http::peer_identity::PeerIdentityAcceptor;
use rpc::Rpc;
use store::Store;
use tape_protocol::Api;
use tape_protocol::api::routes as api_routes;
use tokio_util::sync::CancellationToken;
use tower::ServiceBuilder;
use tower::limit::ConcurrencyLimitLayer;
use tower::load_shed::LoadShedLayer;
use tower::timeout::TimeoutLayer;
use tower_http::trace::TraceLayer;
use tracing::{debug, info};

use crate::config::http::HttpConfig;
use crate::context::NodeContext;
use crate::core::error::NodeError;
use crate::features::http::handlers;
use crate::features::http::state::AppState;

pub struct HttpServer<Db: Store, Cluster: Api, Blockchain: Rpc> {
    context: Arc<NodeContext<Db, Cluster, Blockchain>>,
    config: HttpConfig,
    #[cfg_attr(not(feature = "metrics"), allow(dead_code))]
    metrics_enabled: bool,
    cancel: CancellationToken,
}

impl<Db: Store + 'static, Cluster: Api + 'static, Blockchain: Rpc + 'static>
    HttpServer<Db, Cluster, Blockchain>
{
    pub fn new(
        context: Arc<NodeContext<Db, Cluster, Blockchain>>,
        config: HttpConfig,
        metrics_enabled: bool,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            context,
            config,
            metrics_enabled,
            cancel,
        }
    }

    fn build_router(&self) -> Router {
        let state = AppState {
            context: self.context.clone(),
        };

        #[allow(unused_mut)]
        let mut public_routes = Router::new()
            .route(
                api_routes::NODE_HEALTH_PATH,
                get(handlers::health::health::<Db, Cluster, Blockchain>),
            )
            .route(
                api_routes::TRACK_PATH,
                get(handlers::track::catalog::get_track::<Db, Cluster, Blockchain>),
            )
            .route(
                api_routes::TRACK_DATA_PATH,
                get(handlers::track::catalog::get_track_data::<Db, Cluster, Blockchain>),
            )
            .route(
                api_routes::TRACK_PROOF_PATH,
                get(handlers::track::catalog::get_track_proof::<Db, Cluster, Blockchain>),
            )
            .route(
                api_routes::TAPE_TRACK_PATH,
                get(handlers::track::catalog::get_track_by_number::<Db, Cluster, Blockchain>),
            )
            .route(
                api_routes::TRACK_SIGN_PATH,
                get(handlers::track::sign::certify::<Db, Cluster, Blockchain>),
            );

        #[cfg(feature = "metrics")]
        if self.metrics_enabled {
            public_routes = public_routes.route(
                api_routes::NODE_METRICS_PATH,
                get(handlers::metrics::metrics),
            );
        }

        let slice_routes = Router::new()
            .route(
                api_routes::TRACK_SLICE_PATH,
                get(handlers::track::slice::get_slice::<Db, Cluster, Blockchain>)
                    .put(handlers::track::slice::put_slice::<Db, Cluster, Blockchain>),
            )
            .layer(DefaultBodyLimit::max(self.config.slice_max_bytes));

        let public_post_routes = Router::new()
            .route(
                api_routes::TAPE_TRACK_FIND_PATH,
                post(handlers::track::catalog::find_track::<Db, Cluster, Blockchain>),
            )
            .route(
                api_routes::TAPE_TRACK_LIST_PATH,
                post(handlers::track::catalog::list_tracks_by_tape::<Db, Cluster, Blockchain>),
            )
            .route(
                api_routes::TRACK_REPAIR_PATH,
                post(handlers::track::repair::repair::<Db, Cluster, Blockchain>),
            )
            .layer(DefaultBodyLimit::max(self.config.peer_max_bytes));

        // Peer-only routes: gated by the committee-membership middleware so
        // that only nodes in the current/prev/next committee (authenticated
        // via mTLS) may call them. Anonymous CLI connections and non-peer
        // clients are rejected with 403.
        let peer_only_routes = Router::new()
            .route(
                api_routes::NODE_STATS_PATH,
                get(handlers::health::stats::<Db, Cluster, Blockchain>),
            )
            .route(
                api_routes::SYNC_SLICES_PATH,
                post(handlers::track::sync::sync_slices::<Db, Cluster, Blockchain>),
            )
            .route(
                api_routes::SYNC_TRACKS_PATH,
                post(handlers::track::sync::sync_tracks::<Db, Cluster, Blockchain>),
            )
            .route(
                api_routes::SNAPSHOT_VOTE_PATH,
                post(handlers::snapshot::vote::vote::<Db, Cluster, Blockchain>),
            )
            .route(
                api_routes::TRACK_INCONSISTENCY_PATH,
                post(handlers::track::inconsistency::invalidate::<Db, Cluster, Blockchain>),
            )
            .layer(DefaultBodyLimit::max(self.config.peer_max_bytes))
            .layer(from_fn_with_state(
                state.clone(),
                require_committee_peer::<Db, Cluster, Blockchain>,
            ));

        public_routes
            .merge(slice_routes)
            .merge(public_post_routes)
            .merge(peer_only_routes)
            .with_state(state)
            .layer(from_fn_with_state(
                AppState {
                    context: self.context.clone(),
                },
                count_requests::<Db, Cluster, Blockchain>,
            ))
            .layer(
                ServiceBuilder::new()
                    .layer(HandleErrorLayer::new(handle_http_error))
                    .layer(TraceLayer::new_for_http())
                    .layer(LoadShedLayer::new())
                    .layer(ConcurrencyLimitLayer::new(self.config.concurrency))
                    .layer(TimeoutLayer::new(Duration::from_secs(
                        self.config.timeout_secs,
                    ))),
            )
    }

    fn build_loopback_router(&self) -> Router {
        let state = AppState {
            context: self.context.clone(),
        };

        #[allow(unused_mut)]
        let mut router = Router::new()
            .route(
                api_routes::NODE_HEALTH_PATH,
                get(handlers::health::health::<Db, Cluster, Blockchain>),
            )
            .route(
                api_routes::NODE_STATS_PATH,
                get(handlers::health::stats::<Db, Cluster, Blockchain>),
            );

        #[cfg(feature = "metrics")]
        if self.metrics_enabled {
            router = router.route(
                api_routes::NODE_METRICS_PATH,
                get(handlers::metrics::metrics),
            );
        }

        router.with_state(state)
    }

    pub async fn run(self) -> Result<(), NodeError> {
        debug!(listen = %self.config.listen, "https server starting");

        install_default_provider();

        let listen_ip = self.config.listen.ip();
        let san_ips = cert_san_ips(listen_ip);

        let server_config = match &self.context.config.tls.certificate_path {
            Some(pem_path) => {
                info!(
                    cert = %pem_path.display(),
                    "loading operator-supplied PEM cert for peer TLS"
                );
                build_server_config_from_pem(pem_path, self.context.tls_keypair(), true)
                    .map_err(|e| NodeError::Config(format!("tls server config: {e}")))?
            }
            None => build_server_config_with_peer_auth(self.context.tls_keypair(), &san_ips)
                .map_err(|e| NodeError::Config(format!("tls server config: {e}")))?,
        };
        let rustls_config = RustlsConfig::from_config(server_config);
        let acceptor = PeerIdentityAcceptor::new(RustlsAcceptor::new(rustls_config));

        let app = self.build_router();

        info!(
            listen = %self.config.listen,
            tls_pubkey = %self.context.tls_pubkey(),
            "https server listening"
        );

        let handle = Handle::new();
        let shutdown_handle = handle.clone();
        let cancel = self.cancel.clone();
        let shutdown_task = tokio::spawn(async move {
            cancel.cancelled().await;
            shutdown_handle.graceful_shutdown(Some(Duration::from_secs(10)));
        });

        let loopback_task = match self.context.config.tls.local_plaintext_listen {
            Some(addr) if addr.ip().is_loopback() => {
                let loopback_router = self.build_loopback_router();
                let cancel = self.cancel.clone();
                info!(listen = %addr, "loopback plain-http listener starting");
                Some(tokio::spawn(async move {
                    match tokio::net::TcpListener::bind(addr).await {
                        Ok(listener) => {
                            let _ = axum::serve(listener, loopback_router)
                                .with_graceful_shutdown(async move {
                                    cancel.cancelled().await;
                                })
                                .await;
                        }
                        Err(err) => {
                            tracing::error!(%addr, error = %err, "loopback listener failed to bind");
                        }
                    }
                }))
            }
            Some(addr) => {
                return Err(NodeError::Config(format!(
                    "tls.local_plaintext_listen={addr} must be a loopback address (127.0.0.0/8 or ::1)"
                )));
            }
            None => None,
        };

        let result = axum_server::bind(self.config.listen)
            .acceptor(acceptor)
            .handle(handle)
            .serve(app.into_make_service())
            .await
            .map_err(NodeError::Io);

        shutdown_task.abort();
        let _ = shutdown_task.await;
        if let Some(task) = loopback_task {
            task.abort();
            let _ = task.await;
        }
        result
    }
}

/// Expand the cert SAN list. When the server listens on `0.0.0.0`/`::`, include
/// loopback as well so health checks and local peer dials succeed.
fn cert_san_ips(listen_ip: IpAddr) -> Vec<IpAddr> {
    use std::net::{Ipv4Addr, Ipv6Addr};

    let mut sans = vec![listen_ip];
    if listen_ip.is_unspecified() {
        sans.push(IpAddr::V4(Ipv4Addr::LOCALHOST));
        sans.push(IpAddr::V6(Ipv6Addr::LOCALHOST));
    }
    sans
}

async fn handle_http_error(error: BoxError) -> StatusCode {
    if error.is::<tower::timeout::error::Elapsed>() {
        StatusCode::REQUEST_TIMEOUT
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}

async fn count_requests<Db: Store, Cluster: Api, Blockchain: Rpc>(
    State(state): State<AppState<Db, Cluster, Blockchain>>,
    req: Request,
    next: Next,
) -> Response {
    state.context.metrics.inc_requests_total();
    next.run(req).await
}

/// Middleware for peer-only routes. Rejects anonymous requests (no client
/// cert) and requests from pubkeys that don't match any node in the current,
/// previous, or next committee. The mTLS handshake has already proven key
/// possession; this is the authorization layer.
async fn require_committee_peer<Db: Store, Cluster: Api, Blockchain: Rpc>(
    State(state): State<AppState<Db, Cluster, Blockchain>>,
    req: Request,
    next: Next,
) -> Result<Response, StatusCode> {
    use crate::features::http::peer_identity::PeerIdentity;

    let identity = req
        .extensions()
        .get::<PeerIdentity>()
        .copied()
        .unwrap_or_default();

    let Some(tls_pubkey) = identity.pubkey() else {
        return Err(StatusCode::FORBIDDEN);
    };

    let Some(node_id) = state.context.peer_manager.node_for_tls_pubkey(tls_pubkey) else {
        return Err(StatusCode::FORBIDDEN);
    };

    if !state.context.state().is_committee_peer(node_id) {
        return Err(StatusCode::FORBIDDEN);
    }

    Ok(next.run(req).await)
}
