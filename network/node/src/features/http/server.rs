use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::error_handling::HandleErrorLayer;
use axum::extract::DefaultBodyLimit;
use axum::extract::{Request, State};
use axum::http::StatusCode;
use axum::middleware::{Next, from_fn_with_state};
use axum::response::Response;
use axum::routing::{get, post};
use axum::{BoxError, Router};

use axum_server::Handle;
use axum_server::tls_rustls::{RustlsAcceptor, RustlsConfig};
use peer_tls::{build_server_config_with_peer_auth, install_default_provider};

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
use tracing::{debug, info, Instrument};

use crate::config::http::HttpConfig;
use crate::config::https::HttpsConfig;
use crate::context::NodeContext;
use crate::core::error::NodeError;
use crate::features::http::auth::authorize_peer;
use crate::features::http::handlers;
use crate::features::http::peer_identity::PeerIdentityAcceptor;
use crate::features::http::state::AppState;

pub struct HttpServer<Db: Store, Cluster: Api, Blockchain: Rpc> {
    context: Arc<NodeContext<Db, Cluster, Blockchain>>,
    http_config: HttpConfig,
    https_config: HttpsConfig,
    #[cfg_attr(not(feature = "metrics"), allow(dead_code))]
    metrics_enabled: bool,
    cancel: CancellationToken,
}

impl<Db: Store + 'static, Cluster: Api + 'static, Blockchain: Rpc + 'static>
    HttpServer<Db, Cluster, Blockchain>
{
    pub fn new(
        context: Arc<NodeContext<Db, Cluster, Blockchain>>,
        http_config: HttpConfig,
        https_config: HttpsConfig,
        metrics_enabled: bool,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            context,
            http_config,
            https_config,
            metrics_enabled,
            cancel,
        }
    }

    /// Build the shared router that backs both listeners. Authorization lives
    /// at the handler level via the [`ActivePeer`] and [`StakedPeer`]
    /// extractors, not at the Router-composition level. The HTTPS listener
    /// separately layers the [`authorize_peer`] middleware so that
    /// mTLS-authenticated callers can satisfy those extractors; the HTTP
    /// listener does not, so gated handlers on plaintext fall through to 403.
    ///
    /// [`ActivePeer`]: crate::features::http::auth::ActivePeer
    /// [`StakedPeer`]: crate::features::http::auth::StakedPeer
    fn build_router(&self) -> Router {
        let state = AppState {
            context: self.context.clone(),
        };

        let slice_body_limit = DefaultBodyLimit::max(self.http_config.slice_max_bytes);
        let peer_body_limit = DefaultBodyLimit::max(self.http_config.peer_max_bytes);

        #[allow(unused_mut)]
        let mut router = Router::new()
            // Anonymous monitoring.
            .route(
                api_routes::NODE_HEALTH_PATH,
                get(handlers::health::health::<Db, Cluster, Blockchain>),
            )
            .route(
                api_routes::NODE_STATS_PATH,
                get(handlers::health::stats::<Db, Cluster, Blockchain>),
            )
            // Open metadata GETs used by SDK write flows. Payload-bearing
            // inline data and slice reads remain gated.
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
            // Open direct-write support.
            .route(
                api_routes::TRACK_SIGN_PATH,
                get(handlers::track::sign::certify::<Db, Cluster, Blockchain>),
            )
            // Staked-peer gated slice GET + self-authorizing PUT.
            .route(
                api_routes::TRACK_SLICE_PATH,
                get(handlers::track::slice::get_slice::<Db, Cluster, Blockchain>)
                    .put(handlers::track::slice::put_slice::<Db, Cluster, Blockchain>)
                    .layer(slice_body_limit),
            )
            // Staked-peer gated POSTs. Snapshot/system tape catalogs are also
            // listable for bootstrap catch-up.
            .route(
                api_routes::TAPE_TRACK_FIND_PATH,
                post(handlers::track::catalog::find_track::<Db, Cluster, Blockchain>)
                    .layer(peer_body_limit.clone()),
            )
            .route(
                api_routes::TAPE_TRACK_LIST_PATH,
                post(handlers::track::catalog::list_tracks_by_tape::<Db, Cluster, Blockchain>)
                    .layer(peer_body_limit.clone()),
            )
            .route(
                api_routes::TAPE_OBJECT_LIST_PATH,
                post(handlers::track::catalog::list_objects::<Db, Cluster, Blockchain>)
                    .layer(peer_body_limit.clone()),
            )
            .route(
                api_routes::TRACK_REPAIR_PATH,
                post(handlers::track::repair::repair::<Db, Cluster, Blockchain>)
                    .layer(peer_body_limit.clone()),
            )
            // Active-peer only: handler signatures carry `_active_peer: ActivePeer`,
            // which 403s when the extension isn't present (plaintext listener,
            // anonymous HTTPS, unknown/non-committee pubkey).
            .route(
                api_routes::SYNC_SLICES_PATH,
                post(handlers::track::sync::sync_slices::<Db, Cluster, Blockchain>)
                    .layer(peer_body_limit.clone()),
            )
            .route(
                api_routes::SYNC_TRACKS_PATH,
                post(handlers::track::sync::sync_tracks::<Db, Cluster, Blockchain>)
                    .layer(peer_body_limit.clone()),
            )
            .route(
                api_routes::VOTE_PATH,
                post(handlers::vote::vote::<Db, Cluster, Blockchain>)
                    .layer(peer_body_limit.clone()),
            )
            .route(
                api_routes::TRACK_INCONSISTENCY_PATH,
                post(handlers::track::inconsistency::invalidate::<Db, Cluster, Blockchain>)
                    .layer(peer_body_limit),
            );

        #[cfg(feature = "metrics")]
        if self.metrics_enabled {
            router = router.route(
                api_routes::NODE_METRICS_PATH,
                get(handlers::metrics::metrics),
            );
        }

        router
            .with_state(state.clone())
            .layer(from_fn_with_state(
                state,
                count_requests::<Db, Cluster, Blockchain>,
            ))
            .layer(
                ServiceBuilder::new()
                    .layer(HandleErrorLayer::new(handle_http_error))
                    .layer(TraceLayer::new_for_http())
                    .layer(LoadShedLayer::new())
                    .layer(ConcurrencyLimitLayer::new(self.http_config.concurrency))
                    .layer(TimeoutLayer::new(Duration::from_secs(
                        self.http_config.timeout_secs,
                    ))),
            )
    }

    pub async fn run(self) -> Result<(), NodeError> {
        install_default_provider();

        let https_listen = self.https_config.listen;
        let http_listen = self.http_config.listen;
        let san_ips = cert_san_ips(https_listen.ip());

        let server_config = build_server_config_with_peer_auth(self.context.tls_keypair(), &san_ips)
            .map_err(|e| NodeError::Config(format!("tls server config: {e}")))?;
        let rustls_config = RustlsConfig::from_config(server_config);
        let acceptor = PeerIdentityAcceptor::new(RustlsAcceptor::new(rustls_config));

        let shared_router = self.build_router();
        let auth_state = AppState {
            context: self.context.clone(),
        };
        let https_router = shared_router.clone().layer(from_fn_with_state(
            auth_state,
            authorize_peer::<Db, Cluster, Blockchain>,
        ));
        let http_router = shared_router;

        info!(
            http = %http_listen,
            https = %https_listen,
            tls_pubkey = %self.context.tls_pubkey(),
            "http/https listeners starting"
        );
        debug!(https = %https_listen, "https server starting");

        let https_handle = Handle::new();
        let shutdown_handle = https_handle.clone();
        let cancel = self.cancel.clone();
        let shutdown_task = tokio::spawn(
            async move {
                cancel.cancelled().await;
                shutdown_handle.graceful_shutdown(Some(Duration::from_secs(10)));
            }
            .in_current_span(),
        );

        let http_cancel = self.cancel.clone();
        let http_task = tokio::spawn(
            async move {
                match tokio::net::TcpListener::bind(http_listen).await {
                    Ok(listener) => {
                        info!(listen = %http_listen, "http listener bound");
                        let _ = axum::serve(listener, http_router)
                            .with_graceful_shutdown(async move {
                                http_cancel.cancelled().await;
                            })
                            .await;
                    }
                    Err(err) => {
                        tracing::error!(listen = %http_listen, error = %err, "http listener failed to bind");
                    }
                }
            }
            .in_current_span(),
        );

        let result = axum_server::bind(https_listen)
            .acceptor(acceptor)
            .handle(https_handle)
            .serve(https_router.into_make_service())
            .await
            .map_err(NodeError::Io);

        shutdown_task.abort();
        let _ = shutdown_task.await;
        http_task.abort();
        let _ = http_task.await;
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
