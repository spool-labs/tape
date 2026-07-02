use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::error_handling::HandleErrorLayer;
use axum::extract::{DefaultBodyLimit, Request, State};
use axum::http::StatusCode;
use axum::middleware::{Next, from_fn_with_state};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::Router;
use rpc::Rpc;
use store::Store;
use tape_node::config::http::HttpConfig;
use tape_node::context::NodeContext;
use tape_node::core::error::NodeError;
use tape_protocol::Api;
use tokio_util::sync::CancellationToken;
use tower::ServiceBuilder;
use tower::limit::ConcurrencyLimitLayer;
use tower::load_shed::LoadShedLayer;
use tower::timeout::TimeoutLayer;
use tower_http::trace::TraceLayer;
use tracing::info;

use crate::cache::GatewaySliceCache;
use crate::http::AppState;
use crate::http::handlers::{health, object, track};
use crate::meter::GatewayMeter;

pub struct GatewayHttpServer<Db: Store, Cluster: Api, Blockchain: Rpc> {
    context: Arc<NodeContext<Db, Cluster, Blockchain>>,
    slice_cache: Arc<GatewaySliceCache<Db>>,
    meter: Arc<GatewayMeter>,
    http_config: HttpConfig,
    cancel: CancellationToken,
}

impl<Db, Cluster, Blockchain> GatewayHttpServer<Db, Cluster, Blockchain>
where
    Db: Store + 'static,
    Cluster: Api + 'static,
    Blockchain: Rpc + 'static,
{
    pub fn new(
        context: Arc<NodeContext<Db, Cluster, Blockchain>>,
        http_config: HttpConfig,
        cancel: CancellationToken,
    ) -> Result<Self, NodeError> {
        let slice_cache = Arc::new(
            GatewaySliceCache::new(context.store.clone(), context.config.gateway.cache.clone())
                .map_err(|error| NodeError::Store(error.to_string()))?,
        );
        let meter = Arc::new(GatewayMeter::new(context.config.gateway.metering.clone()));

        Ok(Self {
            context,
            slice_cache,
            meter,
            http_config,
            cancel,
        })
    }

    fn build_router(&self) -> Router {
        let state = AppState {
            context: self.context.clone(),
            slice_cache: self.slice_cache.clone(),
            meter: self.meter.clone(),
        };
        let peer_body_limit = DefaultBodyLimit::max(self.http_config.peer_max_bytes);

        // Status routes serve while the gateway bootstraps; everything else
        // 503s until catch-up completes.
        let status_router = Router::new()
            .route(
                tape_protocol::api::NODE_HEALTH_PATH,
                get(health::health::<Db, Cluster, Blockchain>),
            )
            .route(
                tape_protocol::api::NODE_STATS_PATH,
                get(health::stats::<Db, Cluster, Blockchain>),
            );

        let service_router = Router::new()
            .route(
                object::OBJECT_PATH,
                get(object::get_object::<Db, Cluster, Blockchain>).layer(from_fn_with_state(
                    state.clone(),
                    crate::meter::object_read_metering::<Db, Cluster, Blockchain>,
                )),
            )
            .route(
                object::TRACK_BYTES_PATH,
                get(object::get_track_bytes::<Db, Cluster, Blockchain>).layer(
                    from_fn_with_state(
                        state.clone(),
                        crate::meter::object_read_metering::<Db, Cluster, Blockchain>,
                    ),
                ),
            )
            .route(
                tape_protocol::api::TRACK_PATH,
                get(track::catalog::get_track::<Db, Cluster, Blockchain>),
            )
            .route(
                tape_protocol::api::TRACK_DATA_PATH,
                get(track::catalog::get_track_data::<Db, Cluster, Blockchain>),
            )
            .route(
                tape_protocol::api::TRACK_PROOF_PATH,
                get(track::catalog::get_track_proof::<Db, Cluster, Blockchain>),
            )
            .route(
                tape_protocol::api::TRACK_SLICE_PATH,
                get(track::slice::get_slice::<Db, Cluster, Blockchain>),
            )
            .route(
                tape_protocol::api::TAPE_TRACK_PATH,
                get(track::catalog::get_track_by_number::<Db, Cluster, Blockchain>),
            )
            .route(
                tape_protocol::api::TAPE_TRACK_FIND_PATH,
                post(track::catalog::find_track::<Db, Cluster, Blockchain>)
                    .layer(peer_body_limit.clone()),
            )
            .route(
                tape_protocol::api::TAPE_TRACK_LIST_PATH,
                post(track::catalog::list_tracks_by_tape::<Db, Cluster, Blockchain>)
                    .layer(peer_body_limit.clone()),
            )
            .route(
                tape_protocol::api::TAPE_OBJECT_LIST_PATH,
                post(track::catalog::list_objects::<Db, Cluster, Blockchain>)
                    .layer(peer_body_limit),
            )
            .layer(from_fn_with_state(
                state.clone(),
                require_ready::<Db, Cluster, Blockchain>,
            ));

        status_router
            .merge(service_router)
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
        let listen = self.http_config.listen;
        let router = self.build_router();
        let listener = tokio::net::TcpListener::bind(listen)
            .await
            .map_err(NodeError::Io)?;

        info!(listen = %listen, "gateway http listener bound");

        axum::serve(listener, router.into_make_service_with_connect_info::<SocketAddr>())
            .with_graceful_shutdown(async move {
                self.cancel.cancelled().await;
            })
            .await
            .map_err(NodeError::Io)
    }
}

async fn handle_http_error(error: axum::BoxError) -> StatusCode {
    if error.is::<tower::timeout::error::Elapsed>() {
        StatusCode::REQUEST_TIMEOUT
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}

/// Service routes 503 until bootstrap catch-up completes; status routes are
/// registered outside this layer and serve throughout.
async fn require_ready<Db: Store, Cluster: Api, Blockchain: Rpc>(
    State(state): State<AppState<Db, Cluster, Blockchain>>,
    req: Request,
    next: Next,
) -> Response {
    if !state.context.bootstrap.is_ready() {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    }
    next.run(req).await
}

async fn count_requests<Db: Store, Cluster: Api, Blockchain: Rpc>(
    State(state): State<AppState<Db, Cluster, Blockchain>>,
    req: Request,
    next: Next,
) -> Response {
    state.context.metrics.inc_requests_total();
    next.run(req).await
}
