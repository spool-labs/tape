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

use rpc::Rpc;
use store::Store;
use tape_protocol::Api;
use tape_protocol::api::routes as api_routes;
use tokio::net::TcpListener;
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
        let mut base_routes = Router::new()
            .route(
                api_routes::HEALTH_PATH,
                get(handlers::health::health::<Db, Cluster, Blockchain>),
            )
            .route(
                api_routes::STATS_PATH,
                get(handlers::health::stats::<Db, Cluster, Blockchain>),
            )
            .route(
                api_routes::TRACK_PATH,
                get(handlers::track::get_track::<Db, Cluster, Blockchain>),
            )
            .route(
                api_routes::TRACK_DATA_PATH,
                get(handlers::track::get_track_data::<Db, Cluster, Blockchain>),
            )
            .route(
                api_routes::TRACK_PROOF_PATH,
                get(handlers::track::get_track_proof::<Db, Cluster, Blockchain>),
            )
            .route(
                api_routes::TAPE_TRACK_PATH,
                get(handlers::track::get_track_by_number::<Db, Cluster, Blockchain>),
            )
            .route(
                api_routes::SIGN_PATH,
                get(handlers::sign::certify::<Db, Cluster, Blockchain>),
            );

        #[cfg(feature = "metrics")]
        if self.metrics_enabled {
            base_routes = base_routes.route(
                api_routes::METRICS_PATH,
                get(handlers::metrics::metrics),
            );
        }

        let slice_routes = Router::new()
            .route(
                api_routes::SLICE_PATH,
                get(handlers::slice::get_slice::<Db, Cluster, Blockchain>)
                    .put(handlers::slice::put_slice::<Db, Cluster, Blockchain>),
            )
            .layer(DefaultBodyLimit::max(self.config.slice_max_bytes));

        let peer_post_routes = Router::new()
            .route(
                api_routes::TAPE_FIND_TRACK_PATH,
                post(handlers::track::find_track::<Db, Cluster, Blockchain>),
            )
            .route(
                api_routes::TAPE_LIST_TRACKS_PATH,
                post(handlers::track::list_tracks_by_tape::<Db, Cluster, Blockchain>),
            )
            .route(
                api_routes::SYNC_SLICES_PATH,
                post(handlers::sync::sync_slices::<Db, Cluster, Blockchain>),
            )
            .route(
                api_routes::SYNC_TRACKS_PATH,
                post(handlers::sync::sync_tracks::<Db, Cluster, Blockchain>),
            )
            .route(
                api_routes::REPAIR_PATH,
                post(handlers::repair::repair::<Db, Cluster, Blockchain>),
            )
            .route(
                api_routes::INCONSISTENCY_PATH,
                post(handlers::inconsistency::invalidate::<Db, Cluster, Blockchain>),
            )
            .layer(DefaultBodyLimit::max(self.config.peer_max_bytes));

        base_routes
            .merge(slice_routes)
            .merge(peer_post_routes)
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

    pub async fn run(self) -> Result<(), NodeError> {
        debug!(listen = %self.config.listen, "http server starting");

        let app = self.build_router();
        let listener = TcpListener::bind(self.config.listen)
            .await
            .map_err(NodeError::Io)?;

        info!(listen = %self.config.listen, "http server listening");

        let cancel = self.cancel.clone();

        axum::serve(listener, app)
            .with_graceful_shutdown(async move {
                cancel.cancelled().await;
            })
            .await
            .map_err(NodeError::Io)
    }
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
