use std::sync::Arc;

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

use crate::config::HttpConfig;
use crate::context::NodeContext;
use crate::core::error::NodeError;
use crate::features::http::handlers;
use crate::features::http::state::AppState;

pub struct HttpServer<Db: Store, Cluster: Api, Blockchain: Rpc> {
    context: Arc<NodeContext<Db, Cluster, Blockchain>>,
    config: HttpConfig,
    cancel: CancellationToken,
}

impl<Db: Store + 'static, Cluster: Api + 'static, Blockchain: Rpc + 'static>
    HttpServer<Db, Cluster, Blockchain>
{
    pub fn new(
        context: Arc<NodeContext<Db, Cluster, Blockchain>>,
        config: HttpConfig,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            context,
            config,
            cancel,
        }
    }

    fn build_router(&self) -> Router {
        let state = AppState {
            context: self.context.clone(),
        };

        Router::new()
            .route(
                api_routes::HEALTH_PATH,
                get(handlers::health::health::<Db, Cluster, Blockchain>),
            )
            .route(
                api_routes::STATS_PATH,
                get(handlers::health::stats::<Db, Cluster, Blockchain>),
            )
            .route(
                api_routes::SLICE_PATH,
                get(handlers::slice::get_slice::<Db, Cluster, Blockchain>)
                    .put(handlers::slice::put_slice::<Db, Cluster, Blockchain>),
            )
            .route(
                api_routes::METADATA_PATH,
                get(handlers::metadata::get_metadata::<Db, Cluster, Blockchain>),
            )
            .route(
                api_routes::SYNC_SPOOL_PATH,
                post(handlers::sync::sync_spool::<Db, Cluster, Blockchain>),
            )
            .route(
                api_routes::REPAIR_PATH,
                post(handlers::repair::repair::<Db, Cluster, Blockchain>),
            )
            .route(
                api_routes::SIGN_PATH,
                get(handlers::sign::certify::<Db, Cluster, Blockchain>),
            )
            .route(
                api_routes::INCONSISTENCY_PATH,
                post(handlers::inconsistency::invalidate::<Db, Cluster, Blockchain>),
            )
            .route(
                api_routes::SNAPSHOT_COMMITMENTS_PATH,
                get(handlers::snapshot::get_snapshot::<Db, Cluster, Blockchain>),
            )
            .route(
                api_routes::SNAPSHOT_SIG_PATH,
                post(handlers::sign::put_snapshot::<Db, Cluster, Blockchain>),
            )
            .with_state(state)
            .layer(from_fn_with_state(
                AppState {
                    context: self.context.clone(),
                },
                count_requests::<Db, Cluster, Blockchain>,
            ))
            .layer(DefaultBodyLimit::disable())
            .layer(
                ServiceBuilder::new()
                    .layer(HandleErrorLayer::new(handle_http_error))
                    .layer(TraceLayer::new_for_http())
                    .layer(LoadShedLayer::new())
                    .layer(ConcurrencyLimitLayer::new(self.config.concurrency_limit))
                    .layer(TimeoutLayer::new(self.config.request_timeout)),
            )
    }

    pub async fn run(self) -> Result<(), NodeError> {
        debug!(bind_addr = %self.config.bind_addr, "http server starting");

        let app = self.build_router();
        let listener = TcpListener::bind(self.config.bind_addr)
            .await
            .map_err(NodeError::Io)?;

        info!(address = %self.config.bind_addr, "http server listening");

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
