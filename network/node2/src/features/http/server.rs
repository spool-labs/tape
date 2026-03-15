use axum::error_handling::HandleErrorLayer;
use axum::http::StatusCode;
use axum::routing::get;
use axum::{BoxError, Router};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tower::ServiceBuilder;
use tower::limit::ConcurrencyLimitLayer;
use tower::load_shed::LoadShedLayer;
use tower::timeout::TimeoutLayer;
use tower_http::trace::TraceLayer;
use tracing::{debug, info};

use crate::core::config::HttpConfig;
use crate::core::error::NodeError;
use crate::features::http::routes;

pub struct HttpServer {
    config: HttpConfig,
    cancel: CancellationToken,
}

impl HttpServer {
    pub fn new(config: HttpConfig, cancel: CancellationToken) -> Self {
        Self { config, cancel }
    }

    pub async fn run(self) -> Result<(), NodeError> {
        debug!(bind_addr = %self.config.bind_addr, "http server starting");

        let app = Router::new().route("/health", get(routes::health)).layer(
            ServiceBuilder::new()
                .layer(HandleErrorLayer::new(handle_http_error))
                .layer(TraceLayer::new_for_http())
                .layer(LoadShedLayer::new())
                .layer(ConcurrencyLimitLayer::new(self.config.concurrency_limit))
                .layer(TimeoutLayer::new(self.config.request_timeout)),
        );

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
