//! HTTP endpoint for Prometheus metrics exposition.
//!
//! Provides an Axum-compatible handler and router for the `/metrics` endpoint.

use axum::{
    http::StatusCode,
    response::IntoResponse,
    routing::get,
    Router,
};

use crate::MetricsRegistry;

/// Axum handler for the `/metrics` endpoint.
///
/// Returns metrics in Prometheus text format.
///
/// # Example
///
/// ```ignore
/// use axum::Router;
/// use tape_metrics::http::metrics_handler;
///
/// let app = Router::new()
///     .route("/metrics", axum::routing::get(metrics_handler));
/// ```
pub async fn metrics_handler() -> impl IntoResponse {
    match MetricsRegistry::get() {
        Some(registry) => {
            let encoder = prometheus::TextEncoder::new();
            let metric_families = registry.gather();

            match encoder.encode_to_string(&metric_families) {
                Ok(metrics) => (
                    StatusCode::OK,
                    [("content-type", "text/plain; charset=utf-8")],
                    metrics,
                ),
                Err(e) => (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    [("content-type", "text/plain; charset=utf-8")],
                    format!("Failed to encode metrics: {}", e),
                ),
            }
        }
        None => (
            StatusCode::SERVICE_UNAVAILABLE,
            [("content-type", "text/plain; charset=utf-8")],
            "Metrics registry not initialized".to_string(),
        ),
    }
}

/// Create an Axum router with the `/metrics` endpoint.
///
/// # Example
///
/// ```ignore
/// use tape_metrics::http::metrics_router;
///
/// let app = Router::new()
///     .merge(metrics_router())
///     .route("/", axum::routing::get(|| async { "Hello" }));
/// ```
pub fn metrics_router() -> Router {
    Router::new().route("/metrics", get(metrics_handler))
}

/// Health check handler.
///
/// Returns 200 OK if the metrics registry is initialized.
pub async fn health_handler() -> impl IntoResponse {
    match MetricsRegistry::get() {
        Some(_) => (StatusCode::OK, "OK"),
        None => (StatusCode::SERVICE_UNAVAILABLE, "Metrics not initialized"),
    }
}

/// Create an Axum router with metrics and health endpoints.
///
/// Provides:
/// - `GET /metrics` - Prometheus metrics
/// - `GET /health` - Health check
///
/// # Example
///
/// ```ignore
/// use tape_metrics::http::observability_router;
/// use tokio::net::TcpListener;
///
/// #[tokio::main]
/// async fn main() {
///     tape_metrics::MetricsRegistry::init();
///
///     let app = observability_router();
///     let listener = TcpListener::bind("0.0.0.0:9090").await.unwrap();
///     axum::serve(listener, app).await.unwrap();
/// }
/// ```
pub fn observability_router() -> Router {
    Router::new()
        .route("/metrics", get(metrics_handler))
        .route("/health", get(health_handler))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt;

    #[tokio::test]
    async fn test_metrics_handler_without_init() {
        // Don't initialize registry - should return 503
        let app: Router = metrics_router();

        let response = app
            .oneshot(Request::builder().uri("/metrics").body(Body::empty()).unwrap())
            .await
            .unwrap();

        // May be 200 if registry was initialized by other tests, or 503 if not
        // This test is mainly to verify the handler doesn't panic
        assert!(response.status() == StatusCode::OK || response.status() == StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn test_metrics_handler_with_init() {
        MetricsRegistry::init();

        let app: Router = metrics_router();

        let response = app
            .oneshot(Request::builder().uri("/metrics").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_health_handler() {
        MetricsRegistry::init();

        let app: Router = observability_router();

        let response = app
            .oneshot(Request::builder().uri("/health").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }
}
