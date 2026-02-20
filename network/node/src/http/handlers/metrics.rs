//! Prometheus metrics endpoint.

use axum::http::{header, StatusCode};
use axum::response::IntoResponse;
use prometheus::Encoder;

/// GET /v1/metrics — serve gathered prometheus metrics.
pub async fn get_metrics() -> impl IntoResponse {
    tracing::trace!("http get_metrics start");
    let encoder = prometheus::TextEncoder::new();
    let families = prometheus::gather();
    let mut buf = Vec::new();
    if encoder.encode(&families, &mut buf).is_err() {
        tracing::trace!("http get_metrics failed to encode");
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to encode metrics",
        )
            .into_response();
    }
    tracing::trace!("http get_metrics success");
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "text/plain; version=0.0.4")],
        buf,
    )
        .into_response()
}
