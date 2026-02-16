//! Prometheus metrics endpoint.

use axum::http::{header, StatusCode};
use axum::response::IntoResponse;
use prometheus::Encoder;

/// GET /v1/metrics — serve gathered prometheus metrics.
pub async fn get_metrics() -> impl IntoResponse {
    let encoder = prometheus::TextEncoder::new();
    let families = prometheus::gather();
    let mut buf = Vec::new();
    if encoder.encode(&families, &mut buf).is_err() {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to encode metrics",
        )
            .into_response();
    }
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "text/plain; version=0.0.4")],
        buf,
    )
        .into_response()
}
