use tape_metrics::prometheus::{Encoder, TextEncoder};

pub async fn metrics() -> impl axum::response::IntoResponse {
    let encoder = TextEncoder::new();
    let metric_families = tape_metrics::prometheus::gather();
    let mut buffer = Vec::new();
    encoder.encode(&metric_families, &mut buffer).unwrap();
    (
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        buffer,
    )
}
