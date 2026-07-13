use std::time::Instant;

use axum::body::HttpBody;
use axum::extract::{MatchedPath, Request};
use axum::http::{header, Method, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};

/// Captures request labels before the body is consumed, then records the RED
/// metrics once the response is ready.
struct RequestProbe {
    route: Option<MatchedPath>,
    method: Method,
    start: Instant,
}

impl RequestProbe {
    fn start(req: &Request) -> Self {
        Self {
            route: req.extensions().get::<MatchedPath>().cloned(),
            method: req.method().clone(),
            start: Instant::now(),
        }
    }

    fn finish(self, response: &Response) {
        let route = self.route.as_ref().map(MatchedPath::as_str).unwrap_or("unknown");
        let m = tape_metrics::metrics();
        m.http_request_duration
            .with_label_values(&[route, self.method.as_str(), status_class(response.status())])
            .observe(self.start.elapsed().as_secs_f64());
        // Fixed-size bodies report an exact size hint here; the content-length
        // header only exists this early for handlers that set it themselves.
        let bytes = response.body().size_hint().exact().or_else(|| {
            response
                .headers()
                .get(header::CONTENT_LENGTH)
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.parse::<u64>().ok())
        });
        if let Some(bytes) = bytes {
            m.http_response_bytes_total.with_label_values(&[route]).inc_by(bytes);
        }
    }
}

/// Middleware that records serving metrics around the wrapped handler.
pub async fn instrument(req: Request, next: Next) -> Response {
    let probe = RequestProbe::start(&req);
    let response = next.run(req).await;
    probe.finish(&response);
    response
}

/// Axum handler that serves the global registry in Prometheus text format.
pub async fn metrics() -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, "text/plain; version=0.0.4; charset=utf-8")],
        tape_metrics::render(),
    )
}

fn status_class(status: StatusCode) -> &'static str {
    match status.as_u16() {
        100..=199 => "1xx",
        200..=299 => "2xx",
        300..=399 => "3xx",
        400..=499 => "4xx",
        _ => "5xx",
    }
}
