//! Request-id middleware for the gateway listeners: honor or mint a
//! correlation id, keep it in scope for the request so outbound peer calls
//! carry it, and stamp it on the response for the caller to quote.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use axum::body::Body;
use axum::extract::Request;
use axum::http::HeaderValue;
use axum::middleware::Next;
use axum::response::Response;
use tape_protocol::api::{
    REQUEST_ID_HEADER, current_request_id, sanitize_request_id, with_request_id,
};

/// 64-bit odd "golden ratio" multiplier used to bit-mix the monotonic counter.
const REQUEST_ID_MIX: u64 = 0x9E37_79B9_7F4A_7C15;

/// Generate a request id (uppercase hex). Unique-ish without a random
/// dependency: a process-wide monotonic counter mixed with the wall clock.
pub fn next_request_id() -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let sequence = COUNTER.fetch_add(1, Ordering::Relaxed);
    let nanoseconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_nanos() as u64)
        .unwrap_or(0);
    format!("{:016X}", nanoseconds ^ sequence.wrapping_mul(REQUEST_ID_MIX))
}

/// Middleware: the id is in scope for the handler (and every peer call it
/// makes), and on the response. An inbound proxy id is honored when sane.
pub async fn request_id(request: Request, next: Next) -> Response {
    let id = request
        .headers()
        .get(REQUEST_ID_HEADER)
        .and_then(|value| value.to_str().ok())
        .and_then(sanitize_request_id)
        .map(str::to_string)
        .unwrap_or_else(next_request_id);

    let header = HeaderValue::from_str(&id).ok();
    let mut response = with_request_id(id, next.run(request)).await;
    if let Some(value) = header {
        response.headers_mut().insert(REQUEST_ID_HEADER, value);
    }
    response
}

/// The gateway's request span. The trace layer builds it while polling the
/// middleware-scoped future, so the id comes from the task-local.
pub fn request_span(request: &axum::http::Request<Body>) -> tracing::Span {
    let request_id = current_request_id();
    tape_protocol::api::request_span(request.method(), request.uri(), request_id.as_deref())
}

#[cfg(test)]
mod tests {
    use axum::Router;
    use axum::routing::get;
    use tape_protocol::api::current_request_id;
    use tower::util::ServiceExt;

    use super::*;

    // minted ids are 16 uppercase hex chars and differ between calls
    #[test]
    fn minted_shape() {
        let first = next_request_id();
        let second = next_request_id();
        assert_eq!(first.len(), 16);
        assert!(first.bytes().all(|b| b.is_ascii_hexdigit() && !b.is_ascii_lowercase()));
        assert_ne!(first, second);
    }

    fn probe_router() -> Router {
        Router::new()
            .route("/probe", get(|| async { current_request_id().unwrap_or_default() }))
            .layer(axum::middleware::from_fn(request_id))
    }

    // a fresh id is minted, visible to the handler's scope, and echoed back
    #[tokio::test]
    async fn mints_and_echoes() {
        let response = probe_router()
            .oneshot(Request::builder().uri("/probe").body(Body::empty()).unwrap())
            .await
            .unwrap();
        let header = response
            .headers()
            .get(REQUEST_ID_HEADER)
            .and_then(|value| value.to_str().ok())
            .expect("response carries an id")
            .to_string();
        let body = axum::body::to_bytes(response.into_body(), 1024).await.unwrap();
        assert_eq!(header.as_bytes(), &body[..], "handler scope saw the same id");
        assert_eq!(header.len(), 16);
    }

    // a sane proxy-supplied id is honored end to end; junk is replaced
    #[tokio::test]
    async fn honors_inbound() {
        let response = probe_router()
            .oneshot(
                Request::builder()
                    .uri("/probe")
                    .header(REQUEST_ID_HEADER, "proxy-abc-123")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let header = response.headers().get(REQUEST_ID_HEADER).unwrap();
        assert_eq!(header, "proxy-abc-123");

        let response = probe_router()
            .oneshot(
                Request::builder()
                    .uri("/probe")
                    .header(REQUEST_ID_HEADER, "bad id with spaces")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let header = response.headers().get(REQUEST_ID_HEADER).unwrap();
        assert_ne!(header, "bad id with spaces");
    }
}
