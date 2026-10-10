//! Request deadlines that run from the last body frame
//!
//! A slice push over a slow or shared uplink can take minutes and keep moving
//! the whole time. As with nginx's `client_body_timeout`, the limit runs between
//! body frames, and the handler gets the full limit after the last one.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use http_body_util::BodyExt;
use tokio::time::Instant;

/// When the request's body last moved, in ms since the request came in
struct Progress {
    origin: Instant,
    last_ms: AtomicU64,
}

impl Progress {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            origin: Instant::now(),
            last_ms: AtomicU64::new(0),
        })
    }

    fn touch(&self) {
        let now = self.origin.elapsed().as_millis() as u64;
        self.last_ms.store(now, Ordering::Relaxed);
    }

    /// Time since the request came in or its body last moved
    fn quiet(&self) -> Duration {
        let now = self.origin.elapsed().as_millis() as u64;
        Duration::from_millis(now.saturating_sub(self.last_ms.load(Ordering::Relaxed)))
    }
}

/// Answer 408 once a request goes `limit` with no body frame and no response
pub async fn quiet_deadline(State(limit): State<Duration>, request: Request, next: Next) -> Response {
    let progress = Progress::new();
    let watch = progress.clone();
    let request = request.map(|body| {
        Body::new(body.map_frame(move |frame| {
            watch.touch();
            frame
        }))
    });

    let response = next.run(request);
    tokio::pin!(response);
    loop {
        let quiet = progress.quiet();
        if quiet >= limit {
            return StatusCode::REQUEST_TIMEOUT.into_response();
        }
        tokio::select! {
            biased;
            response = &mut response => return response,
            _ = tokio::time::sleep(limit - quiet) => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use axum::Router;
    use axum::body::Bytes;
    use axum::middleware::from_fn_with_state;
    use axum::routing::put;
    use futures::stream;
    use tower::ServiceExt;

    const LIMIT: Duration = Duration::from_millis(300);

    fn router() -> Router {
        Router::new()
            .route("/fast", put(|body: Bytes| async move { body.len().to_string() }))
            .route(
                "/slow",
                put(|body: Bytes| async move {
                    tokio::time::sleep(LIMIT * 2).await;
                    body.len().to_string()
                }),
            )
            .layer(from_fn_with_state(LIMIT, quiet_deadline))
    }

    /// A body that sends `frames` pieces of 1 KiB, `gap` apart
    fn trickle(frames: usize, gap: Duration) -> Body {
        Body::from_stream(stream::unfold(0, move |sent| async move {
            if sent == frames {
                return None;
            }
            tokio::time::sleep(gap).await;
            Some((Ok::<_, std::io::Error>(Bytes::from(vec![0u8; 1024])), sent + 1))
        }))
    }

    async fn send(path: &str, body: Body) -> (StatusCode, String) {
        let request = Request::put(path).body(body).unwrap();
        let response = router().oneshot(request).await.unwrap();
        let status = response.status();
        let body = response.into_body().collect().await.unwrap().to_bytes();
        (status, String::from_utf8_lossy(&body).into_owned())
    }

    // a body that keeps moving lands however long it takes in all
    #[tokio::test]
    async fn moving_body_outlasts_the_limit() {
        let started = Instant::now();
        let (status, body) = send("/fast", trickle(10, LIMIT / 3)).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, "10240");
        assert!(started.elapsed() > LIMIT * 3, "{:?}", started.elapsed());
    }

    // a body that stalls past the limit times out
    #[tokio::test]
    async fn stalled_body_times_out() {
        let (status, _) = send("/fast", trickle(2, LIMIT * 2)).await;
        assert_eq!(status, StatusCode::REQUEST_TIMEOUT);
    }

    // the handler's own work after the last frame counts against the limit
    #[tokio::test]
    async fn slow_handler_times_out() {
        let (status, _) = send("/slow", Body::from(vec![0u8; 1024])).await;
        assert_eq!(status, StatusCode::REQUEST_TIMEOUT);
    }
}
