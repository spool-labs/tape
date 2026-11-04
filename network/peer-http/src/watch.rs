//! Watches on a slice body, so a push gives up only once it goes quiet and a caller hears the bytes cross

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use futures::stream;
use tape_core::types::StorageUnits;
use tape_protocol::ApiError;
use tokio::time::Instant;

/// Bytes handed to the connection per poll, small enough that a push on a slow, shared link still moves within its idle limit
const PIECE_BYTES: usize = 64 * 1024;

tokio::task_local! {
    static SLICE_WATCH: SliceWatch;
}

/// Hears the bytes of a slice body that have crossed so far in this attempt, then the body's size
pub type SliceWatch = Arc<dyn Fn(StorageUnits, StorageUnits) + Send + Sync>;

/// Run a slice call with a watch in scope, so the client tells it how much of the body has crossed
pub async fn with_slice_watch<Call: Future>(watch: Option<SliceWatch>, call: Call) -> Call::Output {
    match watch {
        Some(watch) => SLICE_WATCH.scope(watch, call).await,
        None => call.await,
    }
}

/// Run a slice call that fails with a timeout once its body moves no byte for `idle`
///
/// The limit counts from the call's start, so a holder that never answers
/// gives up after `idle` too.
pub async fn with_idle_limit<T, Call>(idle: Duration, call: Call) -> Result<T, ApiError>
where
    Call: Future<Output = Result<T, ApiError>>,
{
    let watch = Watch::new();
    let heard: SliceWatch = {
        let watch = watch.clone();
        Arc::new(move |_, _| watch.touch())
    };
    tokio::select! {
        answer = with_slice_watch(Some(heard), call) => answer,
        () = silence(&watch, idle) => Err(ApiError::Timeout),
    }
}

/// The watch in scope, if any
pub(crate) fn current_slice_watch() -> Option<SliceWatch> {
    SLICE_WATCH.try_with(Arc::clone).ok()
}

/// When a push last moved a byte
pub(crate) struct Watch {
    origin: Instant,
    last_ms: AtomicU64,
}

impl Watch {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            origin: Instant::now(),
            last_ms: AtomicU64::new(0),
        })
    }

    pub(crate) fn touch(&self) {
        let now = self.origin.elapsed().as_millis() as u64;
        self.last_ms.store(now, Ordering::Relaxed);
    }

    /// Time since the push last moved a byte
    fn quiet(&self) -> Duration {
        let now = self.origin.elapsed().as_millis() as u64;
        Duration::from_millis(now.saturating_sub(self.last_ms.load(Ordering::Relaxed)))
    }
}

/// Resolve once the push has moved nothing for `limit`
pub(crate) async fn silence(watch: &Watch, limit: Duration) {
    loop {
        let quiet = watch.quiet();
        if quiet >= limit {
            return;
        }
        tokio::time::sleep(limit - quiet).await;
    }
}

/// A body that hands `body` to the connection in pieces, touching the watch as each piece leaves
///
/// The connection asks for the next piece only once it has room for it, so the
/// watch moves at the pace of the link and stops when the link stops. A caller's
/// `slice_watch` hears the bytes taken so far at the same pace.
pub(crate) fn paced_body(body: Bytes, watch: Arc<Watch>, slice_watch: Option<SliceWatch>) -> reqwest::Body {
    let pieces = stream::unfold((body, 0usize), move |(body, offset)| {
        let watch = watch.clone();
        let slice_watch = slice_watch.clone();
        async move {
            if offset >= body.len() {
                return None;
            }
            let end = (offset + PIECE_BYTES).min(body.len());
            let piece = body.slice(offset..end);
            watch.touch();
            if let Some(slice_watch) = slice_watch {
                slice_watch(StorageUnits::from_bytes(end as u64), StorageUnits::from_bytes(body.len() as u64));
            }
            Some((Ok::<Bytes, std::io::Error>(piece), (body, end)))
        }
    });
    reqwest::Body::wrap_stream(pieces)
}

/// Read a response's body piece by piece, touching `watch` as each piece arrives
pub(crate) async fn touched_body(mut resp: reqwest::Response, watch: &Watch) -> Result<Bytes, reqwest::Error> {
    let size = resp.content_length().unwrap_or_default();
    let mut body = BytesMut::with_capacity(size as usize);
    while let Some(piece) = resp.chunk().await? {
        watch.touch();
        body.extend_from_slice(&piece);
    }
    Ok(body.freeze())
}

/// Read a response's body piece by piece, telling `watch` how much has arrived after each
pub(crate) async fn watched_body(mut resp: reqwest::Response, watch: &SliceWatch) -> Result<Bytes, reqwest::Error> {
    let size = resp.content_length().unwrap_or_default();
    let mut body = BytesMut::with_capacity(size as usize);
    while let Some(piece) = resp.chunk().await? {
        body.extend_from_slice(&piece);
        watch(StorageUnits::from_bytes(body.len() as u64), StorageUnits::from_bytes(size));
    }
    Ok(body.freeze())
}

#[cfg(test)]
mod tests {
    use super::*;

    // a watch that was touched reads quiet from the touch on
    #[tokio::test]
    async fn silence_waits_for_quiet() {
        let watch = Watch::new();
        tokio::time::sleep(Duration::from_millis(100)).await;
        watch.touch();
        let started = Instant::now();
        silence(&watch, Duration::from_millis(300)).await;
        assert!(started.elapsed() >= Duration::from_millis(290), "{:?}", started.elapsed());
    }

    // a call that never answers gives up once the idle limit passes
    #[tokio::test]
    async fn idle_call_times_out() {
        let idle = Duration::from_millis(200);
        let started = Instant::now();

        let answer = with_idle_limit(idle, std::future::pending::<Result<(), ApiError>>()).await;

        assert!(matches!(answer, Err(ApiError::Timeout)), "{answer:?}");
        assert!(started.elapsed() >= idle, "{:?}", started.elapsed());
    }

    // a call whose body keeps moving runs past the idle limit and finishes
    #[tokio::test]
    async fn moving_call_finishes() {
        let idle = Duration::from_millis(200);
        let started = Instant::now();
        let call = async {
            for piece in 1..=5u64 {
                tokio::time::sleep(idle / 2).await;
                if let Some(watch) = current_slice_watch() {
                    watch(StorageUnits::from_bytes(piece), StorageUnits::from_bytes(5));
                }
            }
            Ok::<_, ApiError>(())
        };

        let answer = with_idle_limit(idle, call).await;

        assert!(answer.is_ok(), "{answer:?}");
        assert!(started.elapsed() >= idle * 2, "{:?}", started.elapsed());
    }
}
