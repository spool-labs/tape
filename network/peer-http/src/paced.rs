//! Slice push bodies that report their progress, so a push gives up only once it goes quiet

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bytes::Bytes;
use futures::stream;
use tokio::time::Instant;

/// Bytes handed to the connection per poll
const PIECE_BYTES: usize = 256 * 1024;

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

/// A body that hands `body` to the connection in pieces, touching the watch and counting each piece
///
/// The connection asks for the next piece only once it has room for it, so the
/// watch moves at the pace of the link and stops when the link stops.
pub(crate) fn paced_body(body: Bytes, watch: Arc<Watch>, pushed: Arc<AtomicU64>) -> reqwest::Body {
    let pieces = stream::unfold((body, 0usize), move |(body, offset)| {
        let watch = watch.clone();
        let pushed = pushed.clone();
        async move {
            if offset >= body.len() {
                return None;
            }
            let end = (offset + PIECE_BYTES).min(body.len());
            let piece = body.slice(offset..end);
            watch.touch();
            pushed.fetch_add(piece.len() as u64, Ordering::Relaxed);
            Some((Ok::<Bytes, std::io::Error>(piece), (body, end)))
        }
    });
    reqwest::Body::wrap_stream(pieces)
}

#[cfg(test)]
mod tests {
    use super::*;

    // a watch that was touched reads quiet from the touch on
    #[tokio::test(start_paused = true)]
    async fn silence_waits_for_quiet() {
        let watch = Watch::new();
        tokio::time::sleep(Duration::from_secs(5)).await;
        watch.touch();
        let started = tokio::time::Instant::now();
        silence(&watch, Duration::from_secs(30)).await;
        assert!(started.elapsed() >= Duration::from_secs(29));
    }
}
