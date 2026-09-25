//! Slice fetches ask only as many owners as a decode needs, plus a hedge

use std::future::Future;
use std::time::Duration;

use futures::stream::{FuturesUnordered, StreamExt};
use tokio::time::{Instant, sleep};

/// A decode starts this many fetches beyond the count it needs
pub const SLICE_HEDGES: usize = 1;

/// How long a decode waits with nothing landing before it asks one more owner
pub const SLICE_HEDGE_DELAY: Duration = Duration::from_millis(500);

/// Fetch from candidates in order until enough succeed, starting one more on each failure or stall
pub async fn fetch_hedged<Candidate, Item, Failure, Fetch, FetchFuture>(
    candidates: &[Candidate],
    needed: usize,
    fetch: Fetch,
) -> (Vec<Item>, Vec<Failure>)
where
    Candidate: Copy,
    Fetch: Fn(Candidate) -> FetchFuture,
    FetchFuture: Future<Output = Result<Item, Failure>>,
{
    let mut items = Vec::with_capacity(needed);
    let mut failures = Vec::new();
    let mut pending = FuturesUnordered::new();
    let mut launched = 0usize;

    while launched < candidates.len() && launched < needed + SLICE_HEDGES {
        pending.push(fetch(candidates[launched]));
        launched += 1;
    }

    let hedge = sleep(SLICE_HEDGE_DELAY);
    tokio::pin!(hedge);

    while items.len() < needed {
        tokio::select! {
            biased;

            // Safe: next() is cancellation-safe, an unfinished fetch stays in the set
            Some(result) = pending.next() => {
                match result {
                    Ok(item) => items.push(item),
                    Err(failure) => {
                        failures.push(failure);
                        if launched < candidates.len() {
                            pending.push(fetch(candidates[launched]));
                            launched += 1;
                        }
                    }
                }
                hedge.as_mut().reset(Instant::now() + SLICE_HEDGE_DELAY);
            }

            // Safe: the sleep is pinned, so a lost race resumes it
            _ = &mut hedge, if launched < candidates.len() => {
                pending.push(fetch(candidates[launched]));
                launched += 1;
                hedge.as_mut().reset(Instant::now() + SLICE_HEDGE_DELAY);
            }

            else => break,
        }
    }

    (items, failures)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    const GROUP: usize = 20;
    const NEEDED: usize = 7;

    fn candidates() -> Vec<usize> {
        (0..GROUP).collect()
    }

    // healthy owners get exactly the needed fetches plus the hedge
    #[tokio::test]
    async fn healthy_group() {
        let started = AtomicUsize::new(0);

        let (items, failures) = fetch_hedged(&candidates(), NEEDED, |candidate| {
            started.fetch_add(1, Ordering::SeqCst);
            async move { Ok::<_, ()>(candidate) }
        })
        .await;

        assert_eq!(items.len(), NEEDED);
        assert!(failures.is_empty());
        assert_eq!(started.load(Ordering::SeqCst), NEEDED + SLICE_HEDGES);
    }

    // each failure starts the next owner until enough succeed
    #[tokio::test]
    async fn failures_widen() {
        let started = AtomicUsize::new(0);

        let (items, failures) = fetch_hedged(&candidates(), NEEDED, |candidate| {
            started.fetch_add(1, Ordering::SeqCst);
            async move {
                match candidate < 4 {
                    true => Err(candidate),
                    false => Ok(candidate),
                }
            }
        })
        .await;

        assert_eq!(items.len(), NEEDED);
        assert_eq!(failures, vec![0, 1, 2, 3]);
        assert!(started.load(Ordering::SeqCst) <= NEEDED + SLICE_HEDGES + failures.len());
    }

    // owners that never answer are hedged around after the delay
    #[tokio::test]
    async fn stalled_owners() {
        let (items, failures) = fetch_hedged(&candidates(), NEEDED, |candidate| async move {
            if candidate < 3 {
                std::future::pending::<()>().await;
            }
            Ok::<_, ()>(candidate)
        })
        .await;

        assert_eq!(items.len(), NEEDED);
        assert!(failures.is_empty());
        assert!(items.iter().all(|candidate| *candidate >= 3));
    }

    // too few good owners returns what landed along with every failure
    #[tokio::test]
    async fn short_group() {
        let (items, failures) = fetch_hedged(&candidates(), NEEDED, |candidate| async move {
            match candidate < 15 {
                true => Err(candidate),
                false => Ok(candidate),
            }
        })
        .await;

        assert_eq!(items.len(), GROUP - 15);
        assert_eq!(failures.len(), 15);
    }
}
