//! How many slice pushes one client runs at once
//!
//! A node drops a slice upload that runs past its request timeout, 60 s by
//! default, and every push on one uplink shares that uplink. The gate starts at
//! one chunk's pushes, halves when a push runs long or times out, and adds one
//! each time a push lands quickly, so a push on a slow link still lands well
//! inside the node's limit.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use tape_core::erasure::MAX_GROUP_SIZE;
use tape_protocol::ApiError;
use tokio::sync::Notify;
use tokio::time::Instant;
use tracing::info;

/// Pushes the gate allows at most, whatever the link
const MAX_PUSHES: usize = 256;

/// A push that runs this long halves the gate, half a node's default request timeout
const SLOW_PUSH: Duration = Duration::from_secs(30);

/// A push that lands this fast lets the gate grow by one
const FAST_PUSH: Duration = Duration::from_secs(10);

/// What a push came to, as far as the link goes
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PushPace {
    /// The node took the slice after this long
    Landed(Duration),
    /// The push ran out of time at one end or the other
    TimedOut,
    /// The push failed for a reason that says nothing about the link
    Other,
}

impl PushPace {
    /// How a push that ran for `elapsed` and ended in `result` reads
    pub fn of<T>(result: &Result<T, ApiError>, elapsed: Duration) -> Self {
        match result {
            Ok(_) => Self::Landed(elapsed),
            Err(ApiError::Timeout) | Err(ApiError::ServerError { status: 408, .. }) => Self::TimedOut,
            Err(_) => Self::Other,
        }
    }
}

/// The client's slice pushes in flight, and how many the link carries in time
pub struct PushGate {
    origin: Instant,
    limit: AtomicUsize,
    in_flight: AtomicUsize,
    shrunk_ms: AtomicU64,
    freed: Notify,
}

impl Default for PushGate {
    fn default() -> Self {
        Self::new()
    }
}

impl PushGate {
    pub fn new() -> Self {
        Self {
            origin: Instant::now(),
            limit: AtomicUsize::new(MAX_GROUP_SIZE),
            in_flight: AtomicUsize::new(0),
            shrunk_ms: AtomicU64::new(0),
            freed: Notify::new(),
        }
    }

    /// Pushes allowed at once right now
    pub fn limit(&self) -> usize {
        self.limit.load(Ordering::Acquire)
    }

    /// Wait until one more push fits
    pub async fn enter(self: &Arc<Self>) -> Pass {
        loop {
            let freed = self.freed.notified();
            let in_flight = self.in_flight.load(Ordering::Acquire);
            if in_flight < self.limit()
                && self
                    .in_flight
                    .compare_exchange(in_flight, in_flight + 1, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
            {
                return Pass { gate: self.clone() };
            }
            freed.await;
        }
    }

    fn record(&self, pace: PushPace) {
        match pace {
            PushPace::Landed(elapsed) if elapsed <= FAST_PUSH => self.grow(),
            PushPace::Landed(elapsed) if elapsed >= SLOW_PUSH => self.shrink(),
            PushPace::TimedOut => self.shrink(),
            PushPace::Landed(_) | PushPace::Other => {}
        }
    }

    fn grow(&self) {
        let grown = self
            .limit
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |limit| {
                (limit < MAX_PUSHES).then_some(limit + 1)
            });
        if grown.is_ok() {
            self.freed.notify_one();
        }
    }

    fn shrink(&self) {
        let now = self.origin.elapsed().as_millis() as u64;
        let last = self.shrunk_ms.load(Ordering::Acquire);
        if last != 0 && now.saturating_sub(last) < SLOW_PUSH.as_millis() as u64 {
            return;
        }
        if self
            .shrunk_ms
            .compare_exchange(last, now.max(1), Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }
        let was = self.limit.load(Ordering::Acquire);
        let limit = (was / 2).max(1);
        self.limit.store(limit, Ordering::Release);
        info!(pushes = limit, was, "slice pushes ran long, fewer at once");
    }
}

/// One push's place in the gate, given back when it drops
pub struct Pass {
    gate: Arc<PushGate>,
}

impl Pass {
    /// Tell the gate how the push went, and leave
    pub fn finish(self, pace: PushPace) {
        self.gate.record(pace);
    }
}

impl Drop for Pass {
    fn drop(&mut self) {
        self.gate.in_flight.fetch_sub(1, Ordering::AcqRel);
        self.gate.freed.notify_one();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // pushes that run long halve the gate once per slow window
    #[tokio::test(start_paused = true)]
    async fn slow_pushes_halve_once_a_window() {
        let gate = Arc::new(PushGate::new());
        tokio::time::sleep(Duration::from_millis(5)).await;
        for _ in 0..5 {
            gate.enter().await.finish(PushPace::TimedOut);
        }
        assert_eq!(gate.limit(), MAX_GROUP_SIZE / 2);

        tokio::time::sleep(SLOW_PUSH).await;
        gate.enter().await.finish(PushPace::Landed(SLOW_PUSH * 2));
        assert_eq!(gate.limit(), MAX_GROUP_SIZE / 4);
    }

    // pushes that land quickly widen the gate one at a time
    #[tokio::test]
    async fn fast_pushes_widen() {
        let gate = Arc::new(PushGate::new());
        for _ in 0..3 {
            gate.enter().await.finish(PushPace::Landed(Duration::from_secs(1)));
        }
        assert_eq!(gate.limit(), MAX_GROUP_SIZE + 3);
    }

    // a full gate holds the next push until one leaves
    #[tokio::test]
    async fn a_full_gate_waits() {
        let gate = Arc::new(PushGate::new());
        let mut passes = Vec::new();
        for _ in 0..MAX_GROUP_SIZE {
            passes.push(gate.enter().await);
        }
        let waiting = {
            let gate = gate.clone();
            tokio::spawn(async move { gate.enter().await })
        };
        tokio::task::yield_now().await;
        assert!(!waiting.is_finished());

        passes.pop();
        let pass = tokio::time::timeout(Duration::from_secs(1), waiting).await;
        assert!(matches!(pass, Ok(Ok(_))));
    }
}
