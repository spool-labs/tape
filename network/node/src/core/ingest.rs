use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use tokio::sync::watch;

/// Threshold at which a node is considered caught up to the durable ingest
/// edge. The block ingestor dispatch cursor is allowed to lag the finalized
/// tip by up to this many slots and still report `AtTip`.
pub const AT_TIP_THRESHOLD_SLOTS: u64 = 5;

/// If no block fetch attempt or tip read has occurred for this long, the
/// monitor reports `Stalled`. Independent from any retry-loop pacing.
pub const STALL_THRESHOLD: Duration = Duration::from_secs(30);

/// How often the monitor task recomputes `IngestState` from progress.
pub const MONITOR_TICK: Duration = Duration::from_millis(500);

/// Where the block ingestor is, relative to the finalized dispatch edge.
#[derive(Debug, Clone, Copy)]
pub enum IngestState {
    Catching { lag_slots: u64 },
    AtTip,
    Stalled { since: Instant },
}

impl IngestState {
    pub fn is_at_tip(&self) -> bool {
        matches!(self, IngestState::AtTip)
    }

    /// Stable, snake_case label suitable for stats / logs / metrics.
    pub fn label(&self) -> &'static str {
        match self {
            IngestState::Catching { .. } => "catching",
            IngestState::AtTip => "at_tip",
            IngestState::Stalled { .. } => "stalled",
        }
    }
}

/// Atomic progress fields written by the `BlockIngestor`. Read by the
/// monitor task at every tick.
#[derive(Debug)]
pub struct IngestProgress {
    last_attempt_ms: AtomicU64,
    last_dispatched_slot: AtomicU64,
    last_known_tip: AtomicU64,
    started: Instant,
}

impl IngestProgress {
    fn new() -> Self {
        Self {
            last_attempt_ms: AtomicU64::new(u64::MAX),
            last_dispatched_slot: AtomicU64::new(0),
            last_known_tip: AtomicU64::new(u64::MAX),
            started: Instant::now(),
        }
    }

    pub fn record_attempt(&self) {
        self.last_attempt_ms
            .store(self.elapsed_ms(), Ordering::Relaxed);
    }

    pub fn record_tip(&self, tip: u64) {
        self.last_known_tip.store(tip, Ordering::Relaxed);
        self.record_attempt();
    }

    pub fn record_dispatched(&self, slot: u64) {
        self.last_dispatched_slot.store(slot, Ordering::Relaxed);
        self.record_attempt();
    }

    pub fn last_attempt_ms(&self) -> u64 {
        self.last_attempt_ms.load(Ordering::Relaxed)
    }

    pub fn last_dispatched_slot(&self) -> u64 {
        self.last_dispatched_slot.load(Ordering::Relaxed)
    }

    pub fn last_known_tip(&self) -> u64 {
        self.last_known_tip.load(Ordering::Relaxed)
    }

    /// Tip, dispatched slot, and lag, treating the unset tip sentinel as zero.
    pub fn tip_and_lag(&self) -> (u64, u64, u64) {
        let tip = self.last_known_tip();
        let dispatched = self.last_dispatched_slot();
        if tip == u64::MAX {
            (0, dispatched, 0)
        } else {
            (tip, dispatched, tip.saturating_sub(dispatched))
        }
    }

    pub fn now_ms(&self) -> u64 {
        self.elapsed_ms()
    }

    fn elapsed_ms(&self) -> u64 {
        self.started.elapsed().as_millis() as u64
    }
}

/// Couples a `watch::Sender<IngestState>` with the atomic progress fields
/// the monitor task reads. Mirrors `core::state::StateBus`.
#[derive(Debug)]
pub struct IngestBus {
    tx: watch::Sender<IngestState>,
    progress: Arc<IngestProgress>,
}

impl IngestBus {
    pub fn new() -> Self {
        let initial = IngestState::Catching { lag_slots: u64::MAX };
        let (tx, _rx) = watch::channel(initial);

        Self {
            tx,
            progress: Arc::new(IngestProgress::new()),
        }
    }

    pub fn current(&self) -> IngestState {
        *self.tx.borrow()
    }

    pub fn subscribe(&self) -> watch::Receiver<IngestState> {
        self.tx.subscribe()
    }

    pub fn publish(&self, state: IngestState) {
        self.tx.send_replace(state);
    }

    pub fn progress(&self) -> Arc<IngestProgress> {
        self.progress.clone()
    }

    pub fn is_at_tip(&self) -> bool {
        self.current().is_at_tip()
    }

    /// Compute the next `IngestState` from current progress. Pure function
    /// of the atomics — exposed for the monitor task and unit tests.
    pub fn compute(&self) -> IngestState {
        let now = self.progress.now_ms();
        let last_attempt = self.progress.last_attempt_ms();
        let last_tip = self.progress.last_known_tip();
        let last_dispatched = self.progress.last_dispatched_slot();

        if last_attempt == u64::MAX || last_tip == u64::MAX {
            return IngestState::Catching { lag_slots: u64::MAX };
        }

        let stall_threshold_ms = STALL_THRESHOLD.as_millis() as u64;
        if now.saturating_sub(last_attempt) > stall_threshold_ms {
            return IngestState::Stalled { since: Instant::now() };
        }

        let lag_slots = last_tip.saturating_sub(last_dispatched);
        if lag_slots <= AT_TIP_THRESHOLD_SLOTS {
            IngestState::AtTip
        } else {
            IngestState::Catching { lag_slots }
        }
    }
}

impl Default for IngestBus {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fresh_bus_reports_catching_with_max_lag() {
        let bus = IngestBus::new();
        let state = bus.compute();

        assert!(matches!(
            state,
            IngestState::Catching { lag_slots: u64::MAX }
        ));
        assert!(!bus.is_at_tip());
    }

    #[test]
    fn caught_up_progress_reports_at_tip() {
        let bus = IngestBus::new();
        let progress = bus.progress();

        progress.record_tip(1000);
        progress.record_dispatched(998);

        assert!(matches!(bus.compute(), IngestState::AtTip));
    }

    #[test]
    fn lagging_progress_reports_catching() {
        let bus = IngestBus::new();
        let progress = bus.progress();

        progress.record_tip(1000);
        progress.record_dispatched(900);

        match bus.compute() {
            IngestState::Catching { lag_slots } => assert_eq!(lag_slots, 100),
            other => panic!("expected Catching, got {other:?}"),
        }
    }

    #[test]
    fn at_tip_threshold_boundary() {
        let bus = IngestBus::new();
        let progress = bus.progress();

        progress.record_tip(1000);
        progress.record_dispatched(1000 - AT_TIP_THRESHOLD_SLOTS);

        assert!(matches!(bus.compute(), IngestState::AtTip));

        progress.record_dispatched(1000 - AT_TIP_THRESHOLD_SLOTS - 1);

        assert!(matches!(bus.compute(), IngestState::Catching { .. }));
    }
}
