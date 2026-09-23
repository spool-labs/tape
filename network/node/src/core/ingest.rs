use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use tokio::sync::watch;

/// Threshold at which a node is considered caught up to the durable ingest
/// edge. The block ingestor dispatch cursor is allowed to lag the confirmed
/// tip by up to this many slots and still report `AtTip`.
pub const AT_TIP_THRESHOLD_SLOTS: u64 = 5;

/// How long the monitor waits before reporting `Stalled`
///
/// Counted from the last fetch attempt or tip read and, behind the tip,
/// from the last dispatch. Independent from any retry-loop pacing.
pub const STALL_THRESHOLD: Duration = Duration::from_secs(30);

/// How often the monitor task recomputes `IngestState` from progress.
pub const MONITOR_TICK: Duration = Duration::from_millis(500);

/// Where the block ingestor is, relative to the confirmed dispatch edge.
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
    last_dispatch_ms: AtomicU64,
    last_dispatched_slot: AtomicU64,
    last_known_tip: AtomicU64,
    last_fetch_slot: AtomicU64,
    chain_time: AtomicI64,
    /// Slot and chain timestamp the measured slot rate is counted from.
    rate_base_slot: AtomicU64,
    rate_base_time: AtomicI64,
    queue_len: AtomicU64,
    started: Instant,
}

/// Held until a block carrying a timestamp is dispatched. No real block time is
/// ever this, so "not yet known" stays distinct from "earlier than everything".
const CHAIN_TIME_UNSET: i64 = i64::MIN;

/// Slots the measured rate averages over before its baseline rolls forward.
const RATE_WINDOW_SLOTS: u64 = 512;

/// Slots the baseline must span before a rate is reported.
const RATE_MIN_SLOTS: u64 = 64;

impl IngestProgress {
    fn new(started: Instant) -> Self {
        Self {
            last_attempt_ms: AtomicU64::new(u64::MAX),
            last_dispatch_ms: AtomicU64::new(0),
            last_dispatched_slot: AtomicU64::new(0),
            last_known_tip: AtomicU64::new(u64::MAX),
            last_fetch_slot: AtomicU64::new(0),
            chain_time: AtomicI64::new(CHAIN_TIME_UNSET),
            rate_base_slot: AtomicU64::new(0),
            rate_base_time: AtomicI64::new(CHAIN_TIME_UNSET),
            queue_len: AtomicU64::new(0),
            started,
        }
    }

    /// Seed the cursors at the bootstrap handoff and start the dispatch clock,
    /// so a long bootstrap does not count against the stall window
    pub fn start_live(&self, last_dispatched_slot: u64) {
        self.last_dispatched_slot.store(last_dispatched_slot, Ordering::Relaxed);
        self.last_fetch_slot.store(last_dispatched_slot, Ordering::Relaxed);
        self.last_dispatch_ms.store(self.elapsed_ms(), Ordering::Relaxed);
    }

    pub fn record_attempt(&self) {
        self.last_attempt_ms
            .store(self.elapsed_ms(), Ordering::Relaxed);
    }

    /// Record a block fetch attempt at `slot`
    pub fn record_fetch(&self, slot: u64) {
        self.last_fetch_slot.store(slot, Ordering::Relaxed);
        self.record_attempt();
    }

    pub fn record_queue_len(&self, len: u64) {
        self.queue_len.store(len, Ordering::Relaxed);
    }

    pub fn record_tip(&self, tip: u64) {
        self.last_known_tip.store(tip, Ordering::Relaxed);
        self.record_attempt();
    }

    pub fn record_dispatched(&self, slot: u64) {
        self.last_dispatched_slot.store(slot, Ordering::Relaxed);
        self.last_dispatch_ms
            .store(self.elapsed_ms(), Ordering::Relaxed);
        self.record_attempt();
    }

    /// Record a dispatched block's timestamp, which is the chain's own clock.
    /// Rolls the slot-rate baseline once it falls too far behind.
    pub fn record_chain_time(&self, unix_seconds: i64) {
        self.chain_time.store(unix_seconds, Ordering::Relaxed);

        let slot = self.last_dispatched_slot();
        let base_slot = self.rate_base_slot.load(Ordering::Relaxed);
        let unset = self.rate_base_time.load(Ordering::Relaxed) == CHAIN_TIME_UNSET;
        if unset || slot < base_slot || slot.saturating_sub(base_slot) > RATE_WINDOW_SLOTS {
            self.rate_base_slot.store(slot, Ordering::Relaxed);
            self.rate_base_time.store(unix_seconds, Ordering::Relaxed);
        }
    }

    /// Timestamp of the latest dispatched block, or None before one arrives.
    pub fn chain_time(&self) -> Option<i64> {
        let time = self.chain_time.load(Ordering::Relaxed);
        (time != CHAIN_TIME_UNSET).then_some(time)
    }

    /// Measured slot time from the chain's own clock. None until the baseline
    /// spans enough slots to outrun a block timestamp's one-second granularity.
    pub fn slot_ms(&self) -> Option<u64> {
        let base_slot = self.rate_base_slot.load(Ordering::Relaxed);
        let base_time = self.rate_base_time.load(Ordering::Relaxed);
        let now_time = self.chain_time.load(Ordering::Relaxed);
        if base_time == CHAIN_TIME_UNSET || now_time == CHAIN_TIME_UNSET {
            return None;
        }
        let slots = self.last_dispatched_slot().checked_sub(base_slot)?;
        let seconds = now_time.checked_sub(base_time)?;
        if slots < RATE_MIN_SLOTS || seconds <= 0 {
            return None;
        }
        Some((seconds as u64 * 1_000 / slots).max(1))
    }

    pub fn last_attempt_ms(&self) -> u64 {
        self.last_attempt_ms.load(Ordering::Relaxed)
    }

    /// Progress clock at the last dispatch; zero before the first one
    pub fn last_dispatch_ms(&self) -> u64 {
        self.last_dispatch_ms.load(Ordering::Relaxed)
    }

    pub fn last_dispatched_slot(&self) -> u64 {
        self.last_dispatched_slot.load(Ordering::Relaxed)
    }

    pub fn last_known_tip(&self) -> u64 {
        self.last_known_tip.load(Ordering::Relaxed)
    }

    /// The tip once one has been recorded
    pub fn known_tip(&self) -> Option<u64> {
        match self.last_known_tip() {
            u64::MAX => None,
            tip => Some(tip),
        }
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

    pub fn last_fetch_slot(&self) -> u64 {
        self.last_fetch_slot.load(Ordering::Relaxed)
    }

    pub fn queue_len(&self) -> u64 {
        self.queue_len.load(Ordering::Relaxed)
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
        Self::started_at(Instant::now())
    }

    /// A bus whose progress clock began at `started`, so tests can age it
    pub fn started_at(started: Instant) -> Self {
        let initial = IngestState::Catching { lag_slots: u64::MAX };
        let (tx, _rx) = watch::channel(initial);

        Self {
            tx,
            progress: Arc::new(IngestProgress::new(started)),
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
        let lag_slots = last_tip.saturating_sub(last_dispatched);
        let is_behind = lag_slots > AT_TIP_THRESHOLD_SLOTS;
        let is_attempt_stale = now.saturating_sub(last_attempt) > stall_threshold_ms;
        // Behind the tip the fetch cursor alone keeps the attempt clock fresh,
        // so the dispatch clock is checked on its own
        let is_dispatch_stale =
            now.saturating_sub(self.progress.last_dispatch_ms()) > stall_threshold_ms;
        if is_attempt_stale || (is_behind && is_dispatch_stale) {
            return IngestState::Stalled { since: Instant::now() };
        }
        if !is_behind {
            return IngestState::AtTip;
        }
        IngestState::Catching { lag_slots }
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

    fn aged_bus() -> IngestBus {
        IngestBus::started_at(Instant::now() - STALL_THRESHOLD * 2)
    }

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

    // a stale attempt clock stalls even at the tip
    #[test]
    fn stale_attempt() {
        let bus = aged_bus();
        let progress = bus.progress();

        progress.record_tip(1000);
        progress.record_dispatched(998);
        progress.last_attempt_ms.store(0, Ordering::Relaxed);

        assert!(matches!(bus.compute(), IngestState::Stalled { .. }));
    }

    // behind the tip, fetching keeps the attempt clock fresh and only a dispatch clears the stall
    #[test]
    fn stale_dispatch() {
        let bus = aged_bus();
        let progress = bus.progress();

        progress.record_tip(1000);
        progress.record_fetch(999);

        assert!(matches!(bus.compute(), IngestState::Stalled { .. }));

        progress.record_dispatched(900);

        assert!(matches!(bus.compute(), IngestState::Catching { lag_slots: 100 }));
    }

    // at the tip a quiet dispatch clock still reads AtTip
    #[test]
    fn quiet_at_tip() {
        let bus = aged_bus();
        let progress = bus.progress();

        progress.record_tip(1000);
        progress.last_dispatched_slot.store(1000, Ordering::Relaxed);

        assert!(matches!(bus.compute(), IngestState::AtTip));
    }

    // the handoff seeds the cursors and starts the dispatch clock fresh
    #[test]
    fn live_start() {
        let bus = aged_bus();
        let progress = bus.progress();

        progress.start_live(900);
        progress.record_tip(1000);
        progress.record_fetch(901);

        assert_eq!(progress.last_dispatched_slot(), 900);
        assert!(matches!(bus.compute(), IngestState::Catching { lag_slots: 100 }));
    }
}
