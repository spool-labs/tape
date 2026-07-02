//! Bootstrap progress bus. Written by the bootstrap replay phases, read by
//! the HTTP status handlers while the node catches up. Mirrors `core::ingest`.

use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};
use std::time::Instant;

/// Where bootstrap catch-up is, as exposed to operators.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BootstrapPhase {
    Starting,
    SnapshotReplay,
    BlockReplay,
    Ready,
}

impl BootstrapPhase {
    /// Stable, snake_case label suitable for stats / logs / metrics.
    pub fn label(&self) -> &'static str {
        match self {
            BootstrapPhase::Starting => "starting",
            BootstrapPhase::SnapshotReplay => "snapshot_replay",
            BootstrapPhase::BlockReplay => "block_replay",
            BootstrapPhase::Ready => "ready",
        }
    }

    fn from_u8(value: u8) -> Self {
        match value {
            1 => BootstrapPhase::SnapshotReplay,
            2 => BootstrapPhase::BlockReplay,
            3 => BootstrapPhase::Ready,
            _ => BootstrapPhase::Starting,
        }
    }

    fn as_u8(self) -> u8 {
        match self {
            BootstrapPhase::Starting => 0,
            BootstrapPhase::SnapshotReplay => 1,
            BootstrapPhase::BlockReplay => 2,
            BootstrapPhase::Ready => 3,
        }
    }
}

/// Point-in-time view of bootstrap progress with derived rate/ETA.
#[derive(Debug, Clone, Copy)]
pub struct BootstrapSnapshot {
    pub phase: BootstrapPhase,
    pub snapshot_epoch: u64,
    pub start_slot: u64,
    pub current_slot: u64,
    pub target_slot: u64,
    pub skipped_slots: u64,
    pub slots_per_sec: f64,
    pub eta_secs: Option<u64>,
}

impl BootstrapSnapshot {
    pub fn slots_done(&self) -> u64 {
        self.current_slot.saturating_sub(self.start_slot)
    }

    pub fn slots_total(&self) -> u64 {
        self.target_slot.saturating_sub(self.start_slot)
    }

    pub fn percent_done(&self) -> f64 {
        let total = self.slots_total();
        if total == 0 {
            return 0.0;
        }
        self.slots_done() as f64 * 100.0 / total as f64
    }
}

/// Atomic progress fields written by the bootstrap replay phases.
#[derive(Debug)]
pub struct BootstrapBus {
    phase: AtomicU8,
    snapshot_epoch: AtomicU64,
    start_slot: AtomicU64,
    current_slot: AtomicU64,
    target_slot: AtomicU64,
    skipped_slots: AtomicU64,
    phase_started_ms: AtomicU64,
    started: Instant,
}

impl BootstrapBus {
    pub fn new() -> Self {
        Self {
            phase: AtomicU8::new(BootstrapPhase::Starting.as_u8()),
            snapshot_epoch: AtomicU64::new(0),
            start_slot: AtomicU64::new(0),
            current_slot: AtomicU64::new(0),
            target_slot: AtomicU64::new(0),
            skipped_slots: AtomicU64::new(0),
            phase_started_ms: AtomicU64::new(0),
            started: Instant::now(),
        }
    }

    pub fn begin_snapshot_replay(&self, epoch: u64) {
        self.snapshot_epoch.store(epoch, Ordering::Relaxed);
        self.set_phase(BootstrapPhase::SnapshotReplay);
    }

    pub fn begin_block_replay(&self, start_slot: u64, target_slot: u64) {
        self.start_slot.store(start_slot, Ordering::Relaxed);
        self.current_slot.store(start_slot, Ordering::Relaxed);
        self.target_slot.store(target_slot, Ordering::Relaxed);
        self.set_phase(BootstrapPhase::BlockReplay);
    }

    pub fn record_slot(&self, slot: u64) {
        self.current_slot.store(slot, Ordering::Relaxed);
    }

    pub fn record_skipped(&self) {
        self.skipped_slots.fetch_add(1, Ordering::Relaxed);
    }

    pub fn mark_ready(&self) {
        self.set_phase(BootstrapPhase::Ready);
    }

    pub fn is_ready(&self) -> bool {
        self.phase() == BootstrapPhase::Ready
    }

    pub fn phase(&self) -> BootstrapPhase {
        BootstrapPhase::from_u8(self.phase.load(Ordering::Relaxed))
    }

    /// Rate is computed over the current phase, so a long snapshot phase does
    /// not dilute the block-replay slots/sec.
    pub fn snapshot(&self) -> BootstrapSnapshot {
        let phase = self.phase();
        let start_slot = self.start_slot.load(Ordering::Relaxed);
        let current_slot = self.current_slot.load(Ordering::Relaxed);
        let target_slot = self.target_slot.load(Ordering::Relaxed);

        let phase_elapsed_ms = self
            .elapsed_ms()
            .saturating_sub(self.phase_started_ms.load(Ordering::Relaxed));
        let slots_done = current_slot.saturating_sub(start_slot);
        let slots_per_sec = if phase_elapsed_ms == 0 {
            0.0
        } else {
            slots_done as f64 * 1000.0 / phase_elapsed_ms as f64
        };

        let remaining = target_slot.saturating_sub(current_slot);
        let eta_secs = if phase == BootstrapPhase::BlockReplay && slots_per_sec > 0.0 {
            Some((remaining as f64 / slots_per_sec) as u64)
        } else {
            None
        };

        BootstrapSnapshot {
            phase,
            snapshot_epoch: self.snapshot_epoch.load(Ordering::Relaxed),
            start_slot,
            current_slot,
            target_slot,
            skipped_slots: self.skipped_slots.load(Ordering::Relaxed),
            slots_per_sec,
            eta_secs,
        }
    }

    fn set_phase(&self, phase: BootstrapPhase) {
        self.phase_started_ms.store(self.elapsed_ms(), Ordering::Relaxed);
        self.phase.store(phase.as_u8(), Ordering::Relaxed);
    }

    fn elapsed_ms(&self) -> u64 {
        self.started.elapsed().as_millis() as u64
    }
}

impl Default for BootstrapBus {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fresh_bus_is_starting() {
        let bus = BootstrapBus::new();
        assert_eq!(bus.phase(), BootstrapPhase::Starting);
        assert!(!bus.is_ready());
    }

    #[test]
    fn block_replay_tracks_progress() {
        let bus = BootstrapBus::new();
        bus.begin_block_replay(100, 200);
        bus.record_slot(150);
        bus.record_skipped();

        let snapshot = bus.snapshot();
        assert_eq!(snapshot.phase, BootstrapPhase::BlockReplay);
        assert_eq!(snapshot.slots_done(), 50);
        assert_eq!(snapshot.slots_total(), 100);
        assert_eq!(snapshot.percent_done(), 50.0);
        assert_eq!(snapshot.skipped_slots, 1);
    }

    #[test]
    fn ready_after_mark() {
        let bus = BootstrapBus::new();
        bus.begin_snapshot_replay(5);
        assert_eq!(bus.phase(), BootstrapPhase::SnapshotReplay);
        assert_eq!(bus.snapshot().snapshot_epoch, 5);

        bus.mark_ready();
        assert!(bus.is_ready());
        assert!(bus.snapshot().eta_secs.is_none());
    }
}
