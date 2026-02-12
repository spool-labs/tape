//! RAII metric guards for tracking "currently active" counts.
//!
//! All "active count" metrics MUST use `GaugeGuard`. No manual
//! `gauge.inc()` / `gauge.dec()` pairs.

use tape_metrics::prometheus::IntGauge;

/// RAII guard that increments a gauge on creation and decrements on drop.
///
/// ```ignore
/// let _guard = GaugeGuard::acquire(&metrics.active_syncs);
/// // gauge is incremented
/// do_work().await;
/// // guard dropped here — gauge decremented automatically
/// ```
#[must_use]
pub struct GaugeGuard(IntGauge);

impl GaugeGuard {
    pub fn acquire(gauge: &IntGauge) -> Self {
        gauge.inc();
        Self(gauge.clone())
    }
}

impl Drop for GaugeGuard {
    fn drop(&mut self) {
        self.0.dec();
    }
}
