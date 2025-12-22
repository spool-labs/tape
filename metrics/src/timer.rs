//! Operation timer for measuring latency.

use std::time::Instant;

/// A simple timer for measuring operation duration.
///
/// This is a lightweight wrapper around `std::time::Instant` that provides
/// a convenient interface for recording latency metrics.
///
/// # Example
///
/// ```ignore
/// use tape_metrics::OperationTimer;
///
/// let timer = OperationTimer::new();
///
/// // ... perform operation ...
///
/// let elapsed = timer.elapsed_secs();
/// metrics.request_duration.observe(elapsed);
/// ```
#[derive(Debug, Clone)]
pub struct OperationTimer {
    start: Instant,
}

impl OperationTimer {
    /// Create a new timer, starting immediately.
    #[inline]
    pub fn new() -> Self {
        Self {
            start: Instant::now(),
        }
    }

    /// Get the elapsed time in seconds as a floating-point value.
    ///
    /// This is the format expected by Prometheus histograms.
    #[inline]
    pub fn elapsed_secs(&self) -> f64 {
        self.start.elapsed().as_secs_f64()
    }

    /// Get the elapsed time in milliseconds.
    #[inline]
    pub fn elapsed_millis(&self) -> u128 {
        self.start.elapsed().as_millis()
    }

    /// Get the elapsed time in microseconds.
    #[inline]
    pub fn elapsed_micros(&self) -> u128 {
        self.start.elapsed().as_micros()
    }

    /// Get the underlying `Instant` when the timer started.
    #[inline]
    pub fn start_time(&self) -> Instant {
        self.start
    }
}

impl Default for OperationTimer {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_timer_measures_time() {
        let timer = OperationTimer::new();
        thread::sleep(Duration::from_millis(10));
        let elapsed = timer.elapsed_secs();

        // Should be at least 10ms
        assert!(elapsed >= 0.01);
        // But not too long (allow some slack)
        assert!(elapsed < 0.1);
    }

    #[test]
    fn test_timer_elapsed_millis() {
        let timer = OperationTimer::new();
        thread::sleep(Duration::from_millis(5));
        let elapsed = timer.elapsed_millis();

        assert!(elapsed >= 5);
    }

    #[test]
    fn test_timer_can_be_read_multiple_times() {
        let timer = OperationTimer::new();
        thread::sleep(Duration::from_millis(5));

        let e1 = timer.elapsed_secs();
        thread::sleep(Duration::from_millis(5));
        let e2 = timer.elapsed_secs();

        // Second reading should be larger
        assert!(e2 > e1);
    }
}
