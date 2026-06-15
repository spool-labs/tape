//! Client-side SDK timing metrics.
//!
//! The SDK records coarse phase timings for read and write workflows through
//! this module. The default recorder is [`Noop`]; callers that want local
//! inspection can install [`InMemory`], structured logs can use [`Tracing`],
//! and Prometheus is available behind the SDK `metrics` feature.

use std::collections::HashMap;
use std::sync::RwLock;
use std::time::{Duration, Instant};

/// Records SDK timing events.
pub trait Metrics: Send + Sync {
    fn record(&self, event: Event);
}

/// Top-level SDK workflow being measured.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Operation {
    Write,
    WriteTrack,
    WriteRaw,
    WriteBlob,
    Upload,
    Certify,
    WriteStream,
    ReadTrack,
    ReadStream,
    Verify,
}

impl Operation {
    pub fn as_str(self) -> &'static str {
        match self {
            Operation::Write => "write",
            Operation::WriteTrack => "write_track",
            Operation::WriteRaw => "write_raw",
            Operation::WriteBlob => "write_blob",
            Operation::Upload => "upload",
            Operation::Certify => "certify",
            Operation::WriteStream => "write_stream",
            Operation::ReadTrack => "read_track",
            Operation::ReadStream => "read_stream",
            Operation::Verify => "verify",
        }
    }
}

/// Phase within an SDK workflow.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Phase {
    Total,
    Preflight,
    Reserve,
    Encode,
    Register,
    Visibility,
    Locate,
    Store,
    CertifyCollect,
    CertifyProof,
    CertifySubmit,
    CertifyVisible,
    Metadata,
    Bootstrap,
    ResolvePeers,
    TrackMetadata,
    BlobData,
    Download,
    Decode,
    WriteSink,
}

impl Phase {
    pub fn as_str(self) -> &'static str {
        match self {
            Phase::Total => "total",
            Phase::Preflight => "preflight",
            Phase::Reserve => "reserve",
            Phase::Encode => "encode",
            Phase::Register => "register",
            Phase::Visibility => "visibility",
            Phase::Locate => "locate",
            Phase::Store => "store",
            Phase::CertifyCollect => "certify_collect",
            Phase::CertifyProof => "certify_proof",
            Phase::CertifySubmit => "certify_submit",
            Phase::CertifyVisible => "certify_visible",
            Phase::Metadata => "metadata",
            Phase::Bootstrap => "bootstrap",
            Phase::ResolvePeers => "resolve_peers",
            Phase::TrackMetadata => "track_metadata",
            Phase::BlobData => "track_data",
            Phase::Download => "download",
            Phase::Decode => "decode",
            Phase::WriteSink => "write_sink",
        }
    }
}

/// Bounded outcome classification for a phase.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Outcome {
    Ok,
    Error,
    Timeout,
    Cancelled,
    Retry,
}

impl Outcome {
    pub fn as_str(self) -> &'static str {
        match self {
            Outcome::Ok => "ok",
            Outcome::Error => "error",
            Outcome::Timeout => "timeout",
            Outcome::Cancelled => "cancelled",
            Outcome::Retry => "retry",
        }
    }

    pub fn from_result<T, E>(result: &Result<T, E>) -> Self {
        match result {
            Ok(_) => Outcome::Ok,
            Err(_) => Outcome::Error,
        }
    }
}

/// One timing observation.
#[derive(Debug, Clone)]
pub struct Event {
    pub operation: Operation,
    pub phase: Phase,
    pub outcome: Outcome,
    pub elapsed: Duration,
    pub bytes: Option<u64>,
    pub chunks: Option<u64>,
    pub attempts: Option<u64>,
}

impl Event {
    pub fn new(operation: Operation, phase: Phase, outcome: Outcome, elapsed: Duration) -> Self {
        Self {
            operation,
            phase,
            outcome,
            elapsed,
            bytes: None,
            chunks: None,
            attempts: None,
        }
    }

    pub fn bytes(mut self, bytes: u64) -> Self {
        self.bytes = Some(bytes);
        self
    }

    pub fn chunks(mut self, chunks: u64) -> Self {
        self.chunks = Some(chunks);
        self
    }

    pub fn attempts(mut self, attempts: u64) -> Self {
        self.attempts = Some(attempts);
        self
    }
}

/// Convenience timer used by SDK internals.
pub(crate) struct Timer<'a> {
    metrics: &'a dyn Metrics,
    operation: Operation,
    phase: Phase,
    started: Instant,
    bytes: Option<u64>,
    chunks: Option<u64>,
    attempts: Option<u64>,
}

impl<'a> Timer<'a> {
    pub(crate) fn start(metrics: &'a dyn Metrics, operation: Operation, phase: Phase) -> Self {
        Self {
            metrics,
            operation,
            phase,
            started: Instant::now(),
            bytes: None,
            chunks: None,
            attempts: None,
        }
    }

    pub(crate) fn bytes(mut self, bytes: u64) -> Self {
        self.bytes = Some(bytes);
        self
    }

    pub(crate) fn chunks(mut self, chunks: u64) -> Self {
        self.chunks = Some(chunks);
        self
    }

    pub(crate) fn finish(self, outcome: Outcome) {
        let mut event = Event::new(self.operation, self.phase, outcome, self.started.elapsed());
        event.bytes = self.bytes;
        event.chunks = self.chunks;
        event.attempts = self.attempts;
        self.metrics.record(event);
    }

    pub(crate) fn finish_result<T, E>(self, result: &Result<T, E>) {
        self.finish(Outcome::from_result(result));
    }
}

/// Recorder that drops all events.
pub struct Noop;

impl Metrics for Noop {
    #[inline]
    fn record(&self, _event: Event) {}
}

/// Key used by [`InMemory`] summaries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Key {
    pub operation: Operation,
    pub phase: Phase,
    pub outcome: Outcome,
}

impl Key {
    pub fn new(operation: Operation, phase: Phase, outcome: Outcome) -> Self {
        Self {
            operation,
            phase,
            outcome,
        }
    }
}

/// Aggregated in-process timing summary.
#[derive(Debug, Clone, Copy, Default)]
pub struct Summary {
    pub count: u64,
    pub total: Duration,
    pub min: Duration,
    pub max: Duration,
    pub bytes: u64,
    pub chunks: u64,
    pub attempts: u64,
}

impl Summary {
    fn record(&mut self, event: &Event) {
        if self.count == 0 || event.elapsed < self.min {
            self.min = event.elapsed;
        }
        if event.elapsed > self.max {
            self.max = event.elapsed;
        }

        self.count += 1;
        self.total += event.elapsed;
        self.bytes += event.bytes.unwrap_or(0);
        self.chunks += event.chunks.unwrap_or(0);
        self.attempts += event.attempts.unwrap_or(0);
    }

    pub fn average(self) -> Option<Duration> {
        if self.count == 0 {
            None
        } else {
            Some(Duration::from_secs_f64(
                self.total.as_secs_f64() / self.count as f64,
            ))
        }
    }
}

/// In-process recorder for tests, CLIs, and short-lived clients.
pub struct InMemory {
    summaries: RwLock<HashMap<Key, Summary>>,
}

impl InMemory {
    pub fn new() -> Self {
        Self {
            summaries: RwLock::new(HashMap::new()),
        }
    }

    pub fn snapshot(&self) -> HashMap<Key, Summary> {
        self.summaries
            .read()
            .expect("metrics summaries lock poisoned")
            .clone()
    }

    pub fn clear(&self) {
        self.summaries
            .write()
            .expect("metrics summaries lock poisoned")
            .clear();
    }
}

impl Default for InMemory {
    fn default() -> Self {
        Self::new()
    }
}

impl Metrics for InMemory {
    fn record(&self, event: Event) {
        let key = Key::new(event.operation, event.phase, event.outcome);
        self.summaries
            .write()
            .expect("metrics summaries lock poisoned")
            .entry(key)
            .or_default()
            .record(&event);
    }
}

/// Recorder that emits structured tracing events.
pub struct Tracing;

impl Metrics for Tracing {
    fn record(&self, event: Event) {
        tracing::debug!(
            operation = event.operation.as_str(),
            phase = event.phase.as_str(),
            outcome = event.outcome.as_str(),
            elapsed_ms = event.elapsed.as_millis() as u64,
            bytes = event.bytes,
            chunks = event.chunks,
            attempts = event.attempts,
            "sdk metric"
        );
    }
}

#[cfg(feature = "metrics")]
const PHASE_DURATION_BUCKETS: &[f64] = &[
    0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0,
];

/// Prometheus adapter for SDK metrics.
#[cfg(feature = "metrics")]
pub struct Prometheus {
    phase_duration: tape_metrics::HistogramVec,
    phases_total: tape_metrics::IntCounterVec,
    bytes_total: tape_metrics::IntCounterVec,
    chunks_total: tape_metrics::IntCounterVec,
    attempts_total: tape_metrics::IntCounterVec,
}

#[cfg(feature = "metrics")]
impl Prometheus {
    pub fn new(registry: &tape_metrics::Registry) -> Self {
        let phase_duration = tape_metrics::HistogramVec::new(
            tape_metrics::prometheus::histogram_opts!(
                "sdk_phase_duration_seconds",
                "Duration of SDK workflow phases",
                PHASE_DURATION_BUCKETS.to_vec()
            ),
            &["operation", "phase", "outcome"],
        )
        .unwrap();
        registry.register(Box::new(phase_duration.clone())).unwrap();

        let phases_total = tape_metrics::IntCounterVec::new(
            tape_metrics::prometheus::opts!(
                "sdk_phases_total",
                "Total SDK workflow phase observations"
            ),
            &["operation", "phase", "outcome"],
        )
        .unwrap();
        registry.register(Box::new(phases_total.clone())).unwrap();

        let bytes_total = tape_metrics::IntCounterVec::new(
            tape_metrics::prometheus::opts!(
                "sdk_phase_bytes_total",
                "Total bytes attributed to SDK workflow phases"
            ),
            &["operation", "phase", "outcome"],
        )
        .unwrap();
        registry.register(Box::new(bytes_total.clone())).unwrap();

        let chunks_total = tape_metrics::IntCounterVec::new(
            tape_metrics::prometheus::opts!(
                "sdk_phase_chunks_total",
                "Total chunks attributed to SDK workflow phases"
            ),
            &["operation", "phase", "outcome"],
        )
        .unwrap();
        registry.register(Box::new(chunks_total.clone())).unwrap();

        let attempts_total = tape_metrics::IntCounterVec::new(
            tape_metrics::prometheus::opts!(
                "sdk_phase_attempts_total",
                "Total attempts attributed to SDK workflow phases"
            ),
            &["operation", "phase", "outcome"],
        )
        .unwrap();
        registry.register(Box::new(attempts_total.clone())).unwrap();

        Self {
            phase_duration,
            phases_total,
            bytes_total,
            chunks_total,
            attempts_total,
        }
    }

    pub fn global() -> Self {
        let registry = tape_metrics::MetricsRegistry::init();
        Self::new(registry.prometheus_registry())
    }
}

#[cfg(feature = "metrics")]
impl Metrics for Prometheus {
    fn record(&self, event: Event) {
        let labels = &[
            event.operation.as_str(),
            event.phase.as_str(),
            event.outcome.as_str(),
        ];

        self.phase_duration
            .with_label_values(labels)
            .observe(event.elapsed.as_secs_f64());
        self.phases_total.with_label_values(labels).inc();

        if let Some(bytes) = event.bytes {
            self.bytes_total.with_label_values(labels).inc_by(bytes);
        }
        if let Some(chunks) = event.chunks {
            self.chunks_total.with_label_values(labels).inc_by(chunks);
        }
        if let Some(attempts) = event.attempts {
            self.attempts_total.with_label_values(labels).inc_by(attempts);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn in_memory_records_summaries() {
        let metrics = InMemory::new();
        metrics.record(
            Event::new(
                Operation::WriteStream,
                Phase::Store,
                Outcome::Ok,
                Duration::from_millis(25),
            )
            .bytes(1024)
            .chunks(2),
        );
        metrics.record(
            Event::new(
                Operation::WriteStream,
                Phase::Store,
                Outcome::Ok,
                Duration::from_millis(75),
            )
            .bytes(2048)
            .chunks(3),
        );

        let snapshot = metrics.snapshot();
        let summary = snapshot
            .get(&Key::new(
                Operation::WriteStream,
                Phase::Store,
                Outcome::Ok,
            ))
            .expect("summary present");

        assert_eq!(summary.count, 2);
        assert_eq!(summary.total, Duration::from_millis(100));
        assert_eq!(summary.min, Duration::from_millis(25));
        assert_eq!(summary.max, Duration::from_millis(75));
        assert_eq!(summary.bytes, 3072);
        assert_eq!(summary.chunks, 5);
        assert_eq!(summary.average(), Some(Duration::from_millis(50)));
    }
}
