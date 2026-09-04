//! The single application metric set, registered once into the global registry.

use std::sync::OnceLock;

use prometheus::{
    register_histogram_vec_with_registry, register_int_counter_vec_with_registry,
    register_int_counter_with_registry, HistogramVec, IntCounter, IntCounterVec, Registry,
};

use crate::MetricsRegistry;

const HTTP_DURATION_BUCKETS: &[f64] = &[
    0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
];
const DECODE_BUCKETS: &[f64] = &[0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0];

/// All counters and histograms recorded by the node and gateway. Gauges that
/// reflect live external state are emitted by pull collectors instead.
pub struct Metrics {
    pub http_request_duration: HistogramVec,
    pub http_response_bytes_total: IntCounterVec,

    pub blocks_processed_total: IntCounter,
    pub replay_events_total: IntCounter,
    pub epoch_transitions_total: IntCounter,
    pub repair_escalations_total: IntCounter,
    pub spool_bytes_total: IntCounterVec,

    pub decode_duration: HistogramVec,
    pub decode_output_bytes_total: IntCounter,
    pub decode_total: IntCounterVec,
    pub decode_slices_total: IntCounterVec,

    pub cache_requests_total: IntCounterVec,
    pub cache_evicted_total: IntCounter,

    // Stats endpoint only — intentionally NOT registered, so the HTTP histogram
    // stays the single Prometheus source for request and byte rates.
    pub requests_total: IntCounter,
    pub bytes_uploaded: IntCounter,
    pub bytes_downloaded: IntCounter,
}

impl Metrics {
    fn new(registry: &Registry) -> Self {
        Self {
            http_request_duration: register_histogram_vec_with_registry!(
                "tape_http_request_duration_seconds",
                "HTTP request duration in seconds",
                &["route", "method", "status_class"],
                HTTP_DURATION_BUCKETS.to_vec(),
                registry
            )
            .expect("register tape_http_request_duration_seconds"),
            http_response_bytes_total: register_int_counter_vec_with_registry!(
                "tape_http_response_bytes_total",
                "HTTP response body bytes served",
                &["route"],
                registry
            )
            .expect("register tape_http_response_bytes_total"),

            blocks_processed_total: register_int_counter_with_registry!(
                "tape_node_blocks_processed_total",
                "Blocks parsed and processed",
                registry
            )
            .expect("register tape_node_blocks_processed_total"),
            replay_events_total: register_int_counter_with_registry!(
                "tape_node_replay_events_total",
                "Replay events applied",
                registry
            )
            .expect("register tape_node_replay_events_total"),
            epoch_transitions_total: register_int_counter_with_registry!(
                "tape_node_epoch_transitions_total",
                "Epoch transitions observed",
                registry
            )
            .expect("register tape_node_epoch_transitions_total"),
            repair_escalations_total: register_int_counter_with_registry!(
                "tape_node_repair_escalations_total",
                "Spool repair escalations",
                registry
            )
            .expect("register tape_node_repair_escalations_total"),
            spool_bytes_total: register_int_counter_vec_with_registry!(
                "tape_node_spool_bytes_total",
                "Spool pipeline bytes by op and stage",
                &["op", "stage"],
                registry
            )
            .expect("register tape_node_spool_bytes_total"),

            decode_duration: register_histogram_vec_with_registry!(
                "tape_gw_decode_duration_seconds",
                "Object decode duration in seconds",
                &["kind"],
                DECODE_BUCKETS.to_vec(),
                registry
            )
            .expect("register tape_gw_decode_duration_seconds"),
            decode_output_bytes_total: register_int_counter_with_registry!(
                "tape_gw_decode_output_bytes_total",
                "Decoded object bytes produced",
                registry
            )
            .expect("register tape_gw_decode_output_bytes_total"),
            decode_total: register_int_counter_vec_with_registry!(
                "tape_gw_decode_total",
                "Object decode outcomes",
                &["result"],
                registry
            )
            .expect("register tape_gw_decode_total"),
            decode_slices_total: register_int_counter_vec_with_registry!(
                "tape_gw_decode_slices_total",
                "Erasure slice fetch outcomes",
                &["outcome"],
                registry
            )
            .expect("register tape_gw_decode_slices_total"),

            cache_requests_total: register_int_counter_vec_with_registry!(
                "tape_gw_cache_requests_total",
                "Slice cache lookups by result",
                &["result"],
                registry
            )
            .expect("register tape_gw_cache_requests_total"),
            cache_evicted_total: register_int_counter_with_registry!(
                "tape_gw_cache_evicted_total",
                "Slice cache entries evicted",
                registry
            )
            .expect("register tape_gw_cache_evicted_total"),

            requests_total: IntCounter::new("tape_node_requests_total", "Requests handled")
                .expect("tape_node_requests_total"),
            bytes_uploaded: IntCounter::new("tape_node_bytes_uploaded_total", "Bytes uploaded")
                .expect("tape_node_bytes_uploaded_total"),
            bytes_downloaded: IntCounter::new("tape_node_bytes_downloaded_total", "Bytes downloaded")
                .expect("tape_node_bytes_downloaded_total"),
        }
    }
}

static METRICS: OnceLock<Metrics> = OnceLock::new();

/// Access the global metric set, registering it on first use.
pub fn metrics() -> &'static Metrics {
    METRICS.get_or_init(|| Metrics::new(MetricsRegistry::init().prometheus_registry()))
}

/// Force construction and registration of the metric set at startup.
pub fn init_app_metrics() {
    let _ = metrics();
}
