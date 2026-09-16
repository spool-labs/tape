//! Thin gateway-side facade over the global metric set.

#[cfg_attr(not(feature = "metrics"), allow(unused_variables))]
pub fn observe_decode(kind: &str, elapsed_secs: f64, output_bytes: usize) {
    #[cfg(feature = "metrics")]
    {
        let m = tape_metrics::metrics();
        m.decode_duration.with_label_values(&[kind]).observe(elapsed_secs);
        m.decode_output_bytes_total.inc_by(output_bytes as u64);
    }
}

/// Count produced object bytes without recording a decode duration. Used by the
/// inline path, which serves stored bytes directly and has no decode latency.
#[cfg_attr(not(feature = "metrics"), allow(unused_variables))]
pub fn add_output_bytes(output_bytes: usize) {
    #[cfg(feature = "metrics")]
    tape_metrics::metrics().decode_output_bytes_total.inc_by(output_bytes as u64);
}

#[cfg_attr(not(feature = "metrics"), allow(unused_variables))]
pub fn inc_decode_result(result: &str) {
    #[cfg(feature = "metrics")]
    tape_metrics::metrics().decode_total.with_label_values(&[result]).inc();
}

#[cfg_attr(not(feature = "metrics"), allow(unused_variables))]
pub fn inc_decode_slices(outcome: &str, count: u64) {
    #[cfg(feature = "metrics")]
    tape_metrics::metrics().decode_slices_total.with_label_values(&[outcome]).inc_by(count);
}

#[cfg_attr(not(feature = "metrics"), allow(unused_variables))]
pub fn inc_cache(result: &str) {
    #[cfg(feature = "metrics")]
    tape_metrics::metrics().cache_requests_total.with_label_values(&[result]).inc();
}

#[cfg_attr(not(feature = "metrics"), allow(unused_variables))]
pub fn inc_cache_evicted(count: u64) {
    #[cfg(feature = "metrics")]
    tape_metrics::metrics().cache_evicted_total.inc_by(count);
}

/// Count one queued-write outcome: `enqueued`, `landed` or `failed`.
#[cfg_attr(not(feature = "metrics"), allow(unused_variables))]
pub fn inc_pending_write(outcome: &str) {
    #[cfg(feature = "metrics")]
    tape_metrics::metrics().pending_writes_total.with_label_values(&[outcome]).inc();
}

/// Report how many writes are queued and not yet applied on chain.
#[cfg_attr(not(feature = "metrics"), allow(unused_variables))]
pub fn set_pending_writes_queued(count: u64) {
    #[cfg(feature = "metrics")]
    tape_metrics::metrics().pending_writes_queued.set(count as i64);
}

/// Report the object bytes queued and not yet applied on chain.
#[cfg_attr(not(feature = "metrics"), allow(unused_variables))]
pub fn set_pending_writes_queued_bytes(bytes: u64) {
    #[cfg(feature = "metrics")]
    tape_metrics::metrics().pending_writes_queued_bytes.set(bytes as i64);
}

/// Report the delegate signer's SOL balance in lamports.
#[cfg_attr(not(feature = "metrics"), allow(unused_variables))]
pub fn set_delegate_lamports(lamports: u64) {
    #[cfg(feature = "metrics")]
    tape_metrics::metrics().delegate_lamports.set(lamports as i64);
}
