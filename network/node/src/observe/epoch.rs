//! Freezes the per-epoch deltas of the cumulative counters at each epoch
//! boundary, for the dashboard and for Grafana.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};

use tape_metrics::prometheus::proto::MetricFamily;
use tape_metrics::prometheus::IntGauge;
use tape_metrics::MetricsRegistry;
use tape_observe_api::{Bucket, HttpStats, LastEpoch, DECODE_FAILURES, DECODE_SLICES_WASTED, SPOOL_OPS};

use super::board::{family_counter, family_gauge, histogram_snapshot, split_rpc_errors};

/// Highest ingest lag seen since the last epoch boundary, reset when an epoch
/// rolls. Fed by the status collector each time the registry is scraped.
static MAX_LAG: AtomicU64 = AtomicU64::new(0);

/// Record one lag observation into the epoch's running peak.
pub fn sample_lag(lag_slots: u64) {
    MAX_LAG.fetch_max(lag_slots, Ordering::Relaxed);
}

/// Cumulative counter readings sampled at one instant.
#[derive(Clone, Default)]
struct Counters {
    blocks: u64,
    replay_events: u64,
    decoded_objects: u64,
    decoded_bytes: u64,
    decode_failures: u64,
    slices_used: u64,
    slices_wasted: u64,
    spool_bytes_persisted: u64,
    spool_bytes_fetched: u64,
    cache_hits: u64,
    cache_misses: u64,
    repair_escalations: u64,
    requests: u64,
    egress_bytes: u64,
    bytes_uploaded: u64,
    bytes_downloaded: u64,
    tx_total: u64,
    tx_errors: u64,
    rpc_errors: u64,
    store_ops: u64,
    store_bytes_read: u64,
    store_bytes_written: u64,
    http_buckets: Vec<Bucket>,
    http_total: u64,
    decode_buckets: Vec<Bucket>,
    decode_count: u64,
    // Point-in-time assignment, copied through rather than diffed.
    shards_owned: u64,
    synced_groups: u64,
    max_lag_slots: u64,
}

#[allow(deprecated)] // prometheus proto getters are deprecated but stable
fn read_counters(families: &[MetricFamily]) -> Counters {
    let m = tape_metrics::metrics();
    let decode = |result: &str| m.decode_total.with_label_values(&[result]).get();
    let slices = |outcome: &str| m.decode_slices_total.with_label_values(&[outcome]).get();
    let spool = |stage: &str| {
        SPOOL_OPS
            .iter()
            .map(|&op| m.spool_bytes_total.with_label_values(&[op, stage]).get())
            .sum()
    };
    let (http_buckets, http_total) = histogram_snapshot(families, "tape_http_request_duration_seconds");
    let (decode_buckets, decode_count) = histogram_snapshot(families, "tape_gw_decode_duration_seconds");
    let mut rpc_errors = 0;
    let mut tx_errors = 0;
    for family in families.iter().filter(|family| family.get_name() == "rpc_errors_total") {
        let (rpc, tx) = split_rpc_errors(family);
        rpc_errors += rpc;
        tx_errors += tx;
    }

    Counters {
        blocks: m.blocks_processed_total.get(),
        replay_events: m.replay_events_total.get(),
        decoded_objects: decode("ok"),
        decoded_bytes: m.decode_output_bytes_total.get(),
        decode_failures: DECODE_FAILURES.iter().map(|&r| decode(r)).sum(),
        slices_used: slices("used"),
        slices_wasted: DECODE_SLICES_WASTED.iter().map(|&o| slices(o)).sum(),
        spool_bytes_persisted: spool("persisted"),
        spool_bytes_fetched: spool("fetched"),
        cache_hits: m.cache_requests_total.with_label_values(&["hit"]).get(),
        cache_misses: m.cache_requests_total.with_label_values(&["miss"]).get(),
        repair_escalations: m.repair_escalations_total.get(),
        requests: m.requests_total.get(),
        egress_bytes: family_counter(families, "tape_http_response_bytes_total"),
        bytes_uploaded: m.bytes_uploaded.get(),
        bytes_downloaded: m.bytes_downloaded.get(),
        tx_total: family_counter(families, "tape_client_transactions_total"),
        tx_errors,
        rpc_errors,
        store_ops: family_counter(families, "tape_store_operations_total"),
        store_bytes_read: family_counter(families, "tape_store_bytes_read_total"),
        store_bytes_written: family_counter(families, "tape_store_bytes_written_total"),
        http_buckets,
        http_total,
        decode_buckets,
        decode_count,
        shards_owned: family_gauge(families, "tape_node_shards_owned"),
        synced_groups: family_gauge(families, "tape_node_epoch_synced_groups"),
        max_lag_slots: MAX_LAG.load(Ordering::Relaxed),
    }
}

struct Gauges {
    number: IntGauge,
    blocks: IntGauge,
    replay_events: IntGauge,
    decoded_objects: IntGauge,
    decoded_bytes: IntGauge,
    decode_failures: IntGauge,
    slices_used: IntGauge,
    slices_wasted: IntGauge,
    spool_bytes_persisted: IntGauge,
    spool_bytes_fetched: IntGauge,
    cache_hits: IntGauge,
    cache_misses: IntGauge,
    repair_escalations: IntGauge,
    requests: IntGauge,
    egress_bytes: IntGauge,
    bytes_uploaded: IntGauge,
    bytes_downloaded: IntGauge,
    tx_total: IntGauge,
    tx_errors: IntGauge,
    rpc_errors: IntGauge,
    store_ops: IntGauge,
    store_bytes_read: IntGauge,
    store_bytes_written: IntGauge,
    serving_p95_ms: IntGauge,
    decode_p95_ms: IntGauge,
    max_lag_slots: IntGauge,
    shards_owned: IntGauge,
    synced_groups: IntGauge,
}

fn gauge(name: &str, help: &str) -> IntGauge {
    let gauge = IntGauge::new(name, help).expect("last-epoch gauge");
    let _ = MetricsRegistry::init().prometheus_registry().register(Box::new(gauge.clone()));
    gauge
}

impl Gauges {
    fn register() -> Self {
        Self {
            number: gauge("tape_node_last_epoch_number", "Epoch number these last-epoch deltas cover"),
            blocks: gauge("tape_node_last_epoch_blocks", "Blocks processed during the last epoch"),
            replay_events: gauge("tape_node_last_epoch_replay_events", "Replay events during the last epoch"),
            decoded_objects: gauge("tape_node_last_epoch_decoded_objects", "Objects decoded during the last epoch"),
            decoded_bytes: gauge("tape_node_last_epoch_decoded_bytes", "Object bytes produced during the last epoch"),
            decode_failures: gauge("tape_node_last_epoch_decode_failures", "Decode failures during the last epoch"),
            slices_used: gauge("tape_node_last_epoch_slices_used", "Verified slices used during the last epoch"),
            slices_wasted: gauge("tape_node_last_epoch_slices_wasted", "Rejected/failed slices during the last epoch"),
            spool_bytes_persisted: gauge("tape_node_last_epoch_spool_bytes_persisted", "Spool bytes persisted during the last epoch"),
            spool_bytes_fetched: gauge("tape_node_last_epoch_spool_bytes_fetched", "Spool bytes fetched during the last epoch"),
            cache_hits: gauge("tape_node_last_epoch_cache_hits", "Slice cache hits during the last epoch"),
            cache_misses: gauge("tape_node_last_epoch_cache_misses", "Slice cache misses during the last epoch"),
            repair_escalations: gauge("tape_node_last_epoch_repair_escalations", "Repair escalations during the last epoch"),
            requests: gauge("tape_node_last_epoch_requests", "Requests served during the last epoch"),
            egress_bytes: gauge("tape_node_last_epoch_egress_bytes", "Bytes served during the last epoch"),
            bytes_uploaded: gauge("tape_node_last_epoch_bytes_uploaded", "Bytes uploaded during the last epoch"),
            bytes_downloaded: gauge("tape_node_last_epoch_bytes_downloaded", "Bytes downloaded during the last epoch"),
            tx_total: gauge("tape_node_last_epoch_transactions", "Transactions submitted during the last epoch"),
            tx_errors: gauge("tape_node_last_epoch_tx_errors", "Transaction errors during the last epoch"),
            rpc_errors: gauge("tape_node_last_epoch_rpc_errors", "RPC errors during the last epoch"),
            store_ops: gauge("tape_node_last_epoch_store_ops", "Store operations during the last epoch"),
            store_bytes_read: gauge("tape_node_last_epoch_store_bytes_read", "Store bytes read during the last epoch"),
            store_bytes_written: gauge("tape_node_last_epoch_store_bytes_written", "Store bytes written during the last epoch"),
            serving_p95_ms: gauge("tape_node_last_epoch_serving_p95_ms", "Serving latency p95 during the last epoch"),
            decode_p95_ms: gauge("tape_node_last_epoch_decode_p95_ms", "Decode latency p95 during the last epoch"),
            max_lag_slots: gauge("tape_node_last_epoch_max_lag_slots", "Highest ingest lag sampled during the last epoch"),
            shards_owned: gauge("tape_node_last_epoch_shards_owned", "Shards owned at the last epoch close"),
            synced_groups: gauge("tape_node_last_epoch_synced_groups", "Groups synced at the last epoch close"),
        }
    }

    fn publish(&self, number: u64, d: &LastEpoch) {
        self.number.set(number as i64);
        self.blocks.set(d.blocks as i64);
        self.replay_events.set(d.replay_events as i64);
        self.decoded_objects.set(d.decoded_objects as i64);
        self.decoded_bytes.set(d.decoded_bytes as i64);
        self.decode_failures.set(d.decode_failures as i64);
        self.slices_used.set(d.slices_used as i64);
        self.slices_wasted.set(d.slices_wasted as i64);
        self.spool_bytes_persisted.set(d.spool_bytes_persisted as i64);
        self.spool_bytes_fetched.set(d.spool_bytes_fetched as i64);
        self.cache_hits.set(d.cache_hits as i64);
        self.cache_misses.set(d.cache_misses as i64);
        self.repair_escalations.set(d.repair_escalations as i64);
        self.requests.set(d.requests as i64);
        self.egress_bytes.set(d.egress_bytes as i64);
        self.bytes_uploaded.set(d.bytes_uploaded as i64);
        self.bytes_downloaded.set(d.bytes_downloaded as i64);
        self.tx_total.set(d.tx_total as i64);
        self.tx_errors.set(d.tx_errors as i64);
        self.rpc_errors.set(d.rpc_errors as i64);
        self.store_ops.set(d.store_ops as i64);
        self.store_bytes_read.set(d.store_bytes_read as i64);
        self.store_bytes_written.set(d.store_bytes_written as i64);
        self.serving_p95_ms.set(d.serving_p95_ms as i64);
        self.decode_p95_ms.set(d.decode_p95_ms as i64);
        self.max_lag_slots.set(d.max_lag_slots as i64);
        self.shards_owned.set(d.shards_owned as i64);
        self.synced_groups.set(d.synced_groups as i64);
    }
}

struct EpochRoller {
    gauges: Gauges,
    prev: Mutex<Option<Counters>>,
    last: Mutex<LastEpoch>,
    life: Mutex<LastEpoch>,
    session_start: Counters,
}

static ROLLER: OnceLock<EpochRoller> = OnceLock::new();

fn roller() -> &'static EpochRoller {
    ROLLER.get_or_init(|| EpochRoller {
        gauges: Gauges::register(),
        prev: Mutex::new(None),
        last: Mutex::new(LastEpoch::default()),
        life: Mutex::new(LastEpoch::default()),
        session_start: read_counters(&tape_metrics::prometheus::gather()),
    })
}

/// Register the last-epoch gauges at startup so they read 0 before the first
/// boundary instead of appearing only after one epoch elapses.
pub fn init() {
    let _ = roller();
}

/// A windowed latency p95 (ms) from two cumulative histogram snapshots.
fn p95_ms(base_buckets: &[Bucket], base_total: u64, now_buckets: &[Bucket], now_total: u64) -> f64 {
    let delta = HttpStats::bucket_delta(now_buckets, base_buckets);
    HttpStats::quantile(&delta, now_total.saturating_sub(base_total), 0.95) * 1000.0
}

/// The delta of the cumulative counters between two snapshots, tagged with an
/// epoch number. Assignment figures ride along as point-in-time values.
fn delta(number: u64, base: &Counters, now: &Counters) -> LastEpoch {
    LastEpoch {
        number,
        blocks: now.blocks.saturating_sub(base.blocks),
        replay_events: now.replay_events.saturating_sub(base.replay_events),
        decoded_objects: now.decoded_objects.saturating_sub(base.decoded_objects),
        decoded_bytes: now.decoded_bytes.saturating_sub(base.decoded_bytes),
        decode_failures: now.decode_failures.saturating_sub(base.decode_failures),
        slices_used: now.slices_used.saturating_sub(base.slices_used),
        slices_wasted: now.slices_wasted.saturating_sub(base.slices_wasted),
        spool_bytes_persisted: now.spool_bytes_persisted.saturating_sub(base.spool_bytes_persisted),
        spool_bytes_fetched: now.spool_bytes_fetched.saturating_sub(base.spool_bytes_fetched),
        cache_hits: now.cache_hits.saturating_sub(base.cache_hits),
        cache_misses: now.cache_misses.saturating_sub(base.cache_misses),
        repair_escalations: now.repair_escalations.saturating_sub(base.repair_escalations),
        requests: now.requests.saturating_sub(base.requests),
        egress_bytes: now.egress_bytes.saturating_sub(base.egress_bytes),
        bytes_uploaded: now.bytes_uploaded.saturating_sub(base.bytes_uploaded),
        bytes_downloaded: now.bytes_downloaded.saturating_sub(base.bytes_downloaded),
        tx_total: now.tx_total.saturating_sub(base.tx_total),
        tx_errors: now.tx_errors.saturating_sub(base.tx_errors),
        rpc_errors: now.rpc_errors.saturating_sub(base.rpc_errors),
        store_ops: now.store_ops.saturating_sub(base.store_ops),
        store_bytes_read: now.store_bytes_read.saturating_sub(base.store_bytes_read),
        store_bytes_written: now.store_bytes_written.saturating_sub(base.store_bytes_written),
        serving_p95_ms: p95_ms(&base.http_buckets, base.http_total, &now.http_buckets, now.http_total),
        decode_p95_ms: p95_ms(&base.decode_buckets, base.decode_count, &now.decode_buckets, now.decode_count),
        max_lag_slots: now.max_lag_slots,
        shards_owned: now.shards_owned,
        synced_groups: now.synced_groups,
    }
}

/// Fold one epoch's deltas into the running lifetime totals. Percentiles are
/// not summable and stay unset; the running lag peak and the point-in-time
/// assignment carry through, and the caller owns the measured-epoch count.
fn accumulate(total: &mut LastEpoch, d: &LastEpoch) {
    total.blocks += d.blocks;
    total.replay_events += d.replay_events;
    total.decoded_objects += d.decoded_objects;
    total.decoded_bytes += d.decoded_bytes;
    total.decode_failures += d.decode_failures;
    total.slices_used += d.slices_used;
    total.slices_wasted += d.slices_wasted;
    total.spool_bytes_persisted += d.spool_bytes_persisted;
    total.spool_bytes_fetched += d.spool_bytes_fetched;
    total.cache_hits += d.cache_hits;
    total.cache_misses += d.cache_misses;
    total.repair_escalations += d.repair_escalations;
    total.requests += d.requests;
    total.egress_bytes += d.egress_bytes;
    total.bytes_uploaded += d.bytes_uploaded;
    total.bytes_downloaded += d.bytes_downloaded;
    total.tx_total += d.tx_total;
    total.tx_errors += d.tx_errors;
    total.rpc_errors += d.rpc_errors;
    total.store_ops += d.store_ops;
    total.store_bytes_read += d.store_bytes_read;
    total.store_bytes_written += d.store_bytes_written;
    total.max_lag_slots = total.max_lag_slots.max(d.max_lag_slots);
    total.shards_owned = d.shards_owned;
    total.synced_groups = d.synced_groups;
}

/// Capture the delta over the epoch that just closed. Called once per committee
/// epoch advance, right after the metric counters reflect that epoch's work.
pub fn roll_epoch(number: u64) {
    let roller = roller();
    let now = read_counters(&tape_metrics::prometheus::gather());
    let mut prev = roller.prev.lock().expect("epoch roller prev");
    if let Some(base) = prev.as_ref() {
        let d = delta(number, base, &now);
        roller.gauges.publish(number, &d);
        let mut life = roller.life.lock().expect("epoch roller life");
        accumulate(&mut life, &d);
        life.number += 1;
        *roller.last.lock().expect("epoch roller last") = d;
    }
    *prev = Some(now);
    MAX_LAG.store(0, Ordering::Relaxed);
}

/// Live deltas for the epoch in progress: counters minus the last boundary, or
/// minus the process-start baseline before the first boundary this run.
pub fn current_epoch_progress(number: u64, families: &[MetricFamily]) -> LastEpoch {
    let roller = roller();
    let now = read_counters(families);
    let prev = roller.prev.lock().expect("epoch roller prev");
    let base = prev.as_ref().unwrap_or(&roller.session_start);
    delta(number, base, &now)
}

/// The persisted lifetime totals, without the in-progress epoch.
pub fn lifetime() -> LastEpoch {
    ROLLER
        .get()
        .map(|roller| roller.life.lock().expect("epoch roller life").clone())
        .unwrap_or_default()
}

/// Lifetime totals with the in-progress epoch folded in, for the board. The
/// live epoch does not count as measured until it completes.
pub fn lifetime_including(current: &LastEpoch) -> LastEpoch {
    let mut total = lifetime();
    accumulate(&mut total, current);
    total
}

/// Seed the lifetime totals from a persisted snapshot at startup.
pub fn set_lifetime(life: LastEpoch) {
    *roller().life.lock().expect("epoch roller life") = life;
}

/// The most recently completed epoch's deltas, for the dashboard.
pub fn last_epoch() -> LastEpoch {
    ROLLER
        .get()
        .map(|roller| roller.last.lock().expect("epoch roller last").clone())
        .unwrap_or_default()
}

/// Seed the last-epoch deltas from a persisted snapshot at startup, so the
/// dashboard shows the previous run's last epoch instead of zeros.
pub fn set_last_epoch(last: LastEpoch) {
    *roller().last.lock().expect("epoch roller last") = last;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lifetime_folds_published_epochs_and_live_progress() {
        init();
        let m = tape_metrics::metrics();

        // First boundary after start only seeds the baseline.
        roll_epoch(10);
        assert_eq!(lifetime().number, 0);

        // A full epoch of work, closed at the next boundary.
        m.blocks_processed_total.inc_by(5);
        m.requests_total.inc_by(7);
        roll_epoch(11);
        assert_eq!(last_epoch().number, 11);
        assert_eq!(last_epoch().blocks, 5);
        assert_eq!(lifetime().number, 1);
        assert_eq!(lifetime().blocks, 5);
        assert_eq!(lifetime().requests, 7);

        // Live progress rides on top of the persisted total without counting
        // as a measured epoch.
        m.blocks_processed_total.inc_by(3);
        let families = tape_metrics::prometheus::gather();
        let combined = lifetime_including(&current_epoch_progress(12, &families));
        assert_eq!(combined.number, 1);
        assert_eq!(combined.blocks, 8);

        // The next boundary folds the second epoch in.
        roll_epoch(12);
        assert_eq!(lifetime().number, 2);
        assert_eq!(lifetime().blocks, 8);
    }
}
