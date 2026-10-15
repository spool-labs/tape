//! Every read call on the reel counts as a store op

#![cfg(feature = "metrics")]

use tape_metrics::MetricsRegistry;
use tape_store_api::{get_metrics, init_metrics, Direction, Store};
use tape_store_reel::{bench_config, ReelStore, TAPE_COLUMNS};
use tempfile::TempDir;

const CF: &str = "tape";

/// The segment size a bench arm opens with
const SEGMENT_BYTES: u64 = 256 * 1024 * 1024;

fn calls(op: &str) -> u64 {
    let metrics = get_metrics().expect("store metrics");
    ["ok", "error"]
        .iter()
        .filter_map(|status| metrics.operations_total.get_metric_with_label_values(&[CF, op, status]).ok())
        .map(|counter| counter.get())
        .sum()
}

// every read call adds one to its own counter
#[test]
fn read_calls() {
    MetricsRegistry::init();
    init_metrics();
    let dir = TempDir::new().expect("tempdir");
    let store = ReelStore::open(dir.path(), bench_config(SEGMENT_BYTES), TAPE_COLUMNS).expect("open");
    store.put(CF, b"tape-0001", b"one").expect("put");

    let ops = [
        "contains",
        "iter",
        "iter_prefix",
        "iter_keys_prefix",
        "iter_from",
        "iter_range",
        "count_prefix",
        "sweep",
        "sweep_prefix",
        "sweep_keys_prefix",
    ];
    let mut before = Vec::with_capacity(ops.len());
    for op in &ops {
        before.push(calls(op));
    }

    store.contains(CF, b"tape-0001").expect("contains");
    let _ = store.iter(CF).expect("iter");
    let _ = store.iter_prefix(CF, b"tape-").expect("iter_prefix");
    store.iter_keys_prefix(CF, b"tape-").expect("iter_keys_prefix");
    let _ = store.iter_from(CF, b"tape-", Direction::Asc).expect("iter_from");
    let _ = store.iter_range(CF, b"tape-", b"tape.").expect("iter_range");
    store.count_prefix(CF, b"tape-").expect("count_prefix");
    store.sweep(CF, None, 8).expect("sweep");
    store.sweep_prefix(CF, b"tape-", None, 8).expect("sweep_prefix");
    store.sweep_keys_prefix(CF, b"tape-", None, 8).expect("sweep_keys_prefix");

    for (op, was) in ops.iter().zip(before) {
        assert_eq!(calls(op), was + 1, "{op}");
    }
}
