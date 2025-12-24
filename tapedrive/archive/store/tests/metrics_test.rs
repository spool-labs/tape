//! Integration tests for Prometheus metrics
//!
//! These tests verify that metrics are properly recorded for all Store operations.

#![cfg(feature = "metrics")]

use store::{get_metrics, init_metrics, MemoryStore, Store, WriteBatch};
use tape_metrics::MetricsRegistry;

fn setup_metrics() {
    // Initialize the global registry and store metrics
    MetricsRegistry::init();
    init_metrics();
}

#[test]
fn test_metrics_initialization() {
    setup_metrics();

    let store_metrics = get_metrics().expect("Store metrics should be initialized");

    // Verify we can access the metrics
    store_metrics
        .operations_total
        .with_label_values(&["test_cf", "get", "success"])
        .inc();
}

#[test]
fn test_get_metrics_recorded() {
    setup_metrics();
    let store = MemoryStore::new();

    // Put a value
    store.put("test", b"key1", b"value1").unwrap();

    // Get existing key
    let _ = store.get("test", b"key1").unwrap();

    // Get non-existent key
    let _ = store.get("test", b"nonexistent").unwrap();

    // Verify metrics are recorded by checking the counter directly
    let store_metrics = get_metrics().expect("Store metrics should be initialized");
    let counter = store_metrics
        .operations_total
        .get_metric_with_label_values(&["test", "get", "success"])
        .expect("Should have get success counter");
    assert!(counter.get() >= 1, "Should have at least 1 get operation");
}

#[test]
fn test_put_metrics_recorded() {
    setup_metrics();
    let store = MemoryStore::new();

    // Perform multiple puts
    store.put("test_put", b"key1", b"value1").unwrap();
    store.put("test_put", b"key2", b"value2").unwrap();
    store.put("test_put", b"key3", b"value3").unwrap();

    // Verify metrics are recorded
    let store_metrics = get_metrics().expect("Store metrics should be initialized");
    let counter = store_metrics
        .operations_total
        .get_metric_with_label_values(&["test_put", "put", "success"])
        .expect("Should have put success counter");
    assert!(counter.get() >= 3, "Should have at least 3 put operations");

    // Check bytes written
    let bytes_counter = store_metrics
        .bytes_written_total
        .get_metric_with_label_values(&["test_put"])
        .expect("Should have bytes written counter");
    assert!(bytes_counter.get() >= 30, "Should have at least 30 bytes written");
}

#[test]
fn test_delete_metrics_recorded() {
    setup_metrics();
    let store = MemoryStore::new();

    store.put("test_del", b"key1", b"value1").unwrap();
    store.delete("test_del", b"key1").unwrap();

    let store_metrics = get_metrics().expect("Store metrics should be initialized");
    let counter = store_metrics
        .operations_total
        .get_metric_with_label_values(&["test_del", "delete", "success"])
        .expect("Should have delete success counter");
    assert!(counter.get() >= 1, "Should have at least 1 delete operation");
}

#[test]
fn test_contains_metrics_recorded() {
    setup_metrics();
    let store = MemoryStore::new();

    store.put("test_contains", b"key1", b"value1").unwrap();

    // Contains existing key
    let _ = store.contains("test_contains", b"key1").unwrap();

    // Contains non-existent key
    let _ = store.contains("test_contains", b"nonexistent").unwrap();

    let store_metrics = get_metrics().expect("Store metrics should be initialized");
    let counter = store_metrics
        .operations_total
        .get_metric_with_label_values(&["test_contains", "contains", "success"])
        .expect("Should have contains success counter");
    assert!(counter.get() >= 2, "Should have at least 2 contains operations");
}

#[test]
fn test_write_batch_metrics_recorded() {
    setup_metrics();
    let store = MemoryStore::new();

    let mut batch = WriteBatch::new();
    batch.put("test_batch", b"key1", b"value1");
    batch.put("test_batch", b"key2", b"value2");
    batch.delete("test_batch", b"key3");

    store.write_batch(batch).unwrap();

    let store_metrics = get_metrics().expect("Store metrics should be initialized");
    let counter = store_metrics
        .operations_total
        .get_metric_with_label_values(&["test_batch", "write_batch", "success"])
        .expect("Should have write_batch success counter");
    assert!(counter.get() >= 1, "Should have at least 1 write_batch operation");
}

#[test]
fn test_iter_metrics_recorded() {
    setup_metrics();
    let store = MemoryStore::new();

    store.put("test_iter", b"key1", b"value1").unwrap();
    store.put("test_iter", b"key2", b"value2").unwrap();

    // Test different iterator types
    let _ = store.iter("test_iter").unwrap().collect::<Vec<_>>();
    let _ = store
        .iter_prefix("test_iter", b"key")
        .unwrap()
        .collect::<Vec<_>>();

    let store_metrics = get_metrics().expect("Store metrics should be initialized");

    // Check iter counter
    let iter_counter = store_metrics
        .operations_total
        .get_metric_with_label_values(&["test_iter", "iter", "success"])
        .expect("Should have iter success counter");
    assert!(iter_counter.get() >= 1, "Should have at least 1 iter operation");

    // Check iter_prefix counter
    let prefix_counter = store_metrics
        .operations_total
        .get_metric_with_label_values(&["test_iter", "iter_prefix", "success"])
        .expect("Should have iter_prefix success counter");
    assert!(prefix_counter.get() >= 1, "Should have at least 1 iter_prefix operation");
}

#[test]
fn test_all_expected_metrics_exist() {
    setup_metrics();
    let store_metrics = get_metrics().expect("Store metrics should be initialized");

    // Observe at least one metric from each type to verify they exist
    store_metrics.get_duration.with_label_values(&["test", "true"]).observe(0.001);
    store_metrics.put_duration.with_label_values(&["test"]).observe(0.001);
    store_metrics.delete_duration.with_label_values(&["test"]).observe(0.001);
    store_metrics.contains_duration.with_label_values(&["test", "true"]).observe(0.001);
    store_metrics.batch_duration.with_label_values(&["test"]).observe(0.001);
    store_metrics.iter_duration.with_label_values(&["test", "full"]).observe(0.001);
    store_metrics.operations_total.with_label_values(&["test", "get", "success"]).inc();
    store_metrics.bytes_read_total.with_label_values(&["test"]).inc();
    store_metrics.bytes_written_total.with_label_values(&["test"]).inc();
    store_metrics.key_bytes.with_label_values(&["test", "get"]).observe(10.0);
    store_metrics.value_bytes.with_label_values(&["test", "get"]).observe(100.0);
    store_metrics.batch_items.with_label_values(&["test"]).observe(5.0);
    store_metrics.errors_total.with_label_values(&["test", "get", "database"]).inc();

    // If we got here without panicking, all metrics exist and are properly registered
}

#[test]
fn test_column_family_isolation() {
    setup_metrics();
    let store = MemoryStore::new();

    // Operations on different column families
    store.put("users", b"key1", b"value1").unwrap();
    store.put("posts", b"key2", b"value2").unwrap();

    let store_metrics = get_metrics().expect("Store metrics should be initialized");

    // Each CF should have its own counter
    let users_counter = store_metrics
        .operations_total
        .get_metric_with_label_values(&["users", "put", "success"])
        .expect("Should have users put counter");

    let posts_counter = store_metrics
        .operations_total
        .get_metric_with_label_values(&["posts", "put", "success"])
        .expect("Should have posts put counter");

    assert!(users_counter.get() >= 1, "Should have users put operation");
    assert!(posts_counter.get() >= 1, "Should have posts put operation");
}

#[test]
fn test_bytes_tracking() {
    setup_metrics();
    let store = MemoryStore::new();

    let key = b"test_key"; // 8 bytes
    let value = b"test_value_data"; // 15 bytes
    store.put("bytes_test", key, value).unwrap();

    let store_metrics = get_metrics().expect("Store metrics should be initialized");

    // Check bytes written
    let bytes_counter = store_metrics
        .bytes_written_total
        .get_metric_with_label_values(&["bytes_test"])
        .expect("Should have bytes written counter");

    // Should have written at least key + value bytes
    assert!(bytes_counter.get() >= 23, "Should have at least 23 bytes written (8 + 15)");
}

#[test]
fn test_operation_timer() {
    use store::OperationTimer;

    let timer = OperationTimer::new();
    std::thread::sleep(std::time::Duration::from_millis(10));
    let elapsed = timer.elapsed_secs();
    assert!(elapsed >= 0.01, "Should have at least 10ms elapsed");
    assert!(elapsed < 1.0, "Should have less than 1 second elapsed");
}
