//! Basic example showing how to use Prometheus metrics with store
//!
//! Run with: cargo run --example metrics_basic --features metrics

#[cfg(feature = "metrics")]
fn main() {
    use store::{get_metrics, init_metrics, Store, WriteBatch};
    use store_memory::MemoryStore;
    use tape_metrics::prometheus::{Encoder, TextEncoder};
    use tape_metrics::MetricsRegistry;

    // Initialize metrics registry and store metrics
    println!("Initializing metrics...");
    MetricsRegistry::init();
    init_metrics();

    // Create a store
    let store = MemoryStore::new();

    println!("\nPerforming various operations...");

    // Perform some operations
    store.put("users", b"alice", b"admin").unwrap();
    store.put("users", b"bob", b"user").unwrap();
    store.put("posts", b"post1", b"Hello World").unwrap();

    // Get operations
    let _ = store.get("users", b"alice").unwrap();
    let _ = store.get("users", b"nonexistent").unwrap(); // Not found

    // Contains operations
    let _ = store.contains("users", b"alice").unwrap();
    let _ = store.contains("users", b"charlie").unwrap();

    // Delete operation
    store.delete("users", b"bob").unwrap();

    // Batch operation
    let mut batch = WriteBatch::new();
    batch.put("users", b"charlie", b"moderator");
    batch.put("users", b"diana", b"admin");
    batch.delete("posts", b"post1");
    store.write_batch(batch).unwrap();

    // Iterator operations
    let _all_users: Vec<_> = store.iter("users").unwrap().collect();
    let _prefix_users: Vec<_> = store.iter_prefix("users", b"a").unwrap().collect();

    println!("Operations complete!\n");

    // Gather and print metrics
    println!("=== Collected Metrics ===\n");

    let registry = MetricsRegistry::get().expect("Registry should be initialized");
    let encoder = TextEncoder::new();
    let metric_families = registry.prometheus_registry().gather();

    let mut buffer = vec![];
    encoder.encode(&metric_families, &mut buffer).unwrap();

    let metrics_output = String::from_utf8(buffer).unwrap();
    println!("{}", metrics_output);

    // Print summary using direct metric access
    println!("\n=== Summary Statistics ===\n");

    if let Some(store_metrics) = get_metrics() {
        println!("Operations summary:");

        // Get put operations for users
        if let Ok(counter) = store_metrics
            .operations_total
            .get_metric_with_label_values(&["users", "put", "success"])
        {
            println!("  users/put/success: {}", counter.get());
        }

        // Get operations
        if let Ok(counter) = store_metrics
            .operations_total
            .get_metric_with_label_values(&["users", "get", "success"])
        {
            println!("  users/get/success: {}", counter.get());
        }

        // Delete operations
        if let Ok(counter) = store_metrics
            .operations_total
            .get_metric_with_label_values(&["users", "delete", "success"])
        {
            println!("  users/delete/success: {}", counter.get());
        }

        // Bytes written
        if let Ok(counter) = store_metrics
            .bytes_written_total
            .get_metric_with_label_values(&["users"])
        {
            println!("  users bytes written: {}", counter.get());
        }

        // Bytes read
        if let Ok(counter) = store_metrics
            .bytes_read_total
            .get_metric_with_label_values(&["users"])
        {
            println!("  users bytes read: {}", counter.get());
        }
    }

    println!("\nExample complete!");
}

#[cfg(not(feature = "metrics"))]
fn main() {
    eprintln!("This example requires the 'metrics' feature to be enabled.");
    eprintln!("Run with: cargo run --example metrics_basic --features metrics");
    std::process::exit(1);
}
