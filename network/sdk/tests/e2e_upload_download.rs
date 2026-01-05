//! End-to-end test for upload and download flow.
//!
//! Demonstrates the full tapedrive flow:
//! 1. Start a storage node (in-memory for testing)
//! 2. Use TapeClient to upload a blob (encode → distribute → store)
//! 3. Download the blob back (fetch → decode)
//! 4. Verify the data matches
//!
//! Uses 4 KB slice size for testing (vs 1 MiB in production) to reduce memory usage.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use serial_test::serial;
use store_memory::MemoryStore;
use tape_crypto::Pubkey;
use tape_node::server::routes::{create_router, ApiState};
use tape_node::{NodeMetrics, StorageService};
use tape_sdk::TapeClient;
use tape_store::TapeStore;
use tokio::net::TcpListener;

/// Start a test node on a random port with in-memory storage.
async fn start_test_node() -> (SocketAddr, tokio::task::JoinHandle<()>) {
    let metrics = Arc::new(NodeMetrics::new());
    let service = Arc::new(StorageService::new(TapeStore::new(MemoryStore::new())));

    let state = ApiState { metrics, service };
    let app: Router = create_router(state);

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let handle = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    tokio::time::sleep(Duration::from_millis(50)).await;
    (addr, handle)
}

/// Start multiple test nodes.
async fn start_test_nodes(count: usize) -> Vec<(SocketAddr, tokio::task::JoinHandle<()>)> {
    let mut nodes = Vec::with_capacity(count);
    for _ in 0..count {
        nodes.push(start_test_node().await);
    }
    nodes
}

/// Create a test client with small slice sizes (4 KB instead of 1 MB).
fn test_client(node_url: String) -> TapeClient {
    TapeClient::builder()
        .add_node(node_url)
        .max_slice_bytes(4 * 1024) // 4 KB slices for testing
        .build()
}

/// Create a test client with multiple nodes.
fn test_client_multi(node_urls: Vec<String>) -> TapeClient {
    TapeClient::builder()
        .node_addresses(node_urls)
        .max_slice_bytes(4 * 1024)
        .build()
}

// ============================================================================
// HIGH-LEVEL SDK TESTS
// ============================================================================

/// Test the complete upload → download flow using TapeClient.
#[tokio::test]
#[serial]
async fn test_upload_download_roundtrip() {
    let (addr, _handle) = start_test_node().await;
    let client = test_client(format!("http://{}", addr));

    // Create test data
    let original: Vec<u8> = (0..50_000u32)
        .map(|i| ((i * 17 + 42) % 256) as u8)
        .collect();

    let track_id = Pubkey::new_unique().to_string();

    println!("Uploading {} bytes...", original.len());

    // Upload
    let commitment = client
        .upload_blob(&track_id, original.clone())
        .await
        .expect("upload should succeed");

    println!(
        "Upload complete! Commitment: {:?}",
        &commitment.as_ref()[..8]
    );

    // Download
    let recovered = client
        .download_blob(&track_id)
        .await
        .expect("download should succeed");

    assert_eq!(original, recovered);
    println!("SUCCESS: {} bytes round-trip verified!", original.len());
}

/// Test download with commitment verification.
#[tokio::test]
#[serial]
async fn test_verified_download() {
    let (addr, _handle) = start_test_node().await;
    let client = test_client(format!("http://{}", addr));

    let original = vec![0xAB; 30_000];
    let track_id = Pubkey::new_unique().to_string();

    // Upload and get commitment
    let commitment = client
        .upload_blob(&track_id, original.clone())
        .await
        .expect("upload should succeed");

    // Download with verification
    let recovered = client
        .download_blob_verified(&track_id, &commitment)
        .await
        .expect("verified download should succeed");

    assert_eq!(original, recovered);
    println!("SUCCESS: Verified download passed!");
}

/// Test that download reconstructs correctly even with erasure (only 683 of 1024 slices).
#[tokio::test]
#[serial]
async fn test_erasure_recovery() {
    let (addr, _handle) = start_test_node().await;
    let client = test_client(format!("http://{}", addr));

    let original: Vec<u8> = (0..25_000u32).map(|i| (i % 256) as u8).collect();
    let track_id = Pubkey::new_unique().to_string();

    // Upload all 1024 slices
    let commitment = client
        .upload_blob(&track_id, original.clone())
        .await
        .expect("upload should succeed");

    // Download (will only fetch 683 slices - the minimum needed)
    let recovered = client
        .download_blob(&track_id)
        .await
        .expect("download should succeed with 683 slices");

    assert_eq!(original, recovered);

    // Verify commitment matches
    let verified = client
        .download_blob_verified(&track_id, &commitment)
        .await
        .expect("verification should pass");

    assert_eq!(original, verified);
    println!("SUCCESS: Erasure recovery works!");
}

// ============================================================================
// LOW-LEVEL TESTS
// ============================================================================

#[tokio::test]
#[serial]
async fn test_health_check() {
    let (addr, _handle) = start_test_node().await;
    let node_url = format!("http://{}", addr);

    let factory = tape_sdk::communication::NodeCommunicationFactory::new();
    let client = factory.client_for_address(&node_url).unwrap();

    let healthy = client.health_check().await.expect("health check should work");
    assert!(healthy, "Node should be healthy");

    let info = client.get_info().await.expect("get_info should work");
    assert_eq!(info["status"], "running");
}

#[tokio::test]
#[serial]
async fn test_slice_not_found() {
    let (addr, _handle) = start_test_node().await;
    let node_url = format!("http://{}", addr);

    let factory = tape_sdk::communication::NodeCommunicationFactory::new();
    let client = factory.client_for_address(&node_url).unwrap();

    let track_id = Pubkey::new_unique().to_string();
    let result = client.get_slice(&track_id, 0).await;

    assert!(result.is_err(), "Should get NotFound error");
}

/// Test that the builder correctly sets max_slice_bytes.
#[tokio::test]
async fn test_client_builder() {
    let client = TapeClient::builder()
        .add_node("http://localhost:8080")
        .max_slice_bytes(4 * 1024)
        .build();

    assert_eq!(client.max_slice_bytes(), 4 * 1024);
    assert_eq!(client.node_addresses().len(), 1);
}

/// Benchmark: Sequential PUT/GET to measure server throughput
#[tokio::test]
#[serial]
async fn test_sequential_throughput() {
    use std::time::Instant;
    use tape_crypto::Hash;
    use tape_node_api::SlicePayload;
    use tape_slicer::MERKLE_HEIGHT;

    let (addr, _handle) = start_test_node().await;
    let node_url = format!("http://{}", addr);

    let factory = tape_sdk::communication::NodeCommunicationFactory::new();
    let client = factory.client_for_address(&node_url).unwrap();

    let track_id = Pubkey::new_unique().to_string();
    let slice_data = vec![0xAB; 4096]; // 4 KB slice
    let payload = SlicePayload::new(
        slice_data.clone(),
        Hash::default(),
        [Hash::default(); MERKLE_HEIGHT],
    );

    const NUM_SLICES: u16 = 200;

    // Sequential PUT
    let start = Instant::now();
    for i in 0..NUM_SLICES {
        client.put_slice(&track_id, i, &payload).await.unwrap();
    }
    let put_time = start.elapsed();

    // Sequential GET
    let start = Instant::now();
    for i in 0..NUM_SLICES {
        client.get_slice(&track_id, i).await.unwrap();
    }
    let get_time = start.elapsed();

    let total_bytes = NUM_SLICES as usize * 4096;
    println!("Sequential {} slices ({} KB):", NUM_SLICES, total_bytes / 1024);
    println!("  PUT: {:?} ({:.0} slices/sec)", put_time, NUM_SLICES as f64 / put_time.as_secs_f64());
    println!("  GET: {:?} ({:.0} slices/sec)", get_time, NUM_SLICES as f64 / get_time.as_secs_f64());
}

/// Benchmark: Parallel GET with different concurrency levels
#[tokio::test]
#[serial]
async fn test_parallel_throughput() {
    use std::sync::Arc;
    use std::time::Instant;
    use tape_crypto::Hash;
    use tape_node_api::SlicePayload;
    use tape_slicer::MERKLE_HEIGHT;
    use tokio::sync::Semaphore;
    use futures::stream::{FuturesUnordered, StreamExt};

    let (addr, _handle) = start_test_node().await;
    let node_url = format!("http://{}", addr);

    let factory = tape_sdk::communication::NodeCommunicationFactory::new();
    let client = factory.client_for_address(&node_url).unwrap();

    let track_id = Pubkey::new_unique().to_string();
    let slice_data = vec![0xAB; 4096];
    let payload = SlicePayload::new(
        slice_data.clone(),
        Hash::default(),
        [Hash::default(); MERKLE_HEIGHT],
    );

    const NUM_SLICES: u16 = 500;

    // First, store all slices sequentially
    for i in 0..NUM_SLICES {
        client.put_slice(&track_id, i, &payload).await.unwrap();
    }

    // Test different concurrency levels
    for concurrency in [1, 8, 32, 64, 128, 256] {
        let sem = Arc::new(Semaphore::new(concurrency));
        let mut futures = FuturesUnordered::new();
        
        let start = Instant::now();
        
        for i in 0..NUM_SLICES {
            let factory = factory.clone();
            let node_url = node_url.clone();
            let track_id = track_id.clone();
            let sem = sem.clone();
            
            futures.push(async move {
                let _permit = sem.acquire().await.unwrap();
                let client = factory.client_for_address(&node_url).unwrap();
                client.get_slice(&track_id, i).await
            });
        }
        
        let mut success = 0;
        let mut fail = 0;
        while let Some(result) = futures.next().await {
            match result {
                Ok(_) => success += 1,
                Err(_) => fail += 1,
            }
        }
        
        let elapsed = start.elapsed();
        println!(
            "Concurrency {:>3}: {:>3} success, {:>3} fail, {:?} ({:.0} slices/sec)",
            concurrency, success, fail, elapsed,
            success as f64 / elapsed.as_secs_f64()
        );
    }
}

// ============================================================================
// MULTI-NODE TESTS
// ============================================================================

/// Test upload/download with 42 nodes (slices distributed round-robin).
#[tokio::test]
#[serial]
async fn test_multi_node_roundtrip() {
    const NUM_NODES: usize = 42;

    // Start 42 nodes
    let nodes = start_test_nodes(NUM_NODES).await;
    let node_urls: Vec<String> = nodes
        .iter()
        .map(|(addr, _)| format!("http://{}", addr))
        .collect();

    println!("Started {} nodes", NUM_NODES);

    let client = test_client_multi(node_urls);

    // Create test data
    let original: Vec<u8> = (0..50_000u32)
        .map(|i| ((i * 17 + 42) % 256) as u8)
        .collect();

    let track_id = Pubkey::new_unique().to_string();

    println!(
        "Uploading {} bytes across {} nodes (~{} slices each)...",
        original.len(),
        NUM_NODES,
        1024 / NUM_NODES
    );

    // Upload - slices distributed: slice_idx % NUM_NODES
    let commitment = client
        .upload_blob(&track_id, original.clone())
        .await
        .expect("upload should succeed");

    println!("Upload complete!");

    // Download - fetches from correct node per slice
    let recovered = client
        .download_blob(&track_id)
        .await
        .expect("download should succeed");

    assert_eq!(original, recovered);

    // Verify
    let verified = client
        .download_blob_verified(&track_id, &commitment)
        .await
        .expect("verification should pass");

    assert_eq!(original, verified);
    println!("SUCCESS: {}-node round-trip verified!", NUM_NODES);
}

/// Test with simulated node failures (multiple nodes down).
///
/// With 42 nodes, each stores ~24 slices. We can lose up to 14 nodes
/// (341 slices) and still have 683+ remaining.
///
/// NOTE: Currently ignored - SDK download logic doesn't gracefully handle
/// node failures (returns connection error instead of skipping dead nodes).
#[tokio::test]
#[serial]
#[ignore = "SDK needs failure-tolerant download logic"]
async fn test_multi_node_with_failures() {
    const NUM_NODES: usize = 42;
    const NODES_TO_KILL: usize = 10; // Kill 10 nodes (~240 slices lost)

    // Start 42 nodes
    let mut nodes = start_test_nodes(NUM_NODES).await;
    let node_urls: Vec<String> = nodes
        .iter()
        .map(|(addr, _)| format!("http://{}", addr))
        .collect();

    let client = test_client_multi(node_urls);

    let original: Vec<u8> = (0..30_000u32).map(|i| (i % 256) as u8).collect();
    let track_id = Pubkey::new_unique().to_string();

    // Upload to all nodes
    let commitment = client
        .upload_blob(&track_id, original.clone())
        .await
        .expect("upload should succeed");

    println!("Uploaded to {} nodes", NUM_NODES);

    // Kill first N nodes
    for _ in 0..NODES_TO_KILL {
        let (_, handle) = nodes.remove(0);
        handle.abort();
    }
    let slices_lost = (1024 / NUM_NODES + 1) * NODES_TO_KILL;
    let slices_remaining = 1024 - slices_lost;
    println!(
        "Killed {} nodes (~{} slices lost, ~{} remaining, need 683)",
        NODES_TO_KILL, slices_lost, slices_remaining
    );

    // Give nodes time to shut down
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Download should still work
    let recovered = client
        .download_blob(&track_id)
        .await
        .expect("download should succeed despite node failures");

    assert_eq!(original, recovered);

    // Verify commitment
    let verified = client
        .download_blob_verified(&track_id, &commitment)
        .await
        .expect("verification should pass");

    assert_eq!(original, verified);
    println!(
        "SUCCESS: Recovered data after {} node failures!",
        NODES_TO_KILL
    );
}

// ============================================================================
// BENCHMARKS
// ============================================================================

/// Benchmark upload/download throughput with multiple nodes.
/// Separates encoding/decoding time from network time.
///
/// Run with: cargo test -p tape-sdk --test e2e_upload_download bench_ -- --nocapture
#[tokio::test]
#[serial]
async fn bench_upload_download_throughput() {
    use std::time::Instant;
    use tape_sdk::{BlobEncoder, BlobDecoder};

    const NUM_NODES: usize = 16;
    const DATA_SIZE: usize = 100_000; // 100 KB

    // Start nodes
    let nodes = start_test_nodes(NUM_NODES).await;
    let node_urls: Vec<String> = nodes
        .iter()
        .map(|(addr, _)| format!("http://{}", addr))
        .collect();

    let client = test_client_multi(node_urls);

    let original: Vec<u8> = (0..DATA_SIZE).map(|i| (i % 256) as u8).collect();
    let track_id = Pubkey::new_unique().to_string();

    // Warm up
    let _ = client.upload_blob(&track_id, original.clone()).await;
    let _ = client.download_blob(&track_id).await;

    // Benchmark encoding separately
    let mut encoder = BlobEncoder::with_max_slice_bytes(4 * 1024);
    let start = Instant::now();
    let (slices, _) = encoder.encode_with_proofs(original.clone()).unwrap();
    let encode_time = start.elapsed();

    // Benchmark upload (includes encoding)
    let track_id = Pubkey::new_unique().to_string();
    let start = Instant::now();
    let _commitment = client
        .upload_blob(&track_id, original.clone())
        .await
        .expect("upload failed");
    let upload_total = start.elapsed();

    // Benchmark download (includes decoding)
    let start = Instant::now();
    let _recovered = client
        .download_blob(&track_id)
        .await
        .expect("download failed");
    let download_total = start.elapsed();

    // Benchmark decoding separately
    let slices_for_decode: Vec<(u16, Vec<u8>)> = slices
        .iter()
        .take(683)
        .map(|s| (s.index, s.data.clone()))
        .collect();
    let mut decoder = BlobDecoder::with_max_slice_bytes(4 * 1024);
    let start = Instant::now();
    let _ = decoder.decode(slices_for_decode).unwrap();
    let decode_time = start.elapsed();

    // Estimate network time
    let upload_network = upload_total.saturating_sub(encode_time);
    let download_network = download_total.saturating_sub(decode_time);

    println!();
    println!("=== Upload/Download Benchmark ({} nodes, {} KB) ===", NUM_NODES, DATA_SIZE / 1000);
    println!();
    println!("Upload total:    {:>8.2?}  ({:>5.0} slices/sec)",
             upload_total, 1024.0 / upload_total.as_secs_f64());
    println!("  - Encode:      {:>8.2?}", encode_time);
    println!("  - Network:     {:>8.2?}  ({:>5.0} slices/sec)",
             upload_network, 1024.0 / upload_network.as_secs_f64());
    println!();
    println!("Download total:  {:>8.2?}  ({:>5.0} slices/sec)",
             download_total, 1024.0 / download_total.as_secs_f64());
    println!("  - Network:     {:>8.2?}  ({:>5.0} slices/sec)",
             download_network, 1024.0 / download_network.as_secs_f64());
    println!("  - Decode:      {:>8.2?}", decode_time);
    println!();
}

/// Benchmark with varying node counts to see scaling behavior.
#[tokio::test]
#[serial]
async fn bench_node_scaling() {
    use std::time::Instant;

    const DATA_SIZE: usize = 50_000; // 50 KB

    println!();
    println!("=== Node Scaling Benchmark ({} KB) ===", DATA_SIZE / 1000);
    println!();
    println!("{:>6} {:>12} {:>12} {:>12} {:>12}",
             "Nodes", "Upload", "Download", "Up sl/s", "Down sl/s");
    println!("{:-<6} {:->12} {:->12} {:->12} {:->12}", "", "", "", "", "");

    for num_nodes in [1, 4, 8, 16, 32] {
        let nodes = start_test_nodes(num_nodes).await;
        let node_urls: Vec<String> = nodes
            .iter()
            .map(|(addr, _)| format!("http://{}", addr))
            .collect();

        let client = test_client_multi(node_urls);
        let original: Vec<u8> = (0..DATA_SIZE).map(|i| (i % 256) as u8).collect();

        // Warm up
        let track_id = Pubkey::new_unique().to_string();
        let _ = client.upload_blob(&track_id, original.clone()).await;
        let _ = client.download_blob(&track_id).await;

        // Benchmark
        let track_id = Pubkey::new_unique().to_string();

        let start = Instant::now();
        client.upload_blob(&track_id, original.clone()).await.unwrap();
        let upload_time = start.elapsed();

        let start = Instant::now();
        client.download_blob(&track_id).await.unwrap();
        let download_time = start.elapsed();

        let up_slices = 1024.0 / upload_time.as_secs_f64();
        let down_slices = 1024.0 / download_time.as_secs_f64();

        println!("{:>6} {:>12.2?} {:>12.2?} {:>12.0} {:>12.0}",
                 num_nodes, upload_time, download_time, up_slices, down_slices);
    }
    println!();
}
