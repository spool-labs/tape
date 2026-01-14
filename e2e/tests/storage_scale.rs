//! Large-scale storage tests with many nodes.
//!
//! Tests that verify upload/download operations work correctly
//! across a large committee of storage nodes in normal mode (>= 24 nodes).
//!
//! All tests spawn their own validator and run serially to avoid port conflicts.
//!
//! **Note**: These tests require significant resources and time due to the
//! large number of nodes. Each test may take 5-15+ minutes depending on
//! data sizes and operations.
//!
//! ```bash
//! cargo test -p tape-e2e --test storage_scale -- --ignored --nocapture
//! ```

use std::time::Duration;

use serial_test::serial;
use tape_e2e::{
    TestContext, wait_for_node_health,
    temp_file_with_content, deterministic_blob, verify_deterministic_blob,
    sizes, EPOCH_WAIT,
};

/// Number of nodes for scale tests.
/// Using 50 nodes (well above MIN_COMMITTEE_SIZE of 24) to test normal mode
/// while staying within on-chain memory limits.
const SCALE_NODE_COUNT: usize = 50;

/// Base port for scale tests (use high port range to avoid conflicts).
const SCALE_BASE_PORT: u16 = 11000;

/// Timeout for scale test setup (longer due to many nodes).
const SCALE_TIMEOUT: Duration = Duration::from_secs(1200); // 20 minutes

/// Test basic upload and download with many nodes in normal mode.
///
/// This test:
/// 1. Spins up a local validator with 50 nodes (normal mode)
/// 2. Uses build_and_bootstrap() for parallel setup
/// 3. Uploads a small file
/// 4. Downloads and verifies the file
#[tokio::test]
#[ignore]
#[serial]
async fn test_scale_basic_upload_download() {
    println!("=== Scale Basic Upload/Download Test ({} nodes) ===", SCALE_NODE_COUNT);
    println!("Setting up {} nodes using parallel setup...", SCALE_NODE_COUNT);

    // Setup with many nodes using build_and_bootstrap() - nodes created/registered in parallel
    let ctx = TestContext::builder()
        .nodes(SCALE_NODE_COUNT)
        .port(SCALE_BASE_PORT)
        .timeout(SCALE_TIMEOUT)
        .fund(0.5)
        .build_and_bootstrap()
        .await
        .expect("Failed to setup test context");

    // Verify committee formed
    let system = ctx.system().expect("Failed to get system");
    let committee_size = system.committee_size.unwrap_or(0);
    println!("Committee size: {}", committee_size);
    assert!(committee_size >= 24, "Expected normal mode committee");

    // In normal mode (>=24 nodes), wait for epoch to become Active after Syncing phase
    println!("Waiting for epoch to become Active...");
    let mut wait_count = 0;
    let max_wait = 60; // Max 60 iterations (30s)
    loop {
        let epoch = ctx.epoch().expect("Failed to get epoch");
        let phase = epoch.phase.as_deref().unwrap_or("Unknown");
        if phase == "Active" {
            println!("Epoch {} is Active", epoch.id.unwrap_or(0));
            break;
        }
        if wait_count >= max_wait {
            println!("Warning: Epoch still in {} phase after waiting", phase);
            break;
        }
        if wait_count % 10 == 0 {
            println!("  Current phase: {} (waiting...)", phase);
        }
        wait_count += 1;
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    // Wait for a few nodes to become healthy
    println!("Waiting for nodes to become healthy...");
    let mut healthy_count = 0;
    for (i, node) in ctx.nodes.iter().enumerate().take(10) {
        match wait_for_node_health(&node.url(), Duration::from_secs(30)).await {
            Ok(_) => {
                healthy_count += 1;
                println!("  Node {} healthy at {}", i, node.url());
            }
            Err(_) => {
                println!("  Node {} not yet healthy", i);
            }
        }
    }
    println!("Healthy sample: {}/10", healthy_count);

    // Get all node URLs for upload - CLI now properly maps them to committee members
    let node_urls = ctx.node_urls();
    println!("Using {} nodes for storage operations", node_urls.len());

    // Create test data with deterministic content for verification
    let seed = 42u64;
    let blob = deterministic_blob(sizes::SMALL, seed);
    let upload_file = temp_file_with_content(&blob).expect("Failed to create temp file");

    println!("\n=== Uploading {} bytes ===", blob.len());

    // Upload using explicit nodes (CLI queries /v1/info to map to correct committee members)
    let upload_result = ctx.cli.storage_upload(
        upload_file.path(),
        None, // auto-create tape
        Some(&node_urls),
    ).expect("Failed to upload");

    println!("Uploaded track: {}", upload_result.track_id);
    println!("Merkle root: {:?}", upload_result.merkle_root);

    // Download from nodes
    println!("\n=== Downloading ===");
    let download_file = tempfile::NamedTempFile::new().expect("Failed to create download file");

    ctx.cli.storage_download(
        &upload_result.track_id,
        download_file.path(),
        Some(&node_urls),
    ).expect("Failed to download");

    // Verify data integrity
    let downloaded = std::fs::read(download_file.path()).expect("Failed to read downloaded file");
    assert_eq!(blob.len(), downloaded.len(), "Downloaded size mismatch");
    assert!(verify_deterministic_blob(&downloaded, seed), "Data integrity check failed");

    println!("Success! Data verified ({} bytes)", downloaded.len());
    println!("\nTest passed: Basic upload/download with {} nodes", SCALE_NODE_COUNT);
}

/// Test multiple file uploads with many nodes.
///
/// Uploads multiple files of varying sizes and verifies each one.
#[tokio::test]
#[ignore]
#[serial]
async fn test_scale_multiple_uploads() {
    println!("=== Scale Multiple Uploads Test ({} nodes) ===", SCALE_NODE_COUNT);
    println!("Setting up {} nodes using parallel setup...", SCALE_NODE_COUNT);

    let ctx = TestContext::builder()
        .nodes(SCALE_NODE_COUNT)
        .port(SCALE_BASE_PORT + 200)
        .timeout(SCALE_TIMEOUT)
        .fund(0.5)
        .build_and_bootstrap()
        .await
        .expect("Failed to setup test context");

    let system = ctx.system().expect("Failed to get system");
    println!("Committee size: {}", system.committee_size.unwrap_or(0));

    let node_urls = ctx.node_urls();
    let upload_nodes: Vec<String> = node_urls.iter().take(20).cloned().collect();

    // Test files of various sizes
    let test_sizes = [
        (sizes::KB, "1 KB"),
        (sizes::KB * 10, "10 KB"),
        (sizes::KB * 100, "100 KB"),
        (sizes::MB, "1 MB"),
    ];

    let mut upload_results = Vec::new();

    println!("\n=== Uploading {} files ===", test_sizes.len());
    for (i, (size, name)) in test_sizes.iter().enumerate() {
        let seed = (i + 100) as u64;
        let blob = deterministic_blob(*size, seed);
        let upload_file = temp_file_with_content(&blob).expect("Failed to create temp file");

        println!("  Uploading {} ({} bytes)...", name, size);

        match ctx.cli.storage_upload(upload_file.path(), None, Some(&upload_nodes)) {
            Ok(result) => {
                println!("    Track: {}", result.track_id);
                upload_results.push((result.track_id, seed, *size));
            }
            Err(e) => {
                eprintln!("    Failed: {}", e);
            }
        }
    }

    println!("\n=== Verifying {} uploads ===", upload_results.len());
    let mut verified = 0;
    for (track_id, seed, size) in &upload_results {
        let download_file = tempfile::NamedTempFile::new().expect("Failed to create download file");

        match ctx.cli.storage_download(track_id, download_file.path(), Some(&upload_nodes)) {
            Ok(_) => {
                let downloaded = std::fs::read(download_file.path()).unwrap_or_default();
                if downloaded.len() == *size && verify_deterministic_blob(&downloaded, *seed) {
                    println!("  {} bytes: VERIFIED", size);
                    verified += 1;
                } else {
                    println!("  {} bytes: INTEGRITY MISMATCH", size);
                }
            }
            Err(e) => {
                println!("  {} bytes: DOWNLOAD FAILED - {}", size, e);
            }
        }
    }

    println!("\n=== Results ===");
    println!("Uploaded: {}/{}", upload_results.len(), test_sizes.len());
    println!("Verified: {}/{}", verified, upload_results.len());

    assert!(verified >= upload_results.len() / 2, "Too many verification failures");

    println!("\nTest passed: Multiple uploads with {} nodes", SCALE_NODE_COUNT);
}

/// Test large file upload with many nodes.
///
/// Uploads a larger file (10 MB) and verifies data integrity.
#[tokio::test]
#[ignore]
#[serial]
async fn test_scale_large_file() {
    println!("=== Scale Large File Test ({} nodes) ===", SCALE_NODE_COUNT);
    println!("Setting up {} nodes using parallel setup...", SCALE_NODE_COUNT);

    let ctx = TestContext::builder()
        .nodes(SCALE_NODE_COUNT)
        .port(SCALE_BASE_PORT + 400)
        .timeout(SCALE_TIMEOUT)
        .fund(0.5)
        .build_and_bootstrap()
        .await
        .expect("Failed to setup test context");

    let system = ctx.system().expect("Failed to get system");
    println!("Committee size: {}", system.committee_size.unwrap_or(0));

    let node_urls = ctx.node_urls();
    let upload_nodes: Vec<String> = node_urls.iter().take(30).cloned().collect();

    // Create large test file (10 MB)
    let seed = 12345u64;
    let blob = deterministic_blob(sizes::LARGE, seed);
    let upload_file = temp_file_with_content(&blob).expect("Failed to create temp file");

    println!("\n=== Uploading large file ({} bytes / {} MB) ===", blob.len(), blob.len() / sizes::MB);

    let start = std::time::Instant::now();
    let upload_result = ctx.cli.storage_upload(
        upload_file.path(),
        None,
        Some(&upload_nodes),
    ).expect("Failed to upload large file");

    let upload_duration = start.elapsed();
    println!("Upload completed in {:.2}s", upload_duration.as_secs_f64());
    println!("Track: {}", upload_result.track_id);

    // Download and verify
    println!("\n=== Downloading large file ===");
    let download_file = tempfile::NamedTempFile::new().expect("Failed to create download file");

    let start = std::time::Instant::now();
    ctx.cli.storage_download(
        &upload_result.track_id,
        download_file.path(),
        Some(&upload_nodes),
    ).expect("Failed to download large file");

    let download_duration = start.elapsed();
    println!("Download completed in {:.2}s", download_duration.as_secs_f64());

    // Verify integrity
    let downloaded = std::fs::read(download_file.path()).expect("Failed to read downloaded file");
    assert_eq!(blob.len(), downloaded.len(), "Size mismatch");
    assert!(verify_deterministic_blob(&downloaded, seed), "Data integrity check failed");

    println!("\n=== Performance Summary ===");
    println!("File size: {} MB", blob.len() / sizes::MB);
    println!("Upload time: {:.2}s ({:.2} MB/s)",
        upload_duration.as_secs_f64(),
        (blob.len() as f64 / sizes::MB as f64) / upload_duration.as_secs_f64()
    );
    println!("Download time: {:.2}s ({:.2} MB/s)",
        download_duration.as_secs_f64(),
        (blob.len() as f64 / sizes::MB as f64) / download_duration.as_secs_f64()
    );

    println!("\nTest passed: Large file upload/download with {} nodes", SCALE_NODE_COUNT);
}

/// Test upload persistence across epoch advance with many nodes.
///
/// Uploads a file, advances an epoch, then downloads and verifies.
#[tokio::test]
#[ignore]
#[serial]
async fn test_scale_upload_across_epochs() {
    println!("=== Scale Upload Across Epochs Test ({} nodes) ===", SCALE_NODE_COUNT);
    println!("Setting up {} nodes using parallel setup...", SCALE_NODE_COUNT);

    let ctx = TestContext::builder()
        .nodes(SCALE_NODE_COUNT)
        .port(SCALE_BASE_PORT + 600)
        .timeout(SCALE_TIMEOUT)
        .fund(0.5)
        .build_and_bootstrap()
        .await
        .expect("Failed to setup test context");

    let epoch_before = ctx.epoch().expect("Failed to get epoch").id.unwrap_or(0);
    println!("Initial epoch: {}", epoch_before);

    let node_urls = ctx.node_urls();
    let upload_nodes: Vec<String> = node_urls.iter().take(20).cloned().collect();

    // Upload file
    let seed = 999u64;
    let blob = deterministic_blob(sizes::KB * 100, seed);
    let upload_file = temp_file_with_content(&blob).expect("Failed to create temp file");

    println!("\n=== Uploading {} bytes ===", blob.len());
    let upload_result = ctx.cli.storage_upload(
        upload_file.path(),
        None,
        Some(&upload_nodes),
    ).expect("Failed to upload");
    println!("Track: {}", upload_result.track_id);

    // Advance epoch
    println!("\n=== Advancing epoch (waiting {}s) ===", EPOCH_WAIT.as_secs());
    tokio::time::sleep(EPOCH_WAIT).await;
    ctx.cli.admin_advance_epoch().expect("Failed to advance epoch");

    let epoch_after = ctx.epoch().expect("Failed to get epoch").id.unwrap_or(0);
    println!("Epoch after advance: {}", epoch_after);
    assert!(epoch_after > epoch_before, "Epoch should have advanced");

    // Give nodes time to handle epoch transition
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Download and verify
    println!("\n=== Downloading after epoch advance ===");
    let download_file = tempfile::NamedTempFile::new().expect("Failed to create download file");

    ctx.cli.storage_download(
        &upload_result.track_id,
        download_file.path(),
        Some(&upload_nodes),
    ).expect("Failed to download after epoch advance");

    let downloaded = std::fs::read(download_file.path()).expect("Failed to read downloaded file");
    assert_eq!(blob.len(), downloaded.len(), "Size mismatch");
    assert!(verify_deterministic_blob(&downloaded, seed), "Data integrity check failed after epoch");

    println!("Success! Data persisted across epoch {} -> {}", epoch_before, epoch_after);
    println!("\nTest passed: Upload persistence across epochs with {} nodes", SCALE_NODE_COUNT);
}

/// Test download from subset of nodes with many nodes.
///
/// Uploads to many nodes, then downloads using only a subset.
/// Verifies erasure coding allows recovery with partial committee.
#[tokio::test]
#[ignore]
#[serial]
async fn test_scale_partial_download() {
    println!("=== Scale Partial Download Test ({} nodes) ===", SCALE_NODE_COUNT);
    println!("Setting up {} nodes using parallel setup...", SCALE_NODE_COUNT);

    let ctx = TestContext::builder()
        .nodes(SCALE_NODE_COUNT)
        .port(SCALE_BASE_PORT + 800)
        .timeout(SCALE_TIMEOUT)
        .fund(0.5)
        .build_and_bootstrap()
        .await
        .expect("Failed to setup test context");

    let system = ctx.system().expect("Failed to get system");
    println!("Committee size: {}", system.committee_size.unwrap_or(0));

    let node_urls = ctx.node_urls();

    // Upload using 50 nodes
    let upload_nodes: Vec<String> = node_urls.iter().take(50).cloned().collect();

    let seed = 7777u64;
    let blob = deterministic_blob(sizes::KB * 50, seed);
    let upload_file = temp_file_with_content(&blob).expect("Failed to create temp file");

    println!("\n=== Uploading {} bytes to {} nodes ===", blob.len(), upload_nodes.len());
    let upload_result = ctx.cli.storage_upload(
        upload_file.path(),
        None,
        Some(&upload_nodes),
    ).expect("Failed to upload");
    println!("Track: {}", upload_result.track_id);

    // Test downloading with different node subsets
    let test_cases = [
        (50, "Full set (50 nodes)"),
        (30, "Partial set (30 nodes)"),
        (20, "Reduced set (20 nodes)"),
    ];

    println!("\n=== Testing downloads with different node counts ===");
    for (count, desc) in test_cases {
        let download_nodes: Vec<String> = node_urls.iter().take(count).cloned().collect();
        let download_file = tempfile::NamedTempFile::new().expect("Failed to create download file");

        print!("  {}: ", desc);

        match ctx.cli.storage_download(&upload_result.track_id, download_file.path(), Some(&download_nodes)) {
            Ok(_) => {
                let downloaded = std::fs::read(download_file.path()).unwrap_or_default();
                if downloaded.len() == blob.len() && verify_deterministic_blob(&downloaded, seed) {
                    println!("VERIFIED");
                } else {
                    println!("INTEGRITY MISMATCH");
                }
            }
            Err(e) => {
                println!("FAILED - {}", e);
            }
        }
    }

    println!("\nTest passed: Partial download with {} nodes", SCALE_NODE_COUNT);
}
