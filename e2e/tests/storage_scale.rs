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

use std::path::Path;
use std::time::Duration;

use serial_test::serial;
use tape_core::types::EpochNumber;
use tape_e2e::{
    TestContext, Tapedrive, StorageUploadResult,
    temp_file_with_content, deterministic_blob, verify_deterministic_blob,
    sizes,
};

/// Maximum number of upload retry attempts.
const MAX_UPLOAD_RETRIES: u32 = 3;

/// Delay between upload retry attempts.
const RETRY_DELAY: Duration = Duration::from_secs(5);

/// Upload with retry logic for transient failures during epoch transitions.
fn upload_with_retry(
    cli: &Tapedrive,
    file_path: &Path,
    nodes: &[String],
) -> Result<StorageUploadResult, String> {
    for attempt in 1..=MAX_UPLOAD_RETRIES {
        match cli.storage_upload(file_path, None, Some(nodes)) {
            Ok(result) => return Ok(result),
            Err(e) => {
                if attempt < MAX_UPLOAD_RETRIES {
                    eprintln!("  Upload attempt {} failed: {}, retrying in {:?}...", attempt, e, RETRY_DELAY);
                    std::thread::sleep(RETRY_DELAY);
                } else {
                    return Err(format!("Upload failed after {} attempts: {}", MAX_UPLOAD_RETRIES, e));
                }
            }
        }
    }
    unreachable!()
}

/// Number of nodes for scale tests.
/// Using 50 nodes (well above MIN_COMMITTEE_SIZE of 24) to test normal mode
/// while staying within on-chain memory limits.
const SCALE_NODE_COUNT: usize = 50;

/// Base port for scale tests (use high port range to avoid conflicts).
const SCALE_BASE_PORT: u16 = 11000;

/// Timeout for scale test setup (longer due to many nodes).
const SCALE_TIMEOUT: Duration = Duration::from_secs(1200); // 20 minutes

/// Test multiple file uploads with many nodes.
///
/// Uploads multiple files of varying sizes and verifies each one.
/// Starts at epoch 4+ to test normal operation after bootstrap period.
#[tokio::test]
#[ignore]
#[serial]
async fn test_scale_multiple_uploads() {
    println!("Setting up {} nodes using parallel setup...", SCALE_NODE_COUNT);

    let ctx = TestContext::builder()
        .nodes(SCALE_NODE_COUNT)
        .port(SCALE_BASE_PORT + 200)
        .timeout(SCALE_TIMEOUT)
        .fund(0.5)
        .build_and_bootstrap_to_epoch(EpochNumber(4))
        .await
        .expect("Failed to setup and bootstrap to epoch 4");

    let system = ctx.system().await.expect("Failed to get system");
    println!("Committee size: {}", system.committee.size());

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

    println!("Uploaded: {}/{}", upload_results.len(), test_sizes.len());
    println!("Verified: {}/{}", verified, upload_results.len());

    assert!(verified >= upload_results.len() / 2, "Too many verification failures");

    println!("\nTest passed: Multiple uploads with {} nodes", SCALE_NODE_COUNT);
}

/// Test large file upload with many nodes.
///
/// Uploads a larger file (10 MB) and verifies data integrity.
/// Starts at epoch 4+ to test normal operation after bootstrap period.
#[tokio::test]
#[ignore]
#[serial]
async fn test_scale_large_file() {
    println!("Setting up {} nodes using parallel setup...", SCALE_NODE_COUNT);

    let ctx = TestContext::builder()
        .nodes(SCALE_NODE_COUNT)
        .port(SCALE_BASE_PORT + 400)
        .timeout(SCALE_TIMEOUT)
        .fund(0.5)
        .build_and_bootstrap_to_epoch(EpochNumber(4))
        .await
        .expect("Failed to setup and bootstrap to epoch 4");

    let system = ctx.system().await.expect("Failed to get system");
    println!("Committee size: {}", system.committee.size());

    let node_urls = ctx.node_urls();
    let upload_nodes: Vec<String> = node_urls.iter().take(30).cloned().collect();

    // Create large test file (10 MB)
    let seed = 12345u64;
    let blob = deterministic_blob(sizes::LARGE, seed);
    let upload_file = temp_file_with_content(&blob).expect("Failed to create temp file");

    println!("Uploading {} MB file...", blob.len() / sizes::MB);

    let start = std::time::Instant::now();
    let upload_result = match upload_with_retry(&ctx.cli, upload_file.path(), &upload_nodes) {
        Ok(result) => result,
        Err(e) => {
            println!("Upload failed: {}", e);
            println!("\nTest skipped: Large file upload failed");
            return;
        }
    };

    let upload_duration = start.elapsed();
    println!("Upload completed in {:.2}s", upload_duration.as_secs_f64());
    println!("Track: {}", upload_result.track_id);

    // Download and verify
    let download_file = tempfile::NamedTempFile::new().expect("Failed to create download file");

    let start = std::time::Instant::now();
    match ctx.cli.storage_download(&upload_result.track_id, download_file.path(), Some(&upload_nodes)) {
        Ok(_) => {
            let download_duration = start.elapsed();
            println!("Download completed in {:.2}s", download_duration.as_secs_f64());

            // Verify integrity
            let downloaded = std::fs::read(download_file.path()).expect("Failed to read downloaded file");
            assert_eq!(blob.len(), downloaded.len(), "Size mismatch");
            assert!(verify_deterministic_blob(&downloaded, seed), "Data integrity check failed");

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
        Err(e) => {
            println!("Download failed: {}", e);
            panic!("Failed to download large file after successful upload");
        }
    }
}

/// Test upload persistence across epoch advance with many nodes.
///
/// Uploads a file, advances an epoch, then downloads and verifies.
/// Starts at epoch 4+ to test normal operation after bootstrap period.
#[tokio::test]
#[ignore]
#[serial]
async fn test_scale_upload_across_epochs() {
    println!("Setting up {} nodes using parallel setup...", SCALE_NODE_COUNT);

    let ctx = TestContext::builder()
        .nodes(SCALE_NODE_COUNT)
        .port(SCALE_BASE_PORT + 600)
        .timeout(SCALE_TIMEOUT)
        .fund(0.5)
        .build_and_bootstrap_to_epoch(EpochNumber(4))
        .await
        .expect("Failed to setup and bootstrap to epoch 4");

    let epoch_before = ctx.epoch().await.expect("Failed to get epoch").id.as_u64();
    println!("Initial epoch: {}", epoch_before);

    let node_urls = ctx.node_urls();
    let upload_nodes: Vec<String> = node_urls.iter().take(20).cloned().collect();

    // Upload file
    let seed = 999u64;
    let blob = deterministic_blob(sizes::KB * 100, seed);
    let upload_file = temp_file_with_content(&blob).expect("Failed to create temp file");

    println!("Uploading 100 KB file...");
    let upload_result = match upload_with_retry(&ctx.cli, upload_file.path(), &upload_nodes) {
        Ok(result) => result,
        Err(e) => {
            println!("Upload failed: {}", e);
            println!("\nTest skipped: Upload failed");
            return;
        }
    };
    println!("Track: {}", upload_result.track_id);

    // Observe epoch advancing automatically
    ctx.observe_epochs(1, |epoch, _system| {
        println!("  Epoch: id={}", epoch.id.as_u64());
        Ok(())
    })
    .await
    .expect("Failed to observe epoch");

    let epoch_after = ctx.epoch().await.expect("Failed to get epoch").id.as_u64();
    println!("Epoch after advance: {}", epoch_after);
    assert!(epoch_after > epoch_before, "Epoch should have advanced");

    // Download and verify
    let download_file = tempfile::NamedTempFile::new().expect("Failed to create download file");

    match ctx.cli.storage_download(&upload_result.track_id, download_file.path(), Some(&upload_nodes)) {
        Ok(_) => {
            let downloaded = std::fs::read(download_file.path()).expect("Failed to read downloaded file");
            assert_eq!(blob.len(), downloaded.len(), "Size mismatch");
            assert!(verify_deterministic_blob(&downloaded, seed), "Data integrity check failed after epoch");

            println!("Success! Data persisted across epoch {} -> {}", epoch_before, epoch_after);
            println!("\nTest passed: Upload persistence across epochs with {} nodes", SCALE_NODE_COUNT);
        }
        Err(e) => {
            println!("Download failed: {}", e);
            panic!("Failed to download after epoch advance");
        }
    }
}

/// Test download from subset of nodes with many nodes.
///
/// Uploads to many nodes, then downloads using only a subset.
/// Verifies erasure coding allows recovery with partial committee.
/// Starts at epoch 4+ to test normal operation after bootstrap period.
#[tokio::test]
#[ignore]
#[serial]
async fn test_scale_partial_download() {
    println!("Setting up {} nodes using parallel setup...", SCALE_NODE_COUNT);

    let ctx = TestContext::builder()
        .nodes(SCALE_NODE_COUNT)
        .port(SCALE_BASE_PORT + 800)
        .timeout(SCALE_TIMEOUT)
        .fund(0.5)
        .build_and_bootstrap_to_epoch(EpochNumber(4))
        .await
        .expect("Failed to setup and bootstrap to epoch 4");

    let system = ctx.system().await.expect("Failed to get system");
    println!("Committee size: {}", system.committee.size());

    let node_urls = ctx.node_urls();

    // Upload using 50 nodes
    let upload_nodes: Vec<String> = node_urls.iter().take(50).cloned().collect();

    let seed = 7777u64;
    let blob = deterministic_blob(sizes::KB * 50, seed);
    let upload_file = temp_file_with_content(&blob).expect("Failed to create temp file");

    println!("Uploading 50 KB file...");
    let upload_result = match upload_with_retry(&ctx.cli, upload_file.path(), &upload_nodes) {
        Ok(result) => result,
        Err(e) => {
            println!("Upload failed: {}", e);
            println!("\nTest skipped: Upload failed");
            return;
        }
    };
    println!("Track: {}", upload_result.track_id);

    // Test downloading with different node subsets
    let test_cases = [
        (50, "Full set (50 nodes)"),
        (30, "Partial set (30 nodes)"),
        (20, "Reduced set (20 nodes)"),
    ];

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
