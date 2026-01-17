//! Striping performance comparison benchmark.
//!
//! Compares single-pass RS encoding (BasicSlicer approach) vs multi-stripe
//! encoding (StripedSlicer approach) to measure overhead and memory differences.
//!
//! Run with: cargo test -p tape-slicer --release -- --nocapture striping

use std::time::{Duration, Instant};

use crate::consts::{DATA_SLICES, CODING_SLICES, SLICE_COUNT};
use crate::{BasicSlicer, Slicer, Blob, Slice};

/// Create deterministic test payload.
fn make_payload(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i % 251) as u8).collect()
}

/// Encode using single RS pass (BasicSlicer approach).
/// Returns (duration, encoded_slices).
fn encode_basic(slicer: &mut BasicSlicer, payload: &[u8]) -> (Duration, [Slice; SLICE_COUNT]) {
    let start = Instant::now();
    let slices = slicer.encode(Blob::from(payload.to_vec())).expect("encode");
    (start.elapsed(), slices)
}

/// Encode using multiple stripes (StripedSlicer approach).
/// Returns (duration, num_stripes, per-stripe slices).
fn encode_striped(slicer: &mut BasicSlicer, payload: &[u8], stripe_size: usize) -> (Duration, usize, Vec<[Slice; SLICE_COUNT]>) {
    let start = Instant::now();

    let num_stripes = (payload.len() + stripe_size - 1) / stripe_size;
    let mut stripe_slices = Vec::with_capacity(num_stripes);

    // Encode each stripe
    for stripe_idx in 0..num_stripes {
        let stripe_start = stripe_idx * stripe_size;
        let stripe_end = (stripe_start + stripe_size).min(payload.len());
        let slices = slicer.encode(Blob::from(payload[stripe_start..stripe_end].to_vec()))
            .expect("encode stripe");
        stripe_slices.push(slices);
    }

    (start.elapsed(), num_stripes, stripe_slices)
}

/// Decode using single RS pass.
fn decode_basic(slicer: &mut BasicSlicer, slices: &[Slice; SLICE_COUNT]) -> Duration {
    let start = Instant::now();

    let opt_slices: [Option<Slice>; SLICE_COUNT] = std::array::from_fn(|i| Some(slices[i].clone()));
    let _restored = slicer.decode(&opt_slices).expect("decode");

    start.elapsed()
}

/// Decode using multiple stripes.
fn decode_striped(slicer: &mut BasicSlicer, stripe_slices: &[[Slice; SLICE_COUNT]]) -> Duration {
    let start = Instant::now();

    for slices in stripe_slices {
        let opt_slices: [Option<Slice>; SLICE_COUNT] = std::array::from_fn(|i| Some(slices[i].clone()));
        let _restored = slicer.decode(&opt_slices).expect("decode stripe");
    }

    start.elapsed()
}

/// Benchmark result for a single configuration.
#[derive(Debug)]
struct BenchResult {
    blob_size_mb: usize,
    approach: &'static str,
    stripe_size_mb: Option<usize>,
    num_stripes: usize,
    encode_time_ms: f64,
    decode_time_ms: f64,
    peak_mem_mb: f64,
}

impl BenchResult {
    fn header() -> String {
        format!(
            "{:<10} {:<10} {:<10} {:<8} {:<12} {:<12} {:<12}",
            "Blob(MB)", "Approach", "Stripe(MB)", "Stripes", "Encode(ms)", "Decode(ms)", "PeakMem(MB)"
        )
    }

    fn row(&self) -> String {
        let stripe_str = self.stripe_size_mb
            .map(|s| s.to_string())
            .unwrap_or_else(|| "-".to_string());
        format!(
            "{:<10} {:<10} {:<10} {:<8} {:<12.2} {:<12.2} {:<12.1}",
            self.blob_size_mb,
            self.approach,
            stripe_str,
            self.num_stripes,
            self.encode_time_ms,
            self.decode_time_ms,
            self.peak_mem_mb
        )
    }
}

/// Estimate peak memory for basic encoding.
/// Peak = blob + working buffers + output slices
fn estimate_basic_mem(blob_size: usize) -> f64 {
    let slice_size = (blob_size + DATA_SLICES - 1) / DATA_SLICES;
    let output_size = SLICE_COUNT * slice_size;
    let working = blob_size; // RS library working buffers
    (blob_size + working + output_size) as f64 / (1 << 20) as f64
}

/// Estimate peak memory for striped encoding.
/// Peak = stripe + working buffers + accumulated output
fn estimate_striped_mem(blob_size: usize, stripe_size: usize) -> f64 {
    let chunk_size = (stripe_size + DATA_SLICES - 1) / DATA_SLICES;
    let stripe_output = SLICE_COUNT * chunk_size;
    let working = stripe_size;
    // Final output is same size as basic, but we build incrementally
    let final_output = (blob_size * SLICE_COUNT / DATA_SLICES) as f64;
    // During encoding, we hold: current stripe + working + partial output
    // Worst case is near the end when output is almost complete
    (stripe_size + working + stripe_output) as f64 / (1 << 20) as f64 + final_output / (1 << 20) as f64 * 0.5
}

/// Run benchmarks for all configurations.
fn run_benchmarks() -> Vec<BenchResult> {
    let mut results = Vec::new();

    // Blob sizes to test (in bytes)
    let blob_sizes = [
        1 << 20,   // 1 MB
        4 << 20,   // 4 MB
        10 << 20,  // 10 MB
        25 << 20,  // 25 MB
        50 << 20,  // 50 MB
    ];

    // Stripe sizes to test (in bytes)
    let stripe_sizes = [
        256 << 10, // 256 KB
        1 << 20,   // 1 MB
        4 << 20,   // 4 MB
    ];

    // Create slicers with enough capacity for largest blob
    let max_slice_bytes = (blob_sizes.iter().max().unwrap() / DATA_SLICES) + 1024;
    let mut slicer = BasicSlicer::with_max_slice_bytes(max_slice_bytes);

    for &blob_size in &blob_sizes {
        let blob_mb = blob_size >> 20;
        let payload = make_payload(blob_size);

        // Basic encoding
        let (encode_time, slices) = encode_basic(&mut slicer, &payload);
        let decode_time = decode_basic(&mut slicer, &slices);

        results.push(BenchResult {
            blob_size_mb: blob_mb,
            approach: "Basic",
            stripe_size_mb: None,
            num_stripes: 1,
            encode_time_ms: encode_time.as_secs_f64() * 1000.0,
            decode_time_ms: decode_time.as_secs_f64() * 1000.0,
            peak_mem_mb: estimate_basic_mem(blob_size),
        });

        // Striped encoding with various stripe sizes
        for &stripe_size in &stripe_sizes {
            if stripe_size >= blob_size {
                continue; // Skip if stripe >= blob (no benefit)
            }

            let stripe_mb = stripe_size >> 20;
            let (encode_time, num_stripes, stripe_slices) = encode_striped(&mut slicer, &payload, stripe_size);
            let decode_time = decode_striped(&mut slicer, &stripe_slices);

            let stripe_label = if stripe_mb > 0 { stripe_mb } else { 1 };

            results.push(BenchResult {
                blob_size_mb: blob_mb,
                approach: "Striped",
                stripe_size_mb: Some(stripe_label),
                num_stripes,
                encode_time_ms: encode_time.as_secs_f64() * 1000.0,
                decode_time_ms: decode_time.as_secs_f64() * 1000.0,
                peak_mem_mb: estimate_striped_mem(blob_size, stripe_size),
            });
        }
    }

    results
}

/// Print results with analysis.
fn print_results(results: &[BenchResult]) {
    println!();
    println!("Striping Performance Comparison");
    println!("================================");
    println!();
    println!("Parameters: DATA_SLICES={}, CODING_SLICES={}, SLICE_COUNT={}",
             DATA_SLICES, CODING_SLICES, SLICE_COUNT);
    println!();
    println!("{}", BenchResult::header());
    println!("{}", "-".repeat(84));

    let mut current_blob_size = 0;
    for result in results {
        if result.blob_size_mb != current_blob_size {
            if current_blob_size != 0 {
                println!();
            }
            current_blob_size = result.blob_size_mb;
        }
        println!("{}", result.row());
    }

    // Analysis section
    println!();
    println!("Analysis");
    println!("--------");

    for &blob_mb in &[1, 4, 10, 25, 50] {
        let blob_results: Vec<_> = results.iter()
            .filter(|r| r.blob_size_mb == blob_mb)
            .collect();

        if blob_results.is_empty() {
            continue;
        }

        let basic = blob_results.iter().find(|r| r.approach == "Basic");

        if let Some(basic) = basic {
            println!();
            println!("{} MB blob:", blob_mb);

            for striped in blob_results.iter().filter(|r| r.approach == "Striped") {
                let enc_overhead = (striped.encode_time_ms / basic.encode_time_ms - 1.0) * 100.0;
                let dec_overhead = (striped.decode_time_ms / basic.decode_time_ms - 1.0) * 100.0;
                let mem_savings = (1.0 - striped.peak_mem_mb / basic.peak_mem_mb) * 100.0;

                println!(
                    "  {} MB stripe: encode {:+.1}%, decode {:+.1}%, mem {:.1}% less ({} stripes)",
                    striped.stripe_size_mb.unwrap_or(0),
                    enc_overhead,
                    dec_overhead,
                    mem_savings,
                    striped.num_stripes
                );
            }
        }
    }

    println!();
    println!("Notes:");
    println!("- Positive % = striped is slower/uses more");
    println!("- Negative % = striped is faster/uses less");
    println!("- Memory estimates are approximate (actual varies with allocator)");
    println!("- Striped memory advantage increases with blob size");
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Quick sanity test.
    #[test]
    fn test_striping_sanity() {
        let payload = make_payload(1 << 20);
        let mut slicer = BasicSlicer::with_max_slice_bytes(1 << 20);

        let (enc_time, slices) = encode_basic(&mut slicer, &payload);
        assert!(enc_time.as_millis() < 10000, "encoding took too long");

        let dec_time = decode_basic(&mut slicer, &slices);
        assert!(dec_time.as_millis() < 10000, "decoding took too long");

        println!("Sanity check passed: encode={:?}, decode={:?}", enc_time, dec_time);
    }

    /// Main comparison test.
    /// Run with: cargo test -p tape-slicer --release -- --nocapture striping_comparison
    #[test]
    fn striping_comparison() {
        let results = run_benchmarks();
        print_results(&results);
    }
}
