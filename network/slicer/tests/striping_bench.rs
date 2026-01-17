//! Benchmark comparing Reed-Solomon encoding performance with and without striping.
//!
//! This benchmark compares two approaches:
//! - **Basic**: Single RS pass over the entire blob
//! - **Striped**: Multiple RS passes over fixed-size stripes (1MB, 4MB, 16MB)
//!
//! Run with: cargo test -p tape-slicer --release -- --nocapture striping

use std::time::{Duration, Instant};
use tape_slicer::reed_solomon::ReedSolomonCoder;
use tape_slicer::{DATA_SLICES, CODING_SLICES, SLICE_COUNT, Slice, SliceIndex};

/// Stripe sizes to test (in bytes).
const STRIPE_SIZES: [usize; 3] = [
    1 << 20,  // 1 MB
    4 << 20,  // 4 MB
    16 << 20, // 16 MB
];

/// Blob sizes to test (in bytes).
const BLOB_SIZES: [usize; 4] = [
    10 << 20,  // 10 MB
    50 << 20,  // 50 MB
    100 << 20, // 100 MB
    250 << 20, // 250 MB
];

/// Number of iterations for timing (fewer for large blobs).
fn iterations_for_size(blob_size: usize) -> usize {
    if blob_size >= 100 << 20 {
        1
    } else if blob_size >= 50 << 20 {
        2
    } else {
        3
    }
}

/// Generate deterministic test data.
fn make_payload(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i % 251) as u8).collect()
}

/// Basic encoding: single RS pass over entire blob.
fn encode_basic(coder: &mut ReedSolomonCoder, payload: &[u8]) -> (Duration, usize) {
    let start = Instant::now();
    let raw = coder.encode(payload).expect("encode should succeed");
    let elapsed = start.elapsed();

    // Approximate memory: input + all output slices
    let slice_size = raw.data.first().map(|d| d.len()).unwrap_or(0);
    let mem = payload.len() + (SLICE_COUNT * slice_size);

    (elapsed, mem)
}

/// Striped encoding: multiple RS passes over fixed-size stripes.
/// Each stripe produces SLICE_COUNT slices. Final output interleaves stripe slices.
fn encode_striped(
    coder: &mut ReedSolomonCoder,
    payload: &[u8],
    stripe_size: usize,
) -> (Duration, usize, usize) {
    let start = Instant::now();

    let num_stripes = (payload.len() + stripe_size - 1) / stripe_size;
    let mut all_slices: Vec<Vec<Vec<u8>>> = vec![Vec::new(); SLICE_COUNT];
    let mut max_slice_size = 0usize;

    for stripe_idx in 0..num_stripes {
        let stripe_start = stripe_idx * stripe_size;
        let stripe_end = (stripe_start + stripe_size).min(payload.len());
        let stripe_data = &payload[stripe_start..stripe_end];

        let raw = coder.encode(stripe_data).expect("encode stripe should succeed");

        // Track slice size for this stripe
        if let Some(d) = raw.data.first() {
            max_slice_size = max_slice_size.max(d.len());
        }

        // Collect data slices
        for (i, data) in raw.data.into_iter().enumerate() {
            all_slices[i].push(data);
        }
        // Collect coding slices
        for (j, coding) in raw.coding.into_iter().enumerate() {
            all_slices[DATA_SLICES + j].push(coding);
        }
    }

    // Concatenate stripe parts into final slices
    let final_slices: Vec<Vec<u8>> = all_slices
        .into_iter()
        .map(|parts| parts.concat())
        .collect();

    let elapsed = start.elapsed();

    // Memory estimate: one stripe in memory at a time + accumulating output
    let stripe_mem = stripe_size + (SLICE_COUNT * max_slice_size);
    let output_mem: usize = final_slices.iter().map(|s| s.len()).sum();
    let peak_mem = stripe_mem + output_mem; // Approximation

    (elapsed, peak_mem, num_stripes)
}

/// Basic decoding: reconstruct from all slices.
fn decode_basic(coder: &mut ReedSolomonCoder, raw_data: &[Vec<u8>], raw_coding: &[Vec<u8>]) -> Duration {
    // Build the slice array
    let mut slices: [Option<Slice>; SLICE_COUNT] = std::array::from_fn(|_| None);

    for (i, data) in raw_data.iter().enumerate() {
        slices[i] = Some(Slice {
            index: SliceIndex::new(i).unwrap(),
            data: data.clone(),
        });
    }
    for (j, coding) in raw_coding.iter().enumerate() {
        let idx = DATA_SLICES + j;
        slices[idx] = Some(Slice {
            index: SliceIndex::new(idx).unwrap(),
            data: coding.clone(),
        });
    }

    let start = Instant::now();
    let _decoded = coder.decode(&slices).expect("decode should succeed");
    start.elapsed()
}

/// Striped decoding: decode each stripe separately then concatenate.
/// Note: This measures the decode time only, not the slice extraction overhead.
fn decode_striped(
    coder: &mut ReedSolomonCoder,
    stripe_slices_list: &[Vec<[Option<Slice>; SLICE_COUNT]>],
) -> Duration {
    let start = Instant::now();

    let mut decoded_data = Vec::new();

    for stripe_slices in stripe_slices_list {
        for slices in stripe_slices {
            let decoded = coder.decode(slices).expect("decode stripe should succeed");
            decoded_data.extend_from_slice(&decoded);
        }
    }

    start.elapsed()
}

/// Build stripe slices for striped decoding.
fn build_stripe_slices_for_decode(
    coder: &mut ReedSolomonCoder,
    payload: &[u8],
    stripe_size: usize,
) -> Vec<[Option<Slice>; SLICE_COUNT]> {
    let num_stripes = (payload.len() + stripe_size - 1) / stripe_size;
    let mut result = Vec::with_capacity(num_stripes);

    for stripe_idx in 0..num_stripes {
        let stripe_start = stripe_idx * stripe_size;
        let stripe_end = (stripe_start + stripe_size).min(payload.len());
        let stripe_data = &payload[stripe_start..stripe_end];

        let raw = coder.encode(stripe_data).expect("encode stripe");

        // Build slice array for this stripe
        let mut slices: [Option<Slice>; SLICE_COUNT] = std::array::from_fn(|_| None);

        for (i, data) in raw.data.into_iter().enumerate() {
            slices[i] = Some(Slice {
                index: SliceIndex::new(i).unwrap(),
                data,
            });
        }
        for (j, coding) in raw.coding.into_iter().enumerate() {
            let idx = DATA_SLICES + j;
            slices[idx] = Some(Slice {
                index: SliceIndex::new(idx).unwrap(),
                data: coding,
            });
        }

        result.push(slices);
    }

    result
}

/// Result for one benchmark run.
#[derive(Clone)]
struct BenchResult {
    approach: String,
    blob_size_mb: usize,
    stripe_size_mb: Option<usize>,
    encode_time_ms: f64,
    decode_time_ms: f64,
    peak_mem_mb: f64,
    num_stripes: usize,
}

impl BenchResult {
    fn header() -> String {
        format!(
            "{:<12} {:>10} {:>12} {:>12} {:>12} {:>10} {:>8}",
            "Approach", "Blob(MB)", "Stripe(MB)", "Encode(ms)", "Decode(ms)", "Mem(MB)", "Stripes"
        )
    }

    fn row(&self) -> String {
        let stripe_str = self.stripe_size_mb
            .map(|s| format!("{}", s))
            .unwrap_or_else(|| "-".to_string());
        format!(
            "{:<12} {:>10} {:>12} {:>12.1} {:>12.1} {:>10.1} {:>8}",
            self.approach,
            self.blob_size_mb,
            stripe_str,
            self.encode_time_ms,
            self.decode_time_ms,
            self.peak_mem_mb,
            self.num_stripes,
        )
    }
}

/// Run all benchmarks and return results.
fn run_benchmarks() -> Vec<BenchResult> {
    let mut results = Vec::new();

    // Create a coder with enough capacity for our largest blobs
    // For 250MB blob with 683 data slices, each slice is ~375KB
    // We need to handle this in one pass for basic encoding
    let max_slice_bytes = 1 << 20; // 1 MB per slice should be plenty
    let mut coder = ReedSolomonCoder::with_max_slice_bytes(DATA_SLICES, CODING_SLICES, max_slice_bytes);

    for &blob_size in &BLOB_SIZES {
        let blob_size_mb = blob_size >> 20;
        let iterations = iterations_for_size(blob_size);

        println!("Testing blob size: {} MB ({} iterations)", blob_size_mb, iterations);

        let payload = make_payload(blob_size);

        // Basic approach
        {
            let mut total_encode = Duration::ZERO;
            let mut total_decode = Duration::ZERO;
            let mut peak_mem = 0usize;

            for _ in 0..iterations {
                let (enc_time, mem) = encode_basic(&mut coder, &payload);
                total_encode += enc_time;
                peak_mem = peak_mem.max(mem);

                // For decode timing, we need the slices
                let raw = coder.encode(&payload).expect("encode");
                let dec_time = decode_basic(&mut coder, &raw.data, &raw.coding);
                total_decode += dec_time;
            }

            results.push(BenchResult {
                approach: "Basic".to_string(),
                blob_size_mb,
                stripe_size_mb: None,
                encode_time_ms: total_encode.as_secs_f64() * 1000.0 / iterations as f64,
                decode_time_ms: total_decode.as_secs_f64() * 1000.0 / iterations as f64,
                peak_mem_mb: peak_mem as f64 / (1 << 20) as f64,
                num_stripes: 1,
            });
        }

        // Striped approaches
        for &stripe_size in &STRIPE_SIZES {
            // Skip if stripe size is larger than blob
            if stripe_size > blob_size {
                continue;
            }

            let stripe_size_mb = stripe_size >> 20;
            let mut total_encode = Duration::ZERO;
            let mut total_decode = Duration::ZERO;
            let mut peak_mem = 0usize;
            let mut final_num_stripes = 0usize;

            for _ in 0..iterations {
                let (enc_time, mem, stripes) = encode_striped(&mut coder, &payload, stripe_size);
                total_encode += enc_time;
                peak_mem = peak_mem.max(mem);
                final_num_stripes = stripes;
            }

            // Build stripe slices for decoding (this is done outside timing)
            let stripe_slices = build_stripe_slices_for_decode(&mut coder, &payload, stripe_size);

            for _ in 0..iterations {
                let dec_time = decode_striped(&mut coder, &[stripe_slices.clone()]);
                total_decode += dec_time;
            }

            results.push(BenchResult {
                approach: "Striped".to_string(),
                blob_size_mb,
                stripe_size_mb: Some(stripe_size_mb),
                encode_time_ms: total_encode.as_secs_f64() * 1000.0 / iterations as f64,
                decode_time_ms: total_decode.as_secs_f64() * 1000.0 / iterations as f64,
                peak_mem_mb: peak_mem as f64 / (1 << 20) as f64,
                num_stripes: final_num_stripes,
            });
        }
    }

    results
}

/// Print results as a formatted table with analysis.
fn print_results(results: &[BenchResult]) {
    println!();
    println!("Reed-Solomon Encoding Benchmark: Basic vs Striped");
    println!("==================================================");
    println!("DATA_SLICES={}, CODING_SLICES={}, SLICE_COUNT={}", DATA_SLICES, CODING_SLICES, SLICE_COUNT);
    println!();
    println!("{}", BenchResult::header());
    println!("{}", "-".repeat(88));

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

    // Group by blob size and compare
    for &blob_size in &[10, 50, 100, 250] {
        let blob_results: Vec<_> = results.iter()
            .filter(|r| r.blob_size_mb == blob_size)
            .collect();

        if blob_results.is_empty() {
            continue;
        }

        let basic = blob_results.iter().find(|r| r.approach == "Basic");

        if let Some(basic) = basic {
            println!();
            println!("{} MB blob:", blob_size);

            for striped in blob_results.iter().filter(|r| r.approach == "Striped") {
                let enc_overhead = (striped.encode_time_ms / basic.encode_time_ms - 1.0) * 100.0;
                let dec_overhead = (striped.decode_time_ms / basic.decode_time_ms - 1.0) * 100.0;
                let mem_ratio = striped.peak_mem_mb / basic.peak_mem_mb;

                println!(
                    "  {} MB stripe: encode {:+.1}%, decode {:+.1}%, mem {:.2}x ({} stripes)",
                    striped.stripe_size_mb.unwrap_or(0),
                    enc_overhead,
                    dec_overhead,
                    mem_ratio,
                    striped.num_stripes
                );
            }
        }
    }

    println!();
    println!("Notes:");
    println!("- Positive overhead % means striped is slower than basic");
    println!("- Memory estimates are approximate (input + working buffers + output)");
    println!("- Striped approach processes one stripe at a time, potentially better cache locality");
}

/// Quick sanity test to verify the benchmark code works.
#[test]
fn test_striping_sanity() {
    let payload = make_payload(1 << 20); // 1 MB
    let mut coder = ReedSolomonCoder::with_max_slice_bytes(DATA_SLICES, CODING_SLICES, 1 << 20);

    // Basic encode/decode
    let (enc_time, _mem) = encode_basic(&mut coder, &payload);
    assert!(enc_time.as_millis() < 10000, "encoding took too long");

    let raw = coder.encode(&payload).expect("encode");
    let dec_time = decode_basic(&mut coder, &raw.data, &raw.coding);
    assert!(dec_time.as_millis() < 10000, "decoding took too long");

    println!("Sanity check passed: encode={:?}, decode={:?}", enc_time, dec_time);
}

/// Main comparison test - prints the full benchmark table.
/// Run with: cargo test -p tape-slicer --release -- --nocapture --ignored striping_comparison
#[test]
#[ignore] // Ignore by default since it takes a while
fn striping_comparison() {
    let results = run_benchmarks();
    print_results(&results);
}

/// Quick comparison with smaller sizes for CI.
/// Run with: cargo test -p tape-slicer --release -- --nocapture striping_quick
#[test]
fn striping_quick() {
    println!();
    println!("Quick Striping Comparison (smaller sizes for fast feedback)");
    println!("============================================================");

    let blob_sizes = [1 << 20, 4 << 20, 10 << 20]; // 1MB, 4MB, 10MB
    let stripe_sizes = [256 << 10, 1 << 20]; // 256KB, 1MB

    let mut coder = ReedSolomonCoder::with_max_slice_bytes(DATA_SLICES, CODING_SLICES, 1 << 20);

    println!();
    println!("{:<10} {:>12} {:>12} {:>12} {:>12}",
        "Blob(MB)", "Approach", "Stripe", "Encode(ms)", "Decode(ms)");
    println!("{}", "-".repeat(60));

    for &blob_size in &blob_sizes {
        let blob_mb = blob_size >> 20;
        let payload = make_payload(blob_size);

        // Basic
        let (enc_time, _) = encode_basic(&mut coder, &payload);
        let raw = coder.encode(&payload).expect("encode");
        let dec_time = decode_basic(&mut coder, &raw.data, &raw.coding);

        println!("{:<10} {:>12} {:>12} {:>12.2} {:>12.2}",
            blob_mb, "Basic", "-",
            enc_time.as_secs_f64() * 1000.0,
            dec_time.as_secs_f64() * 1000.0);

        // Striped
        for &stripe_size in &stripe_sizes {
            if stripe_size >= blob_size {
                continue;
            }

            let stripe_kb = stripe_size >> 10;
            let (enc_time, _, _num_stripes) = encode_striped(&mut coder, &payload, stripe_size);

            // Build stripe slices for decode
            let stripe_slices = build_stripe_slices_for_decode(&mut coder, &payload, stripe_size);
            let dec_time = decode_striped(&mut coder, &[stripe_slices]);

            let stripe_str = if stripe_kb >= 1024 {
                format!("{}MB", stripe_kb >> 10)
            } else {
                format!("{}KB", stripe_kb)
            };

            println!("{:<10} {:>12} {:>12} {:>12.2} {:>12.2}",
                "", "Striped", stripe_str,
                enc_time.as_secs_f64() * 1000.0,
                dec_time.as_secs_f64() * 1000.0);
        }
        println!();
    }

    println!("Quick comparison complete.");
}
