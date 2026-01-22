//! MSR slicer benchmark across various blob sizes.
//!
//! Run with: cargo test -p tape-slicer --release msr_bench -- --nocapture --ignored

use std::time::Instant;

use crate::api::Slicer;
use crate::consts::{DATA_SLICES, SLICE_COUNT};
use crate::msr::{MsrSlicer, REPAIR_BANDWIDTH_FRACTION};
use crate::types::Blob;

fn mk_data(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i % 251) as u8).collect()
}

fn fmt_size(bytes: usize) -> String {
    if bytes >= 1024 * 1024 * 1024 {
        format!("{:.2} GB", bytes as f64 / (1024.0 * 1024.0 * 1024.0))
    } else if bytes >= 1024 * 1024 {
        format!("{:.2} MB", bytes as f64 / (1024.0 * 1024.0))
    } else if bytes >= 1024 {
        format!("{:.2} KB", bytes as f64 / 1024.0)
    } else {
        format!("{} B", bytes)
    }
}

fn fmt_time(ms: f64) -> String {
    if ms >= 1000.0 {
        format!("{:.2}s", ms / 1000.0)
    } else if ms >= 1.0 {
        format!("{:.1}ms", ms)
    } else {
        format!("{:.3}ms", ms)
    }
}

fn fmt_throughput(bytes: usize, ms: f64) -> String {
    if ms == 0.0 {
        return "-".to_string();
    }
    let bytes_per_sec = bytes as f64 / (ms / 1000.0);
    if bytes_per_sec >= 1024.0 * 1024.0 * 1024.0 {
        format!("{:.2} GB/s", bytes_per_sec / (1024.0 * 1024.0 * 1024.0))
    } else if bytes_per_sec >= 1024.0 * 1024.0 {
        format!("{:.0} MB/s", bytes_per_sec / (1024.0 * 1024.0))
    } else if bytes_per_sec >= 1024.0 {
        format!("{:.0} KB/s", bytes_per_sec / 1024.0)
    } else {
        format!("{:.0} B/s", bytes_per_sec)
    }
}

struct MsrBenchResult {
    input_size: usize,
    total_encoded: usize,
    slice_size: usize,
    replication_factor: f64,
    encode_ms: f64,
    decode_ms: f64,
    decode_erasure_ms: f64,
}

fn bench_msr(input_size: usize) -> MsrBenchResult {
    let data = mk_data(input_size);

    let mut slicer = MsrSlicer::new();

    // Warmup
    let warmup_slices = slicer.encode(Blob::from(data.clone())).unwrap();
    let warmup_opt: [Option<_>; SLICE_COUNT] = std::array::from_fn(|i| Some(warmup_slices[i].clone()));
    let _ = slicer.decode(&warmup_opt);

    // Benchmark encode
    let t0 = Instant::now();
    let slices = slicer.encode(Blob::from(data.clone())).unwrap();
    let encode_ms = t0.elapsed().as_secs_f64() * 1000.0;

    let total_encoded: usize = slices.iter().map(|s| s.data.len()).sum();
    let slice_size = slices[0].data.len();

    // Benchmark decode (all slices present)
    let opt: [Option<_>; SLICE_COUNT] = std::array::from_fn(|i| Some(slices[i].clone()));
    let t1 = Instant::now();
    let _ = slicer.decode(&opt);
    let decode_ms = t1.elapsed().as_secs_f64() * 1000.0;

    // Benchmark decode with erasures (only k slices)
    let mut opt_erasure: [Option<_>; SLICE_COUNT] = std::array::from_fn(|i| Some(slices[i].clone()));
    // Remove all parity slices (keep only data slices)
    for i in DATA_SLICES..SLICE_COUNT {
        opt_erasure[i] = None;
    }
    let t2 = Instant::now();
    let _ = slicer.decode(&opt_erasure);
    let decode_erasure_ms = t2.elapsed().as_secs_f64() * 1000.0;

    MsrBenchResult {
        input_size,
        total_encoded,
        slice_size,
        replication_factor: total_encoded as f64 / input_size as f64,
        encode_ms,
        decode_ms,
        decode_erasure_ms,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[ignore]
    fn msr_bench_all_sizes() {
        let sizes = vec![
            1024,                     // 1 KB
            10 * 1024,                // 10 KB
            100 * 1024,               // 100 KB
            1024 * 1024,              // 1 MB
            10 * 1024 * 1024,         // 10 MB
            32 * 1024 * 1024,         // 32 MB
            100 * 1024 * 1024,        // 100 MB
            500 * 1024 * 1024,        // 500 MB
        ];

        println!();
        println!("╔══════════════════════════════════════════════════════════════════════════════════════════════╗");
        println!("║                              MSR SLICER BENCHMARK                                            ║");
        println!("╠══════════════════════════════════════════════════════════════════════════════════════════════╣");
        println!("║  MDS: any 683 of 1024 slices reconstruct | Storage: ~1.5x | Repair BW: 0.44%                 ║");
        println!("╚══════════════════════════════════════════════════════════════════════════════════════════════╝");
        println!();

        println!("┌───────────┬────────────┬────────────┬─────────┬────────────┬────────────┬────────────┬────────────┐");
        println!("│   Input   │  Encoded   │ Slice Size │ Factor  │ Encode     │ Enc Thru   │ Decode     │ Dec w/Eras │");
        println!("├───────────┼────────────┼────────────┼─────────┼────────────┼────────────┼────────────┼────────────┤");

        for &size in &sizes {
            let result = bench_msr(size);

            println!(
                "│ {:>9} │ {:>10} │ {:>10} │ {:>6.3}x │ {:>10} │ {:>10} │ {:>10} │ {:>10} │",
                fmt_size(result.input_size),
                fmt_size(result.total_encoded),
                fmt_size(result.slice_size),
                result.replication_factor,
                fmt_time(result.encode_ms),
                fmt_throughput(result.input_size, result.encode_ms),
                fmt_time(result.decode_ms),
                fmt_time(result.decode_erasure_ms),
            );
        }

        println!("└───────────┴────────────┴────────────┴─────────┴────────────┴────────────┴────────────┴────────────┘");
        println!();

        println!("Notes:");
        println!("  - Encode: Time to encode blob into 1024 slices");
        println!("  - Decode: Time to decode with all slices present (no reconstruction)");
        println!("  - Dec w/Eras: Time to decode with only {} data slices (max erasures)", DATA_SLICES);
        println!("  - Repair BW: {:.2}% of total data to repair one slice", REPAIR_BANDWIDTH_FRACTION * 100.0);
        println!();
    }

    #[test]
    #[ignore] // Takes too long for regular test runs
    fn msr_bench_quick() {
        // MSR has overhead from sub-symbol structure (ALPHA=341 sub-symbols per slice).
        // Sub-symbol granularity overhead becomes negligible at 50MB+.
        let sizes = vec![
            50 * 1024 * 1024, // 50 MB
        ];

        println!();
        println!("MSR Quick Benchmark");
        println!("-------------------");

        for &size in &sizes {
            let result = bench_msr(size);
            println!(
                "{:>10} -> {:>10} ({:.3}x) | encode: {:>10} ({:>10}) | decode: {:>10}",
                fmt_size(result.input_size),
                fmt_size(result.total_encoded),
                result.replication_factor,
                fmt_time(result.encode_ms),
                fmt_throughput(result.input_size, result.encode_ms),
                fmt_time(result.decode_ms),
            );

            // At 50MB, replication should be close to SLICE_COUNT/DATA_SLICES = 1.499
            assert!(result.replication_factor > 1.4 && result.replication_factor < 1.6,
                "Replication factor {} out of expected range", result.replication_factor);
        }

        println!();
    }
}
