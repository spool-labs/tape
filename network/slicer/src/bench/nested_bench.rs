//! NestedSlicer benchmark to produce markdown table results.
//!
//! Run with: cargo test -p tape-slicer --release -- --nocapture nested_bench

use std::io::Cursor;
use std::time::Instant;

use crate::consts::SLICE_COUNT;
use crate::nested::NestedSlicer;
use crate::recovery::RecoveryLayout;

fn mk_segment(len: usize) -> Vec<u8> {
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
    } else {
        format!("{:.0}ms", ms)
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
    } else {
        format!("{:.0} KB/s", bytes_per_sec / 1024.0)
    }
}

struct BenchRow {
    input_size: usize,
    primary_size: usize,
    total_output: usize,
    encode_ms: f64,
    recovery_ms: f64,
}

impl BenchRow {
    fn overhead(&self) -> f64 {
        self.total_output as f64 / self.input_size as f64
    }

    fn throughput(&self) -> String {
        fmt_throughput(self.input_size, self.encode_ms + self.recovery_ms)
    }
}

fn run_bench(input_size: usize, iterations: usize) -> BenchRow {
    let segment = mk_segment(input_size);

    // Warmup
    {
        let mut nested = NestedSlicer::new();
        let primary_slices = nested.encode(&segment).unwrap();
        let mut writers: Vec<Cursor<Vec<u8>>> = (0..SLICE_COUNT)
            .map(|_| Cursor::new(Vec::new()))
            .collect();
        let _ = nested.stream(&primary_slices, &mut writers[..]);
    }

    let mut total_encode = std::time::Duration::ZERO;
    let mut total_recovery = std::time::Duration::ZERO;
    let mut last_primary_size = 0;
    let mut last_shard_len = 0;

    for _ in 0..iterations {
        let mut nested = NestedSlicer::new();

        // Time outer encode
        let t0 = Instant::now();
        let primary_slices = nested.encode(&segment).unwrap();
        total_encode += t0.elapsed();

        last_primary_size = primary_slices[0].data.len();
        last_shard_len = RecoveryLayout::new(last_primary_size).shard_len;

        // Time recovery column streaming
        let mut writers: Vec<Cursor<Vec<u8>>> = (0..SLICE_COUNT)
            .map(|_| Cursor::new(Vec::new()))
            .collect();

        let t1 = Instant::now();
        let _ = nested.stream(&primary_slices, &mut writers[..]);
        total_recovery += t1.elapsed();
    }

    let encode_ms = total_encode.as_secs_f64() * 1000.0 / iterations as f64;
    let recovery_ms = total_recovery.as_secs_f64() * 1000.0 / iterations as f64;

    let total_primary = SLICE_COUNT * last_primary_size;
    let total_recovery_bytes = SLICE_COUNT * SLICE_COUNT * last_shard_len;
    let total_output = total_primary + total_recovery_bytes;

    BenchRow {
        input_size,
        primary_size: last_primary_size,
        total_output,
        encode_ms,
        recovery_ms,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[ignore]
    fn nested_bench_quick() {
        // Quick sanity check with small sizes
        let sizes = vec![
            1024,             // 1 KB
            10 * 1024,        // 10 KB
            100 * 1024,       // 100 KB
            1024 * 1024,      // 1 MB
        ];

        println!();
        println!("NestedSlicer Benchmark (quick, 1 iteration)");
        println!();
        print_table(&sizes, 1);
    }

    #[test]
    #[ignore]
    fn nested_bench_full() {
        // Full benchmark with realistic sizes and multiple iterations
        let sizes = vec![
            1024,                     // 1 KB
            10 * 1024,                // 10 KB
            100 * 1024,               // 100 KB
            1024 * 1024,              // 1 MB
            10 * 1024 * 1024,         // 10 MB
            32 * 1024 * 1024,         // 32 MB (MIN_SEGMENT_SIZE)
            100 * 1024 * 1024,        // 100 MB
            1024 * 1024 * 1024,       // 1 GB
        ];

        println!();
        println!("NestedSlicer Benchmark (full, 3 iterations each)");
        println!();
        print_table(&sizes, 3);
    }

    fn print_table(sizes: &[usize], iterations: usize) {
        println!("┌───────────┬────────────┬────────────┬────────────┬───────────┬────────────┬────────────┐");
        println!("│   Input   │  Primary   │ Total Out  │  Overhead  │  Encode   │  Recovery  │ Throughput │");
        println!("├───────────┼────────────┼────────────┼────────────┼───────────┼────────────┼────────────┤");

        for &input_size in sizes {
            let row = run_bench(input_size, iterations);
            println!(
                "│ {:>9} │ {:>10} │ {:>10} │ {:>9.2}x │ {:>9} │ {:>10} │ {:>10} │",
                fmt_size(row.input_size),
                fmt_size(row.primary_size),
                fmt_size(row.total_output),
                row.overhead(),
                fmt_time(row.encode_ms),
                fmt_time(row.recovery_ms),
                row.throughput(),
            );
        }

        println!("└───────────┴────────────┴────────────┴────────────┴───────────┴────────────┴────────────┘");
        println!();
        println!("  - Encode: time for outer RotatedSlicer encoding (segment -> 1024 primaries)");
        println!("  - Recovery: time to stream all 1024 recovery columns (1024x1024 matrix)");
        println!("  - Throughput: input bytes / total time");
        println!();
    }
}
