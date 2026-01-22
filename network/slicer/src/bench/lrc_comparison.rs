//! LRC vs Nested comparison benchmark.
//!
//! Run with: cargo test -p tape-slicer --release -- --nocapture lrc_comparison

use std::io::Cursor;
use std::time::Instant;

use crate::api::Slicer;
use crate::consts::{DATA_SLICES, SLICE_COUNT};
use crate::lrc::{LrcSlicer, LOCAL_GROUP_SIZE};
use crate::nested::NestedSlicer;
use crate::recovery::RecoveryLayout;
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
    } else if ms >= 0.01 {
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
    } else {
        format!("{:.0} KB/s", bytes_per_sec / 1024.0)
    }
}

/// Results for LRC slicer.
struct LrcResults {
    input_size: usize,
    total_encoded: usize,
    replication_factor: f64,
    encode_ms: f64,
    decode_ms: f64,
    local_repair_slices: usize,
    local_repair_bandwidth_pct: f64,
}

/// Results for Nested slicer.
struct NestedResults {
    input_size: usize,
    total_encoded: usize,
    replication_factor: f64,
    encode_ms: f64,
    stream_ms: f64,
    repair_shards: usize,
    repair_bandwidth_pct: f64,
}

fn bench_lrc(input_size: usize, iterations: usize) -> LrcResults {
    let data = mk_data(input_size);

    // Warmup
    {
        let mut slicer = LrcSlicer::new();
        let _ = slicer.encode(Blob::from(data.clone()));
    }

    let mut total_encode = std::time::Duration::ZERO;
    let mut total_decode = std::time::Duration::ZERO;
    let mut last_total_encoded = 0;

    for _ in 0..iterations {
        let mut slicer = LrcSlicer::new();

        // Time encode
        let t0 = Instant::now();
        let slices = slicer.encode(Blob::from(data.clone())).unwrap();
        total_encode += t0.elapsed();

        last_total_encoded = slices.iter().map(|s| s.data.len()).sum();

        // Convert to optional for decode
        let opt: [Option<_>; SLICE_COUNT] = std::array::from_fn(|i| Some(slices[i].clone()));

        // Time decode
        let t1 = Instant::now();
        let _ = slicer.decode(&opt).unwrap();
        total_decode += t1.elapsed();
    }

    let encode_ms = total_encode.as_secs_f64() * 1000.0 / iterations as f64;
    let decode_ms = total_decode.as_secs_f64() * 1000.0 / iterations as f64;

    // Local repair fetches LOCAL_GROUP_SIZE slices
    let local_repair_slices = LOCAL_GROUP_SIZE;
    let local_repair_bandwidth_pct = (local_repair_slices as f64 / SLICE_COUNT as f64) * 100.0;

    LrcResults {
        input_size,
        total_encoded: last_total_encoded,
        replication_factor: last_total_encoded as f64 / input_size as f64,
        encode_ms,
        decode_ms,
        local_repair_slices,
        local_repair_bandwidth_pct,
    }
}

fn bench_nested(input_size: usize, iterations: usize) -> NestedResults {
    let data = mk_data(input_size);

    // Warmup
    {
        let mut nested = NestedSlicer::new();
        let primary_slices = nested.encode(&data).unwrap();
        let mut writers: Vec<Cursor<Vec<u8>>> = (0..SLICE_COUNT)
            .map(|_| Cursor::new(Vec::new()))
            .collect();
        let _ = nested.stream(&primary_slices, &mut writers[..]);
    }

    let mut total_encode = std::time::Duration::ZERO;
    let mut total_stream = std::time::Duration::ZERO;
    let mut last_primary_size = 0;
    let mut last_shard_len = 0;

    for _ in 0..iterations {
        let mut nested = NestedSlicer::new();

        // Time outer encode
        let t0 = Instant::now();
        let primary_slices = nested.encode(&data).unwrap();
        total_encode += t0.elapsed();

        last_primary_size = primary_slices[0].data.len();
        last_shard_len = RecoveryLayout::new(last_primary_size).shard_len;

        // Time recovery column streaming
        let mut writers: Vec<Cursor<Vec<u8>>> = (0..SLICE_COUNT)
            .map(|_| Cursor::new(Vec::new()))
            .collect();

        let t1 = Instant::now();
        let _ = nested.stream(&primary_slices, &mut writers[..]);
        total_stream += t1.elapsed();
    }

    let encode_ms = total_encode.as_secs_f64() * 1000.0 / iterations as f64;
    let stream_ms = total_stream.as_secs_f64() * 1000.0 / iterations as f64;

    let total_primary = SLICE_COUNT * last_primary_size;
    let total_recovery = SLICE_COUNT * SLICE_COUNT * last_shard_len;
    let total_encoded = total_primary + total_recovery;

    // Nested repair fetches DATA_SLICES tiny shards
    let repair_shards = DATA_SLICES;
    // Each shard is shard_len bytes, total = shard_len * DATA_SLICES
    // As percentage of total encoded
    let repair_bytes = last_shard_len * DATA_SLICES;
    let repair_bandwidth_pct = (repair_bytes as f64 / total_encoded as f64) * 100.0;

    NestedResults {
        input_size,
        total_encoded,
        replication_factor: total_encoded as f64 / input_size as f64,
        encode_ms,
        stream_ms,
        repair_shards,
        repair_bandwidth_pct,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lrc_comparison_50mb() {
        let size = 50 * 1024 * 1024; // 50 MB
        println!();
        println!("LRC vs Nested Comparison at 50 MB");
        println!("==================================");
        println!();

        let lrc = bench_lrc(size, 1);
        let nested = bench_nested(size, 1);

        println!("Storage:");
        println!("  LRC:    {} encoded ({:.3}x replication)", fmt_size(lrc.total_encoded), lrc.replication_factor);
        println!("  Nested: {} encoded ({:.3}x replication)", fmt_size(nested.total_encoded), nested.replication_factor);
        println!();
        println!("Encoding Time:");
        println!("  LRC:    {} ({} throughput)", fmt_time(lrc.encode_ms), fmt_throughput(size, lrc.encode_ms));
        println!("  Nested: {} + {} stream = {} ({} throughput)",
            fmt_time(nested.encode_ms),
            fmt_time(nested.stream_ms),
            fmt_time(nested.encode_ms + nested.stream_ms),
            fmt_throughput(size, nested.encode_ms + nested.stream_ms)
        );
        println!();
        println!("Single-Slice Repair Bandwidth:");
        println!("  LRC:    {} slices ({:.1}% of total)", lrc.local_repair_slices, lrc.local_repair_bandwidth_pct);
        println!("  Nested: {} shards ({:.3}% of total)", nested.repair_shards, nested.repair_bandwidth_pct);
        println!();
        println!("Winner (Storage): {} ({:.2}x better)",
            if lrc.replication_factor < nested.replication_factor { "LRC" } else { "Nested" },
            if lrc.replication_factor < nested.replication_factor {
                nested.replication_factor / lrc.replication_factor
            } else {
                lrc.replication_factor / nested.replication_factor
            }
        );
        println!();

        // Verify LRC beats 3.75x
        assert!(lrc.replication_factor < 3.75, "LRC should beat 3.75x, got {:.3}x", lrc.replication_factor);
        println!("SUCCESS: LRC ({:.3}x) beats target (3.75x)", lrc.replication_factor);
    }

    #[test]
    #[ignore]
    fn lrc_comparison_full() {
        let sizes = vec![
            1024,                     // 1 KB
            10 * 1024,                // 10 KB
            100 * 1024,               // 100 KB
            1024 * 1024,              // 1 MB
            10 * 1024 * 1024,         // 10 MB
            50 * 1024 * 1024,         // 50 MB (target)
            100 * 1024 * 1024,        // 100 MB
        ];

        println!();
        println!("LRC vs Nested Full Comparison");
        println!("==============================");
        println!();
        println!("Storage Overhead:");
        println!();
        println!("┌───────────┬─────────────┬────────────┬─────────────┬────────────┬────────────┐");
        println!("│   Input   │ LRC Encoded │  LRC Rep.  │ Nested Enc. │ Nested Rep │   Winner   │");
        println!("├───────────┼─────────────┼────────────┼─────────────┼────────────┼────────────┤");

        for &input_size in &sizes {
            let lrc = bench_lrc(input_size, 1);
            let nested = bench_nested(input_size, 1);

            let winner = if lrc.replication_factor < nested.replication_factor {
                format!("LRC ({:.1}x)", nested.replication_factor / lrc.replication_factor)
            } else {
                format!("Nested ({:.1}x)", lrc.replication_factor / nested.replication_factor)
            };

            println!(
                "│ {:>9} │ {:>11} │ {:>9.2}x │ {:>11} │ {:>9.2}x │ {:>10} │",
                fmt_size(lrc.input_size),
                fmt_size(lrc.total_encoded),
                lrc.replication_factor,
                fmt_size(nested.total_encoded),
                nested.replication_factor,
                winner,
            );
        }

        println!("└───────────┴─────────────┴────────────┴─────────────┴────────────┴────────────┘");
        println!();
        println!("Repair Bandwidth:");
        println!();
        println!("┌───────────┬───────────────┬────────────────┬───────────────────────────────┐");
        println!("│   Input   │  LRC Repair   │ Nested Repair  │            Notes              │");
        println!("├───────────┼───────────────┼────────────────┼───────────────────────────────┤");

        for &input_size in &sizes {
            let lrc = bench_lrc(input_size, 1);
            let nested = bench_nested(input_size, 1);

            println!(
                "│ {:>9} │ {:>4} ({:>5.1}%) │ {:>4} ({:>6.3}%) │ LRC fetches more per repair   │",
                fmt_size(input_size),
                lrc.local_repair_slices,
                lrc.local_repair_bandwidth_pct,
                nested.repair_shards,
                nested.repair_bandwidth_pct,
            );
        }

        println!("└───────────┴───────────────┴────────────────┴───────────────────────────────┘");
        println!();
        println!("Key Insight:");
        println!("  - LRC has ~2.5x BETTER storage efficiency");
        println!("  - Nested has ~80x BETTER repair bandwidth");
        println!("  - Choice depends on: storage cost vs repair frequency");
        println!();
    }

    #[test]
    #[ignore]
    fn lrc_comparison_encode_times() {
        let sizes = vec![
            1024 * 1024,              // 1 MB
            10 * 1024 * 1024,         // 10 MB
            50 * 1024 * 1024,         // 50 MB
        ];

        println!();
        println!("Encoding Performance Comparison (3 iterations)");
        println!("===============================================");
        println!();
        println!("┌───────────┬────────────────┬────────────────┬────────────────┬────────────────┐");
        println!("│   Input   │   LRC Encode   │ LRC Throughput │ Nested Total   │ Nest Thruput   │");
        println!("├───────────┼────────────────┼────────────────┼────────────────┼────────────────┤");

        for &input_size in &sizes {
            let lrc = bench_lrc(input_size, 3);
            let nested = bench_nested(input_size, 3);
            let nested_total_ms = nested.encode_ms + nested.stream_ms;

            println!(
                "│ {:>9} │ {:>14} │ {:>14} │ {:>14} │ {:>14} │",
                fmt_size(input_size),
                fmt_time(lrc.encode_ms),
                fmt_throughput(input_size, lrc.encode_ms),
                fmt_time(nested_total_ms),
                fmt_throughput(input_size, nested_total_ms),
            );
        }

        println!("└───────────┴────────────────┴────────────────┴────────────────┴────────────────┘");
        println!();
    }
}
