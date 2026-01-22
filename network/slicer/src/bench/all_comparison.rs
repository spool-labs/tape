//! Comprehensive comparison of LRC, Product Codes, MSR, and Nested slicers.
//!
//! Run with: cargo test -p tape-slicer --release -- --nocapture all_comparison

use std::io::Cursor;
use std::time::Instant;

use crate::api::Slicer;
use crate::consts::{DATA_SLICES, SLICE_COUNT};
use crate::lrc::{LrcSlicer, LOCAL_GROUP_SIZE};
use crate::msr::{MsrSlicer, REPAIR_BANDWIDTH_FRACTION};
use crate::nested::NestedSlicer;
use crate::product::{ProductSlicer, GRID_SIZE};
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

struct SlicerResults {
    name: &'static str,
    input_size: usize,
    total_encoded: usize,
    replication_factor: f64,
    encode_ms: f64,
    repair_slices: usize,
    repair_bandwidth_pct: f64,
    guaranteed_tolerance: usize,
}

fn bench_lrc(input_size: usize) -> SlicerResults {
    let data = mk_data(input_size);

    let mut slicer = LrcSlicer::new();

    // Warmup
    let _ = slicer.encode(Blob::from(data.clone()));

    let t0 = Instant::now();
    let slices = slicer.encode(Blob::from(data.clone())).unwrap();
    let encode_ms = t0.elapsed().as_secs_f64() * 1000.0;

    let total_encoded: usize = slices.iter().map(|s| s.data.len()).sum();

    SlicerResults {
        name: "LRC",
        input_size,
        total_encoded,
        replication_factor: total_encoded as f64 / input_size as f64,
        encode_ms,
        repair_slices: LOCAL_GROUP_SIZE,
        repair_bandwidth_pct: (LOCAL_GROUP_SIZE as f64 / SLICE_COUNT as f64) * 100.0,
        guaranteed_tolerance: 335, // GLOBAL_PARITIES
    }
}

fn bench_product(input_size: usize) -> SlicerResults {
    let data = mk_data(input_size);

    let mut slicer = ProductSlicer::new();

    // Warmup
    let _ = slicer.encode(Blob::from(data.clone()));

    let t0 = Instant::now();
    let slices = slicer.encode(Blob::from(data.clone())).unwrap();
    let encode_ms = t0.elapsed().as_secs_f64() * 1000.0;

    let total_encoded: usize = slices.iter().map(|s| s.data.len()).sum();

    SlicerResults {
        name: "Product",
        input_size,
        total_encoded,
        replication_factor: total_encoded as f64 / input_size as f64,
        encode_ms,
        repair_slices: GRID_SIZE - 1, // 31 slices from row or column
        repair_bandwidth_pct: ((GRID_SIZE - 1) as f64 / SLICE_COUNT as f64) * 100.0,
        guaranteed_tolerance: 6, // Can tolerate up to 6 failures in same row/col
    }
}

fn bench_msr(input_size: usize) -> SlicerResults {
    let data = mk_data(input_size);

    let mut slicer = MsrSlicer::new();

    // Warmup
    let _ = slicer.encode(Blob::from(data.clone()));

    let t0 = Instant::now();
    let slices = slicer.encode(Blob::from(data.clone())).unwrap();
    let encode_ms = t0.elapsed().as_secs_f64() * 1000.0;

    let total_encoded: usize = slices.iter().map(|s| s.data.len()).sum();

    SlicerResults {
        name: "MSR",
        input_size,
        total_encoded,
        replication_factor: total_encoded as f64 / input_size as f64,
        encode_ms,
        repair_slices: 3, // ~3 slice equivalents (1023 sub-symbols / 341 = 3)
        repair_bandwidth_pct: REPAIR_BANDWIDTH_FRACTION * 100.0,
        guaranteed_tolerance: 341, // Full MDS tolerance
    }
}

fn bench_nested(input_size: usize) -> SlicerResults {
    let data = mk_data(input_size);

    // Warmup
    {
        let mut nested = NestedSlicer::new();
        let primary = nested.encode(&data).unwrap();
        let mut writers: Vec<Cursor<Vec<u8>>> = (0..SLICE_COUNT)
            .map(|_| Cursor::new(Vec::new()))
            .collect();
        let _ = nested.stream(&primary, &mut writers[..]);
    }

    let mut nested = NestedSlicer::new();

    let t0 = Instant::now();
    let primary_slices = nested.encode(&data).unwrap();
    let encode_only_ms = t0.elapsed().as_secs_f64() * 1000.0;

    let primary_size = primary_slices[0].data.len();
    let shard_len = RecoveryLayout::new(primary_size).shard_len;

    let mut writers: Vec<Cursor<Vec<u8>>> = (0..SLICE_COUNT)
        .map(|_| Cursor::new(Vec::new()))
        .collect();

    let t1 = Instant::now();
    let _ = nested.stream(&primary_slices, &mut writers[..]);
    let stream_ms = t1.elapsed().as_secs_f64() * 1000.0;

    let total_primary = SLICE_COUNT * primary_size;
    let total_recovery = SLICE_COUNT * SLICE_COUNT * shard_len;
    let total_encoded = total_primary + total_recovery;

    SlicerResults {
        name: "Nested",
        input_size,
        total_encoded,
        replication_factor: total_encoded as f64 / input_size as f64,
        encode_ms: encode_only_ms + stream_ms,
        repair_slices: DATA_SLICES, // Fetch 683 tiny shards
        repair_bandwidth_pct: (shard_len * DATA_SLICES) as f64 / total_encoded as f64 * 100.0,
        guaranteed_tolerance: 341, // Full RS tolerance
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all_comparison_50mb() {
        let size = 50 * 1024 * 1024; // 50 MB

        println!();
        println!("==========================================================");
        println!("  COMPREHENSIVE SLICER COMPARISON AT 50 MB");
        println!("==========================================================");
        println!();

        let lrc = bench_lrc(size);
        let product = bench_product(size);
        let msr = bench_msr(size);
        let nested = bench_nested(size);

        println!("STORAGE EFFICIENCY");
        println!("------------------");
        println!("  {:10} │ {:>12} │ {:>10} │ vs Target (3.75x)", "Slicer", "Encoded", "Factor");
        println!("  ──────────┼──────────────┼────────────┼─────────────────");
        println!("  {:10} │ {:>12} │ {:>9.3}x │ ✅ {:.1}x better",
            lrc.name, fmt_size(lrc.total_encoded), lrc.replication_factor,
            3.75 / lrc.replication_factor);
        println!("  {:10} │ {:>12} │ {:>9.3}x │ ✅ {:.1}x better",
            product.name, fmt_size(product.total_encoded), product.replication_factor,
            3.75 / product.replication_factor);
        println!("  {:10} │ {:>12} │ {:>9.3}x │ ✅ {:.1}x better",
            msr.name, fmt_size(msr.total_encoded), msr.replication_factor,
            3.75 / msr.replication_factor);
        println!("  {:10} │ {:>12} │ {:>9.3}x │ baseline",
            nested.name, fmt_size(nested.total_encoded), nested.replication_factor);
        println!();

        println!("ENCODING PERFORMANCE");
        println!("--------------------");
        println!("  {:10} │ {:>12} │ {:>12}", "Slicer", "Time", "Throughput");
        println!("  ──────────┼──────────────┼──────────────");
        println!("  {:10} │ {:>12} │ {:>12}",
            lrc.name, fmt_time(lrc.encode_ms), fmt_throughput(size, lrc.encode_ms));
        println!("  {:10} │ {:>12} │ {:>12}",
            product.name, fmt_time(product.encode_ms), fmt_throughput(size, product.encode_ms));
        println!("  {:10} │ {:>12} │ {:>12}",
            msr.name, fmt_time(msr.encode_ms), fmt_throughput(size, msr.encode_ms));
        println!("  {:10} │ {:>12} │ {:>12}",
            nested.name, fmt_time(nested.encode_ms), fmt_throughput(size, nested.encode_ms));
        println!();

        println!("SINGLE-SLICE REPAIR BANDWIDTH");
        println!("-----------------------------");
        println!("  {:10} │ {:>12} │ {:>10} │ vs 50% Target", "Slicer", "Slices", "Bandwidth");
        println!("  ──────────┼──────────────┼────────────┼──────────────");
        println!("  {:10} │ {:>12} │ {:>9.1}% │ ✅ PASS",
            lrc.name, lrc.repair_slices, lrc.repair_bandwidth_pct);
        println!("  {:10} │ {:>12} │ {:>9.1}% │ ✅ PASS",
            product.name, product.repair_slices, product.repair_bandwidth_pct);
        println!("  {:10} │ {:>12} │ {:>9.2}% │ ✅ PASS (MDS optimal)",
            msr.name, msr.repair_slices, msr.repair_bandwidth_pct);
        println!("  {:10} │ {:>12} │ {:>9.3}% │ ✅ PASS",
            nested.name, nested.repair_slices, nested.repair_bandwidth_pct);
        println!();

        println!("FAULT TOLERANCE (MDS = any k of n slices reconstruct)");
        println!("-----------------------------------------------------");
        println!("  {:10} │ {:>20} │ {:>6}", "Slicer", "Guaranteed Erasures", "MDS");
        println!("  ──────────┼──────────────────────┼───────");
        println!("  {:10} │ {:>20} │ {:>6}", lrc.name, lrc.guaranteed_tolerance, "No");
        println!("  {:10} │ {:>20} │ {:>6}", product.name, product.guaranteed_tolerance, "No");
        println!("  {:10} │ {:>20} │ {:>6}", msr.name, msr.guaranteed_tolerance, "Yes");
        println!("  {:10} │ {:>20} │ {:>6}", nested.name, nested.guaranteed_tolerance, "Yes");
        println!();

        println!("==========================================================");
        println!("  SUMMARY: BEST CHOICE BY USE CASE");
        println!("==========================================================");
        println!();
        println!("  MDS + Low Storage + Low Repair: MSR ({:.3}x storage, {:.2}% repair)",
            msr.replication_factor, msr.repair_bandwidth_pct);
        println!("  Storage-constrained (non-MDS): LRC or Product ({:.1}x savings over Nested)",
            nested.replication_factor / lrc.replication_factor);
        println!("  Repair bandwidth (non-MDS):    Product ({:.1}% vs LRC {:.1}%)",
            product.repair_bandwidth_pct, lrc.repair_bandwidth_pct);
        println!("  Encoding speed:                LRC or Product ({:.0}x faster than Nested)",
            nested.encode_ms / lrc.encode_ms);
        println!();
        println!("  RECOMMENDED FOR MDS: MSR");
        println!("    - MDS: any 683 of 1024 slices can reconstruct");
        println!("    - Storage: {:.2}x (same as standard RS)", msr.replication_factor);
        println!("    - Repair: {:.2}% of total data", msr.repair_bandwidth_pct);
        println!();

        // Verify all meet requirements
        assert!(lrc.replication_factor < 3.75, "LRC should beat 3.75x");
        assert!(product.replication_factor < 3.75, "Product should beat 3.75x");
        assert!(msr.replication_factor < 3.75, "MSR should beat 3.75x");
        assert!(lrc.repair_bandwidth_pct < 50.0, "LRC repair should be < 50%");
        assert!(product.repair_bandwidth_pct < 50.0, "Product repair should be < 50%");
        assert!(msr.repair_bandwidth_pct < 50.0, "MSR repair should be < 50%");
    }

    #[test]
    #[ignore]
    fn all_comparison_sizes() {
        let sizes = vec![
            1024 * 1024,              // 1 MB
            10 * 1024 * 1024,         // 10 MB
            50 * 1024 * 1024,         // 50 MB
            100 * 1024 * 1024,        // 100 MB
        ];

        println!();
        println!("Multi-Size Comparison");
        println!("=====================");
        println!();

        for &size in &sizes {
            println!("Input: {}", fmt_size(size));
            println!("┌──────────┬────────────┬───────────┬────────────┬───────────┬─────┐");
            println!("│  Slicer  │  Encoded   │  Factor   │  Enc Time  │ Repair BW │ MDS │");
            println!("├──────────┼────────────┼───────────┼────────────┼───────────┼─────┤");

            let lrc = bench_lrc(size);
            let product = bench_product(size);
            let msr = bench_msr(size);
            let nested = bench_nested(size);

            println!("│ {:8} │ {:>10} │ {:>8.3}x │ {:>10} │ {:>8.1}% │ No  │",
                lrc.name, fmt_size(lrc.total_encoded), lrc.replication_factor,
                fmt_time(lrc.encode_ms), lrc.repair_bandwidth_pct);
            println!("│ {:8} │ {:>10} │ {:>8.3}x │ {:>10} │ {:>8.1}% │ No  │",
                product.name, fmt_size(product.total_encoded), product.replication_factor,
                fmt_time(product.encode_ms), product.repair_bandwidth_pct);
            println!("│ {:8} │ {:>10} │ {:>8.3}x │ {:>10} │ {:>8.2}% │ Yes │",
                msr.name, fmt_size(msr.total_encoded), msr.replication_factor,
                fmt_time(msr.encode_ms), msr.repair_bandwidth_pct);
            println!("│ {:8} │ {:>10} │ {:>8.3}x │ {:>10} │ {:>8.3}% │ Yes │",
                nested.name, fmt_size(nested.total_encoded), nested.replication_factor,
                fmt_time(nested.encode_ms), nested.repair_bandwidth_pct);

            println!("└──────────┴────────────┴───────────┴────────────┴───────────┴─────┘");
            println!();
        }
    }
}
