//! StripedSlicer benchmark to produce markdown table results.
//!
//! Run with: cargo test -p tape-slicer --release -- --nocapture striped_bench

use std::time::Instant;

use crate::consts::{DATA_SLICES, SLICE_COUNT};
use crate::striped::StripedSlicer;
use crate::{Blob, Slicer};

fn make_payload(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i % 251) as u8).collect()
}

fn format_size(bytes: usize) -> String {
    if bytes >= 1 << 20 {
        format!("{} MB", bytes >> 20)
    } else {
        format!("{} KB", bytes >> 10)
    }
}

struct BenchRow {
    input_size: usize,
    padded_size: usize,
    encoded_size: usize,
    num_stripes: usize,
    encode_time_ms: f64,
}

impl BenchRow {
    fn overhead(&self) -> f64 {
        self.encoded_size as f64 / self.input_size as f64
    }

    fn throughput_mb_s(&self) -> f64 {
        let input_mb = self.input_size as f64 / (1 << 20) as f64;
        let time_s = self.encode_time_ms / 1000.0;
        if time_s > 0.0 {
            input_mb / time_s
        } else {
            0.0
        }
    }

    fn to_markdown(&self, bold: bool) -> String {
        let b = if bold { "**" } else { "" };
        format!(
            "| {}{}{} | {}{}{} | {}{}{} | {}{}x{} | {} | {} ms | {:.1} MB/s |",
            b,
            format_size(self.input_size),
            b,
            b,
            format_size(self.padded_size),
            b,
            b,
            format_size(self.encoded_size),
            b,
            b,
            format!("{:.2}", self.overhead()),
            b,
            self.num_stripes,
            self.encode_time_ms as u64,
            self.throughput_mb_s()
        )
    }
}

fn run_bench_with_stripe_size(input_sizes: &[usize], stripe_size: usize, iterations: usize) -> Vec<BenchRow> {
    let mut results = Vec::new();

    for &input_size in input_sizes {
        let payload = make_payload(input_size);
        let mut slicer = StripedSlicer::with_stripe_size(stripe_size);

        // Warmup
        let _ = slicer.encode(Blob::from(payload.clone()));

        // Benchmark
        let mut total_time = std::time::Duration::ZERO;
        let mut slices = None;

        for _ in 0..iterations {
            let start = Instant::now();
            slices = Some(slicer.encode(Blob::from(payload.clone())).unwrap());
            total_time += start.elapsed();
        }

        let slices = slices.unwrap();
        let avg_time_ms = total_time.as_secs_f64() * 1000.0 / iterations as f64;

        // Calculate sizes
        let num_stripes = (input_size + stripe_size - 1).max(1) / stripe_size.max(1);
        let num_stripes = num_stripes.max(1);

        // Padded size = num_stripes * padded_stripe_size
        let padded_stripe = ((stripe_size + DATA_SLICES - 1) / DATA_SLICES) * DATA_SLICES;
        let padded_size = num_stripes * padded_stripe;

        // Encoded size = total bytes in all slices
        let encoded_size: usize = slices.iter().map(|s| s.data.len()).sum();

        results.push(BenchRow {
            input_size,
            padded_size,
            encoded_size,
            num_stripes,
            encode_time_ms: avg_time_ms,
        });
    }

    results
}

fn print_markdown_table(results: &[BenchRow], stripe_size: usize) {
    println!();
    println!("## StripedSlicer Benchmark Results");
    println!();
    println!("Stripe size: {} KB", stripe_size >> 10);
    println!("RS parameters: {}/{}/{}", DATA_SLICES, SLICE_COUNT - DATA_SLICES, SLICE_COUNT);
    println!();
    println!("| Input Size | Padded Size | Encoded Size | Effective Overhead | Stripes | Encode Time | Throughput |");
    println!("|------------|-------------|--------------|-------------------|---------|-------------|------------|");

    for row in results {
        // Bold rows where input >= padded (efficient encoding)
        let bold = row.input_size >= row.padded_size / 2;
        println!("{}", row.to_markdown(bold));
    }
}

/// File size distribution from real-world data
/// Each entry: (representative_size, file_count, weight_percent)
const FILE_DISTRIBUTION: &[(usize, u64, f64)] = &[
    // 0-1 KB: 28.6% - use 512 bytes as representative
    (512, 103_216, 28.6),
    // 1-10 KB: 12.1% - use 5 KB as representative
    (5 * 1024, 43_641, 12.1),
    // 10-100 KB: 18.9% - use 50 KB as representative
    (50 * 1024, 68_208, 18.9),
    // 100 KB-1 MB: 30.8% - use 500 KB as representative
    (500 * 1024, 111_181, 30.8),
    // 1-10 MB: 9.0% - use 5 MB as representative
    (5 * 1024 * 1024, 32_555, 9.0),
    // 10-100 MB: 0.7% - use 30 MB as representative
    (30 * 1024 * 1024, 2_392, 0.7),
];

#[derive(Debug)]
struct StripeAnalysis {
    stripe_size: usize,
    total_input_bytes: u64,
    total_encoded_bytes: u64,
    total_encode_time_ms: f64,
    weighted_overhead: f64,
    effective_throughput_mb_s: f64,
    storage_efficiency_score: f64,
}

fn analyze_stripe_size(stripe_size: usize, iterations: usize) -> StripeAnalysis {
    let mut total_input_bytes: u64 = 0;
    let mut total_encoded_bytes: u64 = 0;
    let mut total_encode_time_ms: f64 = 0.0;
    let mut weighted_overhead_sum: f64 = 0.0;
    let mut total_weight: f64 = 0.0;

    for &(file_size, file_count, weight) in FILE_DISTRIBUTION {
        let payload = make_payload(file_size);
        let mut slicer = StripedSlicer::with_stripe_size(stripe_size);

        // Warmup
        let _ = slicer.encode(Blob::from(payload.clone()));

        // Benchmark
        let mut total_time = std::time::Duration::ZERO;
        let mut slices = None;

        for _ in 0..iterations {
            let start = Instant::now();
            slices = Some(slicer.encode(Blob::from(payload.clone())).unwrap());
            total_time += start.elapsed();
        }

        let slices = slices.unwrap();
        let avg_time_ms = total_time.as_secs_f64() * 1000.0 / iterations as f64;
        let encoded_size: usize = slices.iter().map(|s| s.data.len()).sum();

        let overhead = encoded_size as f64 / file_size as f64;

        // Accumulate weighted stats
        total_input_bytes += file_size as u64 * file_count;
        total_encoded_bytes += encoded_size as u64 * file_count;
        total_encode_time_ms += avg_time_ms * file_count as f64;
        weighted_overhead_sum += overhead * weight;
        total_weight += weight;
    }

    let weighted_overhead = weighted_overhead_sum / total_weight;
    let effective_throughput_mb_s = (total_input_bytes as f64 / (1 << 20) as f64)
        / (total_encode_time_ms / 1000.0);

    // Score: lower is better. Combines overhead penalty with throughput bonus.
    // We want low overhead and high throughput.
    // Score = overhead_factor - throughput_bonus
    let storage_efficiency_score = weighted_overhead - (effective_throughput_mb_s / 500.0);

    StripeAnalysis {
        stripe_size,
        total_input_bytes,
        total_encoded_bytes,
        total_encode_time_ms,
        weighted_overhead,
        effective_throughput_mb_s,
        storage_efficiency_score,
    }
}

fn find_optimal_stripe_size() {
    let stripe_sizes = [
        8 * 1024,    // 8 KB
        16 * 1024,   // 16 KB
        32 * 1024,   // 32 KB
        48 * 1024,   // 48 KB
        64 * 1024,   // 64 KB
        96 * 1024,   // 96 KB
        128 * 1024,  // 128 KB
        192 * 1024,  // 192 KB
        256 * 1024,  // 256 KB
        384 * 1024,  // 384 KB
        512 * 1024,  // 512 KB
    ];

    println!();
    println!("# Stripe Size Optimization Analysis");
    println!();
    println!("## File Size Distribution");
    println!();
    println!("| Size Range | Files | Weight |");
    println!("|------------|-------|--------|");
    for &(size, count, weight) in FILE_DISTRIBUTION {
        println!("| {} | {} | {:.1}% |", format_size(size), count, weight);
    }

    println!();
    println!("## Results by Stripe Size");
    println!();
    println!("| Stripe Size | Weighted Overhead | Total Encoded | Throughput | Score |");
    println!("|-------------|-------------------|---------------|------------|-------|");

    let mut results: Vec<StripeAnalysis> = Vec::new();

    for &stripe_size in &stripe_sizes {
        let analysis = analyze_stripe_size(stripe_size, 3);
        println!(
            "| {} | {:.2}x | {} | {:.1} MB/s | {:.2} |",
            format_size(analysis.stripe_size),
            analysis.weighted_overhead,
            format_size(analysis.total_encoded_bytes as usize),
            analysis.effective_throughput_mb_s,
            analysis.storage_efficiency_score
        );
        results.push(analysis);
    }

    // Find best by different criteria
    let best_overhead = results.iter().min_by(|a, b|
        a.weighted_overhead.partial_cmp(&b.weighted_overhead).unwrap()
    ).unwrap();

    let best_throughput = results.iter().max_by(|a, b|
        a.effective_throughput_mb_s.partial_cmp(&b.effective_throughput_mb_s).unwrap()
    ).unwrap();

    let best_score = results.iter().min_by(|a, b|
        a.storage_efficiency_score.partial_cmp(&b.storage_efficiency_score).unwrap()
    ).unwrap();

    println!();
    println!("## Recommendations");
    println!();
    println!("| Criterion | Best Stripe Size | Value |");
    println!("|-----------|------------------|-------|");
    println!("| Lowest Overhead | {} | {:.2}x |",
        format_size(best_overhead.stripe_size), best_overhead.weighted_overhead);
    println!("| Highest Throughput | {} | {:.1} MB/s |",
        format_size(best_throughput.stripe_size), best_throughput.effective_throughput_mb_s);
    println!("| Best Balance (Score) | {} | {:.2} |",
        format_size(best_score.stripe_size), best_score.storage_efficiency_score);

    println!();
    println!("## Per-Bucket Breakdown for Top Candidates");
    println!();

    let candidates = [32 * 1024, 64 * 1024, 128 * 1024];

    println!("| File Size | 32 KB Stripe | 64 KB Stripe | 128 KB Stripe |");
    println!("|-----------|--------------|--------------|---------------|");

    for &(file_size, _, _) in FILE_DISTRIBUTION {
        let mut row = format!("| {} |", format_size(file_size));

        for &stripe_size in &candidates {
            let payload = make_payload(file_size);
            let mut slicer = StripedSlicer::with_stripe_size(stripe_size);
            let slices = slicer.encode(Blob::from(payload)).unwrap();
            let encoded_size: usize = slices.iter().map(|s| s.data.len()).sum();
            let overhead = encoded_size as f64 / file_size as f64;
            row.push_str(&format!(" {:.1}x |", overhead));
        }
        println!("{}", row);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const STRIPE_16KB: usize = 16 * 1024;
    const STRIPE_64KB: usize = 64 * 1024;
    const STRIPE_512KB: usize = 512 * 1024;

    #[test]
    fn stripe_size_optimization() {
        find_optimal_stripe_size();
    }

    #[test]
    fn striped_bench_16kb() {
        let input_sizes = [
            1 << 10,        // 1 KB
            10 << 10,       // 10 KB
            100 << 10,      // 100 KB
            1 << 20,        // 1 MB
            5 << 20,        // 5 MB
            10 << 20,       // 10 MB
            20 << 20,       // 20 MB
            50 << 20,       // 50 MB
        ];

        let results = run_bench_with_stripe_size(&input_sizes, STRIPE_16KB, 3);
        print_markdown_table(&results, STRIPE_16KB);
    }

    #[test]
    fn striped_bench_64kb() {
        let input_sizes = [
            1 << 10,        // 1 KB
            10 << 10,       // 10 KB
            100 << 10,      // 100 KB
            1 << 20,        // 1 MB
            5 << 20,        // 5 MB
            10 << 20,       // 10 MB
            20 << 20,       // 20 MB
            50 << 20,       // 50 MB
        ];

        let results = run_bench_with_stripe_size(&input_sizes, STRIPE_64KB, 3);
        print_markdown_table(&results, STRIPE_64KB);
    }

    #[test]
    fn striped_bench_512kb() {
        let input_sizes = [
            1 << 10,        // 1 KB
            10 << 10,       // 10 KB
            100 << 10,      // 100 KB
            1 << 20,        // 1 MB
            5 << 20,        // 5 MB
            10 << 20,       // 10 MB
            20 << 20,       // 20 MB
            50 << 20,       // 50 MB
        ];

        let results = run_bench_with_stripe_size(&input_sizes, STRIPE_512KB, 3);
        print_markdown_table(&results, STRIPE_512KB);
    }

}
