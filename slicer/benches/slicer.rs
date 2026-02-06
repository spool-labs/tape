//! Slicer benchmarks.

use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use tape_slicer::{ClayCoder, ReedSolomonCoder, Slicer, ErasureCoder};

fn make_data(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i % 251) as u8).collect()
}

fn clay_encode(c: &mut Criterion) {
    let mut group = c.benchmark_group("clay_encode");

    for size in [10_000, 100_000, 1_000_000] {
        let data = make_data(size);
        group.throughput(Throughput::Bytes(size as u64));
        group.bench_function(format!("{size}B"), |b| {
            let mut coder = ClayCoder::new(20, 10, 19);
            b.iter(|| {
                black_box(coder.encode(black_box(&data)).unwrap())
            })
        });
    }

    group.finish();
}

fn clay_decode(c: &mut Criterion) {
    let mut group = c.benchmark_group("clay_decode");

    for size in [10_000, 100_000, 1_000_000] {
        let data = make_data(size);
        let mut coder = ClayCoder::new(20, 10, 19);
        let chunks = coder.encode(&data).unwrap();
        let refs: Vec<(usize, &[u8])> = chunks.iter()
            .enumerate()
            .take(10) // k chunks
            .map(|(i, c)| (i, c.as_slice()))
            .collect();

        group.throughput(Throughput::Bytes(size as u64));
        group.bench_function(format!("{size}B"), |b| {
            b.iter(|| {
                black_box(coder.decode(black_box(&refs)).unwrap())
            })
        });
    }

    group.finish();
}

fn rs_encode(c: &mut Criterion) {
    let mut group = c.benchmark_group("rs_encode");

    // RS with default 4KB max slice handles up to ~40KB
    for size in [1_000, 10_000, 30_000] {
        let data = make_data(size);
        group.throughput(Throughput::Bytes(size as u64));
        group.bench_function(format!("{size}B"), |b| {
            let mut coder = ReedSolomonCoder::new(10, 10);
            b.iter(|| {
                black_box(coder.encode(black_box(&data)).unwrap())
            })
        });
    }

    group.finish();
}

fn rs_decode(c: &mut Criterion) {
    let mut group = c.benchmark_group("rs_decode");

    for size in [1_000, 10_000, 30_000] {
        let data = make_data(size);
        let mut coder = ReedSolomonCoder::new(10, 10);
        let chunks = coder.encode(&data).unwrap();
        let refs: Vec<(usize, &[u8])> = chunks.iter()
            .enumerate()
            .take(10)
            .map(|(i, c)| (i, c.as_slice()))
            .collect();

        group.throughput(Throughput::Bytes(size as u64));
        group.bench_function(format!("{size}B"), |b| {
            b.iter(|| {
                black_box(coder.decode(black_box(&refs)).unwrap())
            })
        });
    }

    group.finish();
}

fn slicer_encode(c: &mut Criterion) {
    let mut group = c.benchmark_group("slicer_encode");

    for size in [100_000, 1_000_000, 10_000_000] {
        let data = make_data(size);
        group.throughput(Throughput::Bytes(size as u64));
        group.bench_function(format!("{size}B"), |b| {
            let mut slicer = Slicer::clay_default();
            b.iter(|| {
                black_box(slicer.encode(black_box(&data)).unwrap())
            })
        });
    }

    group.finish();
}

fn slicer_decode(c: &mut Criterion) {
    let mut group = c.benchmark_group("slicer_decode");

    for size in [100_000, 1_000_000, 10_000_000] {
        let data = make_data(size);
        let mut slicer = Slicer::clay_default();
        let chunks = slicer.encode(&data).unwrap();
        let refs: Vec<(usize, &[u8])> = chunks.iter()
            .enumerate()
            .take(10)
            .map(|(i, c)| (i, c.as_slice()))
            .collect();

        group.throughput(Throughput::Bytes(size as u64));
        group.bench_function(format!("{size}B"), |b| {
            b.iter(|| {
                black_box(slicer.decode(black_box(&refs)).unwrap())
            })
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    clay_encode,
    clay_decode,
    rs_encode,
    rs_decode,
    slicer_encode,
    slicer_decode,
);

criterion_main!(benches);
