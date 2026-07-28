//! Merkle hashing benchmarks.

use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};

use tape_crypto::hash::{sha256_backend, sha256_lane_width, Hash};
use tape_crypto::merkle::{hash_leaf, hash_leaves, hash_level, hash_pair, root_from_leaf_hashes};

/// Bytes covered by one sample leaf beneath a slice root.
const SUB_LEAF_BYTES: usize = 1024;

/// Height of the tree over one slice's sample leaves.
const SUB_TREE_HEIGHT: usize = 16;

/// Slice sizes worth pricing, up to the largest a slice can be.
const SLICE_SIZES: [(&str, usize); 4] = [
    ("64KiB", 64 * 1024),
    ("1MiB", 1024 * 1024),
    ("16MiB", 16 * 1024 * 1024),
    ("64MiB", 64 * 1024 * 1024),
];

fn slice(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i % 251) as u8).collect()
}

fn nodes(count: usize) -> Vec<Hash> {
    (0..count)
        .map(|i| hash_leaf(&(i as u64).to_le_bytes()))
        .collect()
}

fn serial_leaves(bodies: &[&[u8]]) -> Vec<Hash> {
    bodies.iter().map(|body| hash_leaf(body)).collect()
}

fn serial_level(nodes: &[Hash]) -> Vec<Hash> {
    nodes
        .chunks_exact(2)
        .map(|pair| hash_pair(pair[0], pair[1]))
        .collect()
}

/// Sample-leaf hashing, the term that scales with slice size
fn leaves(c: &mut Criterion) {
    println!(
        "sha256 backend: {} ({} lanes)",
        sha256_backend(),
        sha256_lane_width()
    );

    let mut group = c.benchmark_group("sub_leaves");
    group.sample_size(20);

    for (name, size) in SLICE_SIZES {
        let data = slice(size);
        let bodies: Vec<&[u8]> = data.chunks(SUB_LEAF_BYTES).collect();

        group.throughput(Throughput::Bytes(size as u64));
        group.bench_function(format!("{name}_serial"), |b| {
            b.iter(|| serial_leaves(black_box(&bodies)))
        });
        group.bench_function(format!("{name}_batch"), |b| {
            b.iter(|| hash_leaves(black_box(&bodies)))
        });
    }
    group.finish();
}

/// Interior joins, the term that scales with leaf count
///
/// Both arms return an owned level, so allocation sits on both sides of the
/// ratio. Counts run down to one pair because the top of every tree is short.
fn level(c: &mut Criterion) {
    let mut group = c.benchmark_group("level");
    for pairs in [1usize, 4, 16, 512, 32_768] {
        let layer = nodes(pairs * 2);

        group.throughput(Throughput::Elements(pairs as u64));
        group.bench_function(format!("{pairs}_pairs_serial"), |b| {
            b.iter(|| serial_level(black_box(&layer)))
        });
        group.bench_function(format!("{pairs}_pairs_batch"), |b| {
            b.iter(|| hash_level(black_box(&layer)))
        });
    }
    group.finish();
}

/// A whole slice root, so the two terms appear at real proportions
fn slice_root(c: &mut Criterion) {
    let mut group = c.benchmark_group("slice_root");
    group.sample_size(10);

    for (name, size) in SLICE_SIZES {
        let data = slice(size);
        let bodies: Vec<&[u8]> = data.chunks(SUB_LEAF_BYTES).collect();

        group.throughput(Throughput::Bytes(size as u64));
        group.bench_function(format!("{name}_serial"), |b| {
            b.iter(|| {
                let mut level = serial_leaves(black_box(&bodies));
                for depth in 0..SUB_TREE_HEIGHT {
                    if !level.len().is_multiple_of(2) {
                        level.push(tape_crypto::merkle::empty_subtree_root(depth));
                    }
                    level = serial_level(&level);
                }
                level[0]
            })
        });
        group.bench_function(format!("{name}_batch"), |b| {
            b.iter(|| {
                let hashes = hash_leaves(black_box(&bodies));
                root_from_leaf_hashes::<SUB_TREE_HEIGHT>(&hashes)
            })
        });
    }
    group.finish();
}

criterion_group!(benches, leaves, level, slice_root);
criterion_main!(benches);
