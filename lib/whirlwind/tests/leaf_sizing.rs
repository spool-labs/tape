//! Sizing evidence for the sub-leaf: 1 KiB at height 16 against 4 KiB at 14.
//!
//! Each height is the one its leaf size needs to cover a 64 MiB track held in a
//! single slice, which is Reed-Solomon at k=1 and the case the on-chain height
//! has to survive. The timings are taken on the Clay k=7 worst case instead,
//! since that is the largest slice the default profile produces and where the
//! per-track cost actually lands. Both candidates are asserted to cover it and to
//! round-trip a proof, using the real merkle code and the shipped response shape.
//! Run with nocapture to read the timings.

use std::time::Instant;

use tape_crypto::hash::{hashv, Hash};
use tape_crypto::merkle::{compute_path, create_proof_from_leaf_hashes, hash_leaf, root_from_leaf_hashes};

/// Sub-leaves a 64 MiB track puts in one Clay k=7 slice at 1 KiB, measured with
/// the commitment probe at that payload size. It moved from 9,497 when the
/// slicer started deriving stripe sizes instead of picking them off a ladder.
const MAX_TRACK_SUB_LEAVES_1K: usize = 9364;
const SLICE_BYTES: usize = MAX_TRACK_SUB_LEAVES_1K * 1024;
const SLICES_PER_TRACK: usize = 20;
const SLICE_TREE_HEIGHT: usize = 5;

fn slice_bytes() -> Vec<u8> {
    let mut data = vec![0u8; SLICE_BYTES];
    let mut state: u64 = 0x9E37_79B9_7F4A_7C15;
    for byte in data.iter_mut() {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        *byte = (state >> 24) as u8;
    }
    data
}

fn micros(start: Instant) -> f64 {
    start.elapsed().as_nanos() as f64 / 1_000.0
}

/// One configuration's timings, all on a single slice unless noted.
fn measure<const HEIGHT: usize>(label: &str, slice: &[u8], leaf_bytes: usize) {
    let chunks: Vec<&[u8]> = slice.chunks(leaf_bytes).collect();
    assert!(
        chunks.len() <= 1 << HEIGHT,
        "{label}: {} leaves over capacity {}",
        chunks.len(),
        1 << HEIGHT
    );

    // Write side: hash every sub-leaf, then build the per-slice tree.
    let start = Instant::now();
    let hashes: Vec<Hash> = chunks.iter().map(|chunk| hash_leaf(chunk)).collect();
    let leaf_hash_us = micros(start);

    let start = Instant::now();
    let slice_root = root_from_leaf_hashes::<HEIGHT>(&hashes);
    let tree_us = micros(start);

    // Top tree over the slice roots, built once per track.
    let roots = vec![slice_root; SLICES_PER_TRACK];
    let start = Instant::now();
    let commitment = root_from_leaf_hashes::<SLICE_TREE_HEIGHT>(&roots);
    let top_us = micros(start);

    // Round side: produce a proof for one sampled sub-leaf.
    let index = chunks.len() / 3;
    let start = Instant::now();
    let sub_proof = create_proof_from_leaf_hashes::<HEIGHT>(&hashes, index).unwrap();
    let top_proof = create_proof_from_leaf_hashes::<SLICE_TREE_HEIGHT>(&roots, 0).unwrap();
    let prove_us = micros(start);

    // Round side: verify it, the work every observer repeats.
    let start = Instant::now();
    let leaf = hash_leaf(chunks[index]);
    let reached_root = *compute_path(&sub_proof, leaf, index as u64, HEIGHT).last().unwrap();
    let reached_commitment = *compute_path(&top_proof, reached_root, 0, SLICE_TREE_HEIGHT).last().unwrap();
    let verify_us = micros(start);
    assert_eq!(reached_root, slice_root);
    assert_eq!(reached_commitment, commitment);

    // The shipped response is the leaf and its path to the slice root. It carries
    // neither the root nor the top path, because a verifier holds the encoding.
    let response = leaf_bytes + sub_proof.len() * Hash::LEN;
    let track_build_ms = (leaf_hash_us + tree_us) * SLICES_PER_TRACK as f64 / 1_000.0 + top_us / 1_000.0;

    println!("{label}");
    println!("  sub-leaves per slice   {}", chunks.len());
    println!("  leaf hashing           {leaf_hash_us:.0} us per slice");
    println!("  slice tree build       {tree_us:.0} us per slice");
    println!("  top tree build         {top_us:.1} us per track");
    println!("  whole track commitment {track_build_ms:.1} ms for {SLICES_PER_TRACK} slices");
    println!("  prove one sub-leaf     {prove_us:.1} us");
    println!("  verify one sub-leaf    {verify_us:.1} us");
    println!("  response on the wire   {response} B");
    println!();
}

// both candidate leaf sizes cover the largest legal track and prove a sub-leaf
#[test]
fn leaf_sizes() {
    let slice = slice_bytes();
    println!();
    println!(
        "one slice of the largest legal track: {} B ({:.2} MiB), sha256 over {} slices per track",
        SLICE_BYTES,
        SLICE_BYTES as f64 / (1024.0 * 1024.0),
        SLICES_PER_TRACK,
    );
    println!();
    measure::<16>("1 KiB leaves, height 16", &slice, 1024);
    measure::<14>("4 KiB leaves, height 14", &slice, 4096);

    // A single node hash for reference, the unit both trees are built from.
    let a = hashv(&[b"a"]);
    let start = Instant::now();
    let rounds = 100_000;
    for _ in 0..rounds {
        std::hint::black_box(hashv(&[a.as_ref(), a.as_ref()]));
    }
    println!("one 64 B node hash     {:.3} us", micros(start) / rounds as f64);
}
