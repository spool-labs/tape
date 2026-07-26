//! Probe: the largest slice any legal track can produce, which is what fixes
//! SUB_TREE_HEIGHT. Run with `cargo test --release -p tape-slicer --test
//! capacity_probe -- --ignored --nocapture`.

use tape_core::erasure::{sub_leaf_count, slice_root, SUB_LEAF_BYTES, SUB_TREE_HEIGHT};
use tape_slicer::{ErasureCoder, ReedSolomonCoder, Slicer};

const MAX_TRACK_SIZE: usize = 64 * 1024 * 1024;

fn report(label: &str, k: usize, slices: &[Vec<u8>]) {
    let bytes = slices.iter().map(Vec::len).max().unwrap();
    let leaves = sub_leaf_count(bytes);
    let needed = leaves.next_power_of_two().ilog2();

    // The height only holds if every slice actually roots, so assert rather
    // than leave a printed table for a human to check.
    assert!(
        slices.iter().all(|slice| slice_root(slice).is_some()),
        "{label} k={k} exceeds the sub-leaf tree at height {SUB_TREE_HEIGHT}"
    );

    println!("{label:<16} k={k:<3} slice={bytes:<10} leaves={leaves:<7} needs_height={needed}");
}

#[test]
#[ignore = "encodes 64 MiB repeatedly, run explicitly in release"]
fn probe_worst_case_slice() {
    println!(
        "SUB_LEAF_BYTES={SUB_LEAF_BYTES} height={SUB_TREE_HEIGHT} capacity={}",
        1usize << SUB_TREE_HEIGHT
    );

    let payload = vec![0xA5u8; MAX_TRACK_SIZE];

    let mut clay = Slicer::clay_default();
    let clay_k = clay.k();
    report("clay striped", clay_k, &clay.encode(&payload).expect("clay"));

    // Bare coder, which is how the SDK's Basic path uses it. k=1 is replication,
    // so one slice carries the whole track and sets the height.
    for k in [1usize, 2, 3, 4, 5, 7, 10, 16] {
        let mut rs = ReedSolomonCoder::new(k, 20 - k);
        report("rs bare", k, &rs.encode(&payload).expect("rs"));
    }
}
