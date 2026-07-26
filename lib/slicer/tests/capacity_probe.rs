//! Probe: the largest slice any legal track can produce, which is what fixes
//! SUB_TREE_HEIGHT. Run with `cargo test --release -p tape-slicer --test
//! capacity_probe -- --ignored --nocapture`.

use tape_core::erasure::{sub_leaf_count, SUB_LEAF_BYTES, SUB_TREE_HEIGHT};
use tape_slicer::{ErasureCoder, ReedSolomonCoder, Slicer};

const MAX_TRACK_SIZE: usize = 64 * 1024 * 1024;

fn report(label: &str, k: usize, slices: &[Vec<u8>], capacity: usize) {
    let bytes = slices.iter().map(Vec::len).max().unwrap();
    let leaves = sub_leaf_count(bytes);
    let needed = (usize::BITS - (leaves - 1).leading_zeros()) as usize;
    println!(
        "{label:<16} k={k:<3} slice={bytes:<10} leaves={leaves:<7} \
         needs_height={needed:<3} fits_{SUB_TREE_HEIGHT}={}",
        leaves <= capacity
    );
}

#[test]
#[ignore = "encodes 64 MiB repeatedly, run explicitly in release"]
fn probe_worst_case_slice() {
    let capacity = 1usize << SUB_TREE_HEIGHT;
    println!("SUB_LEAF_BYTES={SUB_LEAF_BYTES} height={SUB_TREE_HEIGHT} capacity={capacity}");

    let payload = vec![0xA5u8; MAX_TRACK_SIZE];

    let mut clay = Slicer::clay_default();
    report("clay striped", 7, &clay.encode(&payload).expect("clay"), capacity);

    // Bare coder, which is how the SDK's Basic path uses it.
    for k in [1usize, 2, 3, 4, 5, 7, 10, 16] {
        let mut rs = ReedSolomonCoder::new(k, 20 - k);
        match rs.encode(&payload) {
            Ok(slices) => report("rs bare", k, &slices, capacity),
            Err(e) => println!("rs bare          k={k:<3} encode failed: {e:?}"),
        }
    }
}
