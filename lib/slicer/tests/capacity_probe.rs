//! Probe: the largest slice any legal track can produce, which is what fixes
//! SUB_TREE_HEIGHT. Run with `cargo test --release -p tape-slicer --test
//! capacity_probe -- --ignored --nocapture`.

use tape_core::erasure::{sub_leaf_count, SUB_LEAF_BYTES, SUB_TREE_HEIGHT};
use tape_slicer::{ErasureCoder, ReedSolomonCoder, Slicer};

const MAX_TRACK_SIZE: usize = 64 * 1024 * 1024;

fn leaves_for(slices: &[Vec<u8>]) -> (usize, usize) {
    let longest = slices.iter().map(Vec::len).max().unwrap();
    (longest, sub_leaf_count(longest))
}

#[test]
#[ignore = "encodes 64 MiB, run explicitly in release"]
fn probe_worst_case_slice() {
    let capacity = 1usize << SUB_TREE_HEIGHT;
    println!("SUB_LEAF_BYTES={SUB_LEAF_BYTES} height={SUB_TREE_HEIGHT} capacity={capacity}");

    let payload = vec![0xA5u8; MAX_TRACK_SIZE];
    let mut clay = Slicer::clay_default();
    let (bytes, leaves) = leaves_for(&clay.encode(&payload).expect("clay encode"));
    println!("clay k=7  max track: slice={bytes} leaves={leaves} fits={}", leaves <= capacity);

    // Reed-Solomon caps its payload well below a max track, so find the real
    // ceiling and the worst slice it can produce at the smallest legal k.
    for k in [1usize, 2, 7, 10] {
        let mut lo = 1usize;
        let mut hi = MAX_TRACK_SIZE;
        while lo < hi {
            let mid = (lo + hi + 1) / 2;
            let mut rs = Slicer::new(ReedSolomonCoder::new(k, 20 - k));
            if rs.encode(&vec![0xA5u8; mid]).is_ok() {
                lo = mid;
            } else {
                hi = mid - 1;
            }
        }
        let mut rs = Slicer::new(ReedSolomonCoder::new(k, 20 - k));
        let (bytes, leaves) = leaves_for(&rs.encode(&vec![0xA5u8; lo]).expect("rs encode"));
        println!(
            "rs   k={k:<3} max payload={lo:<10} slice={bytes:<10} leaves={leaves:<7} fits={}",
            leaves <= capacity
        );
    }
}
