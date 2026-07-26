//! Cost of rebuilding a slice's sub-leaf tree, which whole-slice verification
//! pays on every gateway read and node write. Run with
//! `cargo test --release -p tape-core --test slice_root_bench -- --ignored --nocapture`.

use std::time::Instant;

use tape_core::erasure::{slice_root, sub_leaf_count};

#[test]
#[ignore = "benchmark, run explicitly in release"]
fn slice_root_cost() {
    println!("{:>12} {:>10} {:>10} {:>10}", "slice", "leaves", "ms", "GB/s");

    // 9,724,048 is the measured worst case: one slice of a 64 MiB Clay track.
    for len in [1024 * 1024usize, 9_724_048, 64 * 1024 * 1024] {
        let slice = vec![0xA5u8; len];
        slice_root(&slice).expect("root");

        let reps = (512 * 1024 * 1024 / len).clamp(5, 100);
        let start = Instant::now();
        for _ in 0..reps {
            std::hint::black_box(slice_root(&slice));
        }
        let ms = start.elapsed().as_secs_f64() * 1000.0 / reps as f64;

        println!(
            "{len:>12} {:>10} {ms:>10.2} {:>10.2}",
            sub_leaf_count(len),
            len as f64 / (ms / 1000.0) / 1e9
        );
    }
}
