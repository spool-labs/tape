//! Probe: is tape-reed-solomon competitive at the outer coding shape (17 of 50)?
//! Decides whether outer.rs can drop reed-solomon-simd. Run with
//! `cargo test --release -p tape-slicer --test outer_backend_probe -- --ignored --nocapture`.
//!
//! Shards are timed both packed and staggered. Equal-sized shards laid out
//! back to back all land on the same cache set, so every open stream fights for
//! one set; skewing each shard past its neighbour spreads them out. The gap
//! between the two columns is the cost of that aliasing, not of the codec.

use std::time::Instant;

const K: usize = 17;
const M: usize = 33;

/// `n` shards of `len` bytes, each starting `skew` bytes past the previous end.
fn shards(n: usize, len: usize, skew: usize, k: usize) -> (Vec<u8>, Vec<(usize, usize)>) {
    let stride = len + skew;
    let backing = vec![0u8; stride * n + 64];
    let spans = (0..n)
        .map(|i| (i * stride, len))
        .collect::<Vec<_>>();
    let mut backing = backing;
    for (i, &(off, l)) in spans.iter().enumerate().take(k) {
        backing[off..off + l].fill((i % 251) as u8);
    }
    (backing, spans)
}

fn time<F: FnMut()>(reps: usize, mut f: F) -> f64 {
    f();
    let start = Instant::now();
    for _ in 0..reps {
        f();
    }
    start.elapsed().as_secs_f64() * 1000.0 / reps as f64
}

fn tape_ms(len: usize, skew: usize) -> f64 {
    let rs = tape_reed_solomon::ReedSolomon::new(K, M).expect("tape rs");
    let (mut backing, spans) = shards(K + M, len, skew, K);

    time(5, || {
        let mut views: Vec<&mut [u8]> = Vec::with_capacity(K + M);
        let mut rest = backing.as_mut_slice();
        let mut consumed = 0usize;
        for &(off, l) in &spans {
            let (_, tail) = rest.split_at_mut(off - consumed);
            let (head, tail) = tail.split_at_mut(l);
            views.push(head);
            consumed = off + l;
            rest = tail;
        }
        rs.encode(&mut views).expect("tape encode");
    })
}

fn simd_ms(len: usize, skew: usize) -> f64 {
    let (backing, spans) = shards(K + M, len, skew, K);
    let mut encoder = reed_solomon_simd::ReedSolomonEncoder::new(K, M, len).expect("simd encoder");

    time(5, || {
        encoder.reset(K, M, len).expect("reset");
        for &(off, l) in spans.iter().take(K) {
            encoder.add_original_shard(&backing[off..off + l]).expect("add");
        }
        let out = encoder.encode().expect("simd encode");
        std::hint::black_box(out.recovery_iter().count());
    })
}

#[test]
#[ignore = "benchmark, run explicitly in release"]
fn compare_outer_backends() {
    let rs = tape_reed_solomon::ReedSolomon::new(K, M).expect("tape rs");
    println!("shape {K} of {} route {}", K + M, rs.encode_route(4 * 1024 * 1024));
    println!(
        "{:>10} {:>12} {:>12} {:>12} {:>12}",
        "shard", "tape packed", "tape skewed", "simd packed", "simd skewed"
    );

    for len in [64 * 1024usize, 1024 * 1024, 4 * 1024 * 1024] {
        println!(
            "{len:>10} {:>12.2} {:>12.2} {:>12.2} {:>12.2}",
            tape_ms(len, 0),
            tape_ms(len, 320),
            simd_ms(len, 0),
            simd_ms(len, 320),
        );
    }
}
