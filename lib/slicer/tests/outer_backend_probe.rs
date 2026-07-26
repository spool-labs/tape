//! Probe: is tape-reed-solomon competitive at the outer coding shape (17 of 50)?
//! Decides whether outer.rs can drop reed-solomon-simd. Run with
//! `cargo test --release -p tape-slicer --test outer_backend_probe -- --ignored --nocapture`.

use std::time::Instant;

const K: usize = 17;
const M: usize = 33;

fn time<F: FnMut()>(reps: usize, mut f: F) -> f64 {
    f();
    let start = Instant::now();
    for _ in 0..reps {
        f();
    }
    start.elapsed().as_secs_f64() * 1000.0 / reps as f64
}

#[test]
#[ignore = "benchmark, run explicitly in release"]
fn compare_outer_backends() {
    let tape = tape_reed_solomon::ReedSolomon::new(K, M).expect("tape rs");

    println!("shape {K} of {} (k={K}, m={M})", K + M);
    for len in [64 * 1024usize, 1024 * 1024, 4 * 1024 * 1024] {
        println!("  shard {len:>9}: route {}", tape.encode_route(len));
    }

    // Control: the blob shapes the crate ships generated programs for.
    for (k, m) in [(10usize, 10usize), (7, 13)] {
        let rs = tape_reed_solomon::ReedSolomon::new(k, m).expect("tape rs");
        println!("  control {k} of {}: route {}", k + m, rs.encode_route(1024 * 1024));
    }

    for len in [64 * 1024usize, 1024 * 1024, 4 * 1024 * 1024] {
        let mut shards: Vec<Vec<u8>> = (0..K + M)
            .map(|i| if i < K { vec![(i % 251) as u8; len] } else { vec![0u8; len] })
            .collect();

        let tape_ms = time(5, || {
            tape.encode(&mut shards).expect("tape encode");
        });

        let data: Vec<Vec<u8>> = (0..K).map(|i| vec![(i % 251) as u8; len]).collect();
        let mut encoder =
            reed_solomon_simd::ReedSolomonEncoder::new(K, M, len).expect("simd encoder");
        let simd_ms = time(5, || {
            encoder.reset(K, M, len).expect("reset");
            for chunk in &data {
                encoder.add_original_shard(chunk).expect("add");
            }
            let out = encoder.encode().expect("simd encode");
            std::hint::black_box(out.recovery_iter().count());
        });

        println!(
            "  shard {len:>9}: tape {tape_ms:>8.2} ms   simd {simd_ms:>8.2} ms   ratio {:.2}x",
            tape_ms / simd_ms
        );
    }
}
