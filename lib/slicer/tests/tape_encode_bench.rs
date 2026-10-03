//! Tape's real encode path, for comparing dependency versions. Run with
//! `cargo test --release -p tape-slicer --test tape_encode_bench -- --ignored --nocapture`.

use std::time::Instant;

use tape_slicer::{ErasureCoder, OuterCoder, ReedSolomonCoder, Slicer};

fn bench(label: &str, payload: usize, mut run: impl FnMut(&[u8])) {
    let data: Vec<u8> = (0..payload).map(|i| (i % 251) as u8).collect();
    run(&data);

    let reps = (256 * 1024 * 1024 / payload).clamp(3, 20);
    let start = Instant::now();
    for _ in 0..reps {
        run(&data);
    }
    let ms = start.elapsed().as_secs_f64() * 1000.0 / reps as f64;
    println!(
        "{label:<16} {payload:>10} {ms:>10.2} {:>10.1}",
        payload as f64 / (ms / 1000.0) / (1024.0 * 1024.0)
    );
}

#[test]
#[ignore = "benchmark, run explicitly in release"]
fn encode_paths() {
    println!("{:<16} {:>10} {:>10} {:>10}", "path", "payload", "ms", "MiB/s");

    for payload in [1024 * 1024usize, 10 * 1024 * 1024, 64 * 1024 * 1024] {
        let mut slicer = Slicer::clay_default();
        bench("clay striped", payload, |data| {
            std::hint::black_box(slicer.encode(data).expect("clay encode"));
        });
    }

    // Outer coding at 50 spool groups: k = 17 of 50, 4 MiB chunk ceiling.
    for payload in [17 * 1024 * 1024usize, 17 * 4 * 1024 * 1024] {
        let mut outer = OuterCoder::new(17, 50);
        bench("outer 17of50", payload, |data| {
            std::hint::black_box(outer.encode(data).expect("outer encode"));
        });
    }

    for payload in [1024 * 1024usize, 10 * 1024 * 1024, 64 * 1024 * 1024] {
        let mut coder = ReedSolomonCoder::new(10, 10);
        bench("rs basic", payload, |data| {
            std::hint::black_box(coder.encode(data).expect("rs encode"));
        });
    }
}
