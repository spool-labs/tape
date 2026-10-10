//! Encode one 64 MiB stream chunk into its upload slices.
//!
//! Prints a digest of every wire body, so two builds can be checked for the same bytes.
//! Peak memory comes from running the test binary under `/usr/bin/time -l`.
//!
//! Ignored by default. Run with:
//!   cargo test -p tape-sdk --release --test encode_bench -- --ignored --nocapture

use std::time::Instant;

use tape_core::encoding::TAPENET_PROFILE;
use tape_crypto::hash::hash;
use tape_sdk::codec::encoder::BlobEncoder;

const CHUNK_BYTES: usize = 64 * 1024 * 1024;
const ROUNDS: usize = 3;

fn payload() -> Vec<u8> {
    let mut state: u64 = 0x9E37_79B9_7F4A_7C15;
    (0..CHUNK_BYTES)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (state >> 24) as u8
        })
        .collect()
}

// one chunk through the upload encoder, timed, with its bytes summarized
#[test]
#[ignore]
fn chunk_encode() {
    let data = payload();

    let mut best = f64::MAX;
    let mut digest = String::new();
    for _ in 0..ROUNDS {
        let mut encoder = BlobEncoder::try_new(TAPENET_PROFILE).expect("encoder");

        let started = Instant::now();
        let (slices, root, _) = encoder.encode_with_leaves(&data).expect("encode");
        best = best.min(started.elapsed().as_secs_f64());

        let mut bodies = Vec::new();
        for slice in &slices {
            bodies.extend_from_slice(hash(&slice.body).as_ref());
        }
        bodies.extend_from_slice(root.as_ref());
        digest = hash(&bodies).to_string();
    }

    println!("chunk {} MiB  best {:.1} ms  digest {digest}", CHUNK_BYTES >> 20, best * 1e3);
}
