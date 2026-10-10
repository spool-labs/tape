//! Golden vectors for the production stripe scheme (issue 69).
//!
//! Pins the exact commitment roots and slice geometry writers emit. Any change
//! that alters these values is a storage compatibility break, not a refactor.
//! Regenerate only for a deliberate, fleet-coordinated change:
//!
//!   cargo test -p tape-slicer --test golden_vectors -- --ignored --nocapture

use tape_core::encoding::{ClayParams, ErasureCode, RSParams, RepParams, TAPENET_PROFILE};
use tape_slicer::{blob_merkle_root, ErasureCoder, SliceMetadata, Slicer};

/// (blob_len, stripe_size, slice_len, commitment root)
type Vector = (usize, usize, usize, &'static str);

const SMALL_CLAY: ErasureCode = ErasureCode::Clay(ClayParams::new(4, 2, 3));
const ONE_PARITY: ErasureCode = ErasureCode::ReedSolomon(RSParams::new(3, 1));
const TWO_PARITY: ErasureCode = ErasureCode::ReedSolomon(RSParams::new(4, 2));
const MIRROR: ErasureCode = ErasureCode::Replication(RepParams::new(2));

const TAPENET_LENS: &[usize] =
    &[0, 1, 1_399, 1_400, 100_000, 100_001, 999_999, 1_000_001, 2_500_000];
const CUSTOM_LENS: &[usize] = &[0, 1, 1_000, 100_001, 1_000_001, 2_500_000];

// Tapenet's profile: the vectors every deployed network depends on. Roots
// regenerated when the profile left the slice metadata. Stripe sizes held,
// and each slice is 16 bytes shorter.
const TAPENET_VECTORS: &[Vector] = &[
    (0, 1400, 232, "BMbhXTuR6NtU3D9T1aW7WkWnqhXhjVi5igjWW6oZQE5V"),
    (1, 1400, 232, "Fi69Bgkeh9THHBMNiLgPkmZFV5nxWzQYF1L51P1fcUur"),
    (1399, 1400, 232, "6mzEhYmyyL2W3zsar4BBLiz4h8AfAcYe6Wk5rQxQ3xSN"),
    (1400, 1400, 232, "96gWBfxL5pCFykAWi7JTQrjDJb2yqcRWvfeaqQnpmXFZ"),
    (100000, 100800, 14432, "6JUHuJL8qdormCLsEkiic3aMHRgjQNwFZD5AYf3QEYZn"),
    (100001, 100800, 14432, "FSjMS32Nde5GrNs5eKaYPztXqMJGWiwT9WtiBVPPKm3x"),
    (999999, 1001000, 143032, "DYBr2KgaFrAjYoU8hCJxm8REBF1Qe5qpAkNBp5phJaVP"),
    (1000001, 501200, 143232, "HLpL6qbqiDSyoovnwCypsGoT1cYYBVEknqsTWKW5XKMJ"),
    (2500000, 834400, 357632, "HxB1YQPFc9YHBTy2Sv9Y2PYozoEpSie9MGq5U6igzdzW"),
];

const SMALL_CLAY_VECTORS: &[Vector] = &[
    (0, 16, 40, "5PFCH4cSXvUovBU765P6vA2624hZJZfDpqKxuHb22cQw"),
    (1, 16, 40, "99N4dN17hCZN2R5LbRvxgUWvYEKQJqq3NNwNqJdxdbUz"),
    (1000, 1008, 536, "55zzy6GasAqJ94P9o6Boz6ZmX2vgUKhW6CKar7HjaNnp"),
    (100001, 100016, 50040, "AuRnQP47eviujGbu7RfdgoMD2SGvW4VdqtBCdxgx49pH"),
    (1000001, 500016, 500048, "5dYUkeynK77tN1zTLcPPhA5Kv8N2D3kXx2VVf4ke4Gpa"),
    (2500000, 833344, 1250048, "GAUYrM7uRGDSqbErJTkqJUSPntHCzG7jrmKxDmCVGiwg"),
];

const ONE_PARITY_VECTORS: &[Vector] = &[
    (0, 192, 96, "B8eA4hRDbsEVyn9SgbJfNcpwuHB3gygAZXWMV5XeHkMe"),
    (1, 192, 96, "6VVDXoGv5AfieesW9622i6UkT8bDc2dpS7HE5c6Z7Cmq"),
    (1000, 1152, 416, "BR9a9tszyrBkPr7RTkRqLtRvUCTeP32HLGz5ebJHnht8"),
    (100001, 100032, 33376, "csMHrZYx3hyahRrU2G8ieBEbFJavxjtwQ39Ga3i65Fa"),
    (1000001, 500160, 333472, "3RwZBVuKY9K17RpTT5WBC9a3ss6zzuUMBdDEnmh9TTW4"),
    (2500000, 833472, 833504, "3igBi1dEVF4jTqcipodqK8BsMiP76sJ5mJH9fFXw6SCx"),
];

const TWO_PARITY_VECTORS: &[Vector] = &[
    (0, 256, 96, "4ZSgN5HByMWpjBS2ZCYjcwtiDBgEybARoCTnoxPS8Pj1"),
    (1, 256, 96, "DChjvzW1BpDwxQRmvYq94fdmbBSEUBqyTcvptUzPvpZX"),
    (1000, 1024, 288, "AkrdU5jJnXTSi2wMWRqsBdQ52qnu3WY1wkoPPUWCzbUL"),
    (100001, 100096, 25056, "E7SFk5NmtffjmSPfnH7r9VyeaQsJxPnW9vkZxYPWe12S"),
    (1000001, 500224, 250144, "7QaPb5fSoTZbJg6N41MFdXSFqQV2kmvuG7JMMEW6HHXU"),
    (2500000, 833536, 625184, "DRgMZDdgtimSjSKaLhVQA81y46RFudhAwPUBzwtNwRSW"),
];

const MIRROR_VECTORS: &[Vector] = &[
    (0, 1, 33, "6PupU5Qj78C1ECfq5fZRJgEhJakEgLNW6ig46sEWvb2u"),
    (1, 1, 33, "Afa6KiNiZqpzsDv8RvTkyoLYHY9vJk4JXKuwykds3KRf"),
    (1000, 1000, 1032, "JBzXXnwWbWW6qD7gh8hxX13hG3JnhzVZBY2LpdMFZKxf"),
    (100001, 100001, 100033, "312cC3LLhqSThLadyAw8dD5DJQn8x5waNFTwiAetkVn3"),
    (1000001, 500001, 1000034, "8N6n4YDTSjGPykzhQUJ3M6VYvsbHsnbzvr8EtFVgWoNF"),
    (2500000, 833334, 2500034, "85QHme1mMSvWLACKiwrKwH98AxUWXpjo4R7BrqT1DRtw"),
];

fn mk(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i % 251) as u8).collect()
}

fn measure(profile: ErasureCode, len: usize) -> (usize, usize, usize, String) {
    let mut slicer = Slicer::try_from_profile(profile).expect("valid profile");
    let slices = slicer.encode(&mk(len)).expect("encode");
    let meta = SliceMetadata::parse(&slices[0]).expect("metadata");

    (len, meta.stripe_size(), slices[0].len(), blob_merkle_root(&slices).to_string())
}

fn check(profile: ErasureCode, lens: &[usize], vectors: &[Vector]) {
    assert_eq!(vectors.len(), lens.len(), "{profile:?}: a vector per length");

    for (&len, &(blob_len, stripe_size, slice_len, root)) in lens.iter().zip(vectors) {
        let measured = measure(profile, len);

        assert_eq!(blob_len, len, "{profile:?}: vector order");
        assert_eq!(measured.1, stripe_size, "{profile:?} blob_len {len}: stripe size drifted");
        assert_eq!(measured.2, slice_len, "{profile:?} blob_len {len}: slice length drifted");
        assert_eq!(measured.3, root, "{profile:?} blob_len {len}: commitment root drifted");
    }
}

// writer output cannot drift without breaking every commitment already on the
// network
#[test]
fn tapenet() {
    check(TAPENET_PROFILE, TAPENET_LENS, TAPENET_VECTORS);
}

// a small Clay profile keeps its own geometry and roots
#[test]
fn small_clay() {
    check(SMALL_CLAY, CUSTOM_LENS, SMALL_CLAY_VECTORS);
}

// Reed-Solomon with one parity slice, 3 of 4
#[test]
fn one_parity() {
    check(ONE_PARITY, CUSTOM_LENS, ONE_PARITY_VECTORS);
}

// Reed-Solomon with two parity slices, 4 of 6
#[test]
fn two_parity() {
    check(TWO_PARITY, CUSTOM_LENS, TWO_PARITY_VECTORS);
}

// two full copies
#[test]
fn mirror() {
    check(MIRROR, CUSTOM_LENS, MIRROR_VECTORS);
}

// prints every table above in source form
#[test]
#[ignore = "regenerates the vectors, run explicitly"]
fn print_vectors() {
    let sets = [
        ("TAPENET_VECTORS", TAPENET_PROFILE, TAPENET_LENS),
        ("SMALL_CLAY_VECTORS", SMALL_CLAY, CUSTOM_LENS),
        ("ONE_PARITY_VECTORS", ONE_PARITY, CUSTOM_LENS),
        ("TWO_PARITY_VECTORS", TWO_PARITY, CUSTOM_LENS),
        ("MIRROR_VECTORS", MIRROR, CUSTOM_LENS),
    ];

    for (name, profile, lens) in sets {
        println!("const {name}: &[Vector] = &[");
        for &len in lens {
            let (blob_len, stripe_size, slice_len, root) = measure(profile, len);
            println!("    ({blob_len}, {stripe_size}, {slice_len}, \"{root}\"),");
        }
        println!("];");
    }
}
