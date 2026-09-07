//! Where the milliseconds go in one certificate verification.

use std::time::Instant;

use tape_core::bls::{BlsPrivateKey, BlsQuorumKey, BlsSignature};

// the summed path must agree with the full verify in both directions
#[test]
fn summed_verify_agrees() {
    let message = b"quorum key probe";
    let keys: Vec<BlsPrivateKey> = (0..19).map(|_| BlsPrivateKey::from_random()).collect();
    let sigs: Vec<BlsSignature> = keys.iter().map(|k| k.sign(message).unwrap()).collect();
    let pubkeys: Vec<_> = keys.iter().map(|k| k.public_key().unwrap()).collect();
    let agg = BlsSignature::aggregate(&sigs).unwrap();
    let quorum = BlsQuorumKey::sum(&pubkeys).unwrap();

    agg.verify_aggregate(message, &pubkeys).unwrap();
    let start = std::time::Instant::now();
    agg.verify_quorum(message, &quorum).unwrap();
    let summed_us = start.elapsed().as_micros();
    let start = std::time::Instant::now();
    agg.verify_aggregate(message, &pubkeys).unwrap();
    let full_us = start.elapsed().as_micros();
    println!("summed={summed_us}us full={full_us}us");
    assert!(agg.verify_quorum(b"a different message", &quorum).is_err());

    let short = BlsQuorumKey::sum(&pubkeys[..18]).unwrap();
    assert!(agg.verify_quorum(message, &short).is_err(), "a thinner quorum must not stand");
}

// one aggregate check costs ~10ms live against ~3ms of pairing math; this
// names the difference
#[test]
fn where_the_verify_goes() {
    let message = b"bls cost probe";
    let keys: Vec<BlsPrivateKey> = (0..19).map(|_| BlsPrivateKey::from_random()).collect();
    let sigs: Vec<BlsSignature> = keys.iter().map(|k| k.sign(message).unwrap()).collect();
    let pubkeys: Vec<_> = keys.iter().map(|k| k.public_key().unwrap()).collect();

    let start = Instant::now();
    let agg = BlsSignature::aggregate(&sigs).unwrap();
    let aggregate_us = start.elapsed().as_micros();

    let start = Instant::now();
    agg.verify_aggregate(message, &pubkeys).unwrap();
    let verify_us = start.elapsed().as_micros();

    let start = Instant::now();
    for _ in 0..19 {
        let _ = BlsSignature::aggregate(&sigs[..1]).unwrap();
    }
    let nineteen_roundtrips_us = start.elapsed().as_micros();

    panic!(
        "aggregate(19)={aggregate_us}us verify={verify_us}us 19x(decompress+compress)={nineteen_roundtrips_us}us"
    );
}
