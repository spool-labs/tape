//! What the byte API costs against pubkeys parsed once.

use std::hint::black_box;
use std::time::Instant;

use tape_crypto::bls12254::min_sig::aggregate::{aggregate_partials, verify_aggregate};
use tape_crypto::bls12254::min_sig::g1::G1Point;
use tape_crypto::bls12254::min_sig::g2::G2Point;
use tape_crypto::bls12254::min_sig::native::{BatchItem, Committee, aggregate_partials as ark_aggregate, verify_batch};
use tape_crypto::bls12254::min_sig::privkey::PrivKey;

fn bench<T>(label: &str, iters: u32, mut f: impl FnMut() -> T) -> f64 {
    for _ in 0..2 {
        black_box(f());
    }
    let start = Instant::now();
    for _ in 0..iters {
        black_box(f());
    }
    let per = start.elapsed().as_secs_f64() * 1e6 / f64::from(iters);
    println!("{label:<46} {per:>10.1}us");
    per
}

fn committee(n: usize) -> (Vec<PrivKey>, Vec<G2Point>) {
    let secrets: Vec<PrivKey> = (0..n).map(|_| PrivKey::from_random()).collect();
    let pubkeys = secrets.iter().map(|k| G2Point::try_from(k).unwrap()).collect();
    (secrets, pubkeys)
}

#[test]
fn levers() {
    let msg = b"bn254 lever probe";

    println!("verify_aggregate, byte API vs pubkeys parsed once");
    for k in [1usize, 10, 64, 100] {
        let (secrets, pubkeys) = committee(k);
        let partials: Vec<G1Point> = secrets.iter().map(|s| s.sign(msg).unwrap()).collect();
        let agg = aggregate_partials(&partials).unwrap();
        let parsed = Committee::parse(&pubkeys).unwrap();

        let bytes = bench(&format!("  k={k:<4} byte API"), if k > 32 { 5 } else { 20 }, || {
            verify_aggregate(msg, &pubkeys, &agg).unwrap()
        });
        let native = bench(&format!("  k={k:<4} parsed once"), 20, || {
            parsed.verify_all(msg, &agg).unwrap()
        });
        println!("  k={k:<4} speedup {:.2}x\n", bytes / native);
    }

    println!("single signer, the challenge answer shape");
    let (secrets, pubkeys) = committee(20);
    let parsed = Committee::parse(&pubkeys).unwrap();
    let sigs: Vec<G1Point> = secrets.iter().map(|s| s.sign(msg).unwrap()).collect();
    let one = vec![pubkeys[3]];
    let solo = bench("  k=1    byte API", 20, || {
        verify_aggregate(msg, &one, &sigs[3]).unwrap()
    });
    let solo_native = bench("  k=1    parsed once", 20, || {
        parsed.verify_one(msg, parsed.key(3).unwrap(), &sigs[3]).unwrap()
    });
    println!("  speedup {:.2}x\n", solo / solo_native);

    println!("batching 20 single signer checks");
    let items: Vec<BatchItem<'_>> = (0..20)
        .map(|i| BatchItem { message: msg, signer: parsed.key(i).unwrap(), signature: &sigs[i] })
        .collect();
    let serial = bench("  20x one at a time (parsed)", 5, || {
        for i in 0..20 {
            parsed.verify_one(msg, parsed.key(i).unwrap(), &sigs[i]).unwrap();
        }
    });
    let batched = bench("  20 in one batch", 5, || verify_batch(&items).unwrap());
    println!("  speedup {:.2}x, per signature {:.1}us\n", serial / batched, batched / 20.0);

    println!("aggregate_partials, 20 partials");
    let b = bench("  byte API", 20, || aggregate_partials(&sigs).unwrap());
    let a = bench("  ark accumulation", 20, || ark_aggregate(&sigs).unwrap());
    println!("  speedup {:.2}x", b / a);
}
