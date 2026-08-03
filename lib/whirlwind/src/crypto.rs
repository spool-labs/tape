//! Whirlwind crypto: round seed derivation, the attestation message, and the
//! BLS attestation and certificate primitives, over the real tape crypto.
//!
//! The certificate case is fast-aggregate-verify: q spool owners sign one common
//! round message, their signatures aggregate into a single BLS signature, and a
//! verifier checks it against the exact list of signer public keys.

use std::hint::black_box;
use std::time::Instant;

use anyhow::{anyhow, Result};
use tape_core::bls::{BlsPrivateKey, BlsPubkey, BlsSignature};
use tape_crypto::hash::{hashv, Hash};

/// Domain tag binding a seed to a Whirlwind challenge round.
const SEED_DOMAIN: &[u8] = b"WHRLWSD1";
/// Domain tag binding a Whirlwind attestation message.
const ATTEST_DOMAIN: &[u8] = b"WHRLWAT1";
/// Domain tag binding a Whirlwind eviction proposal message.
const EVICT_DOMAIN: &[u8] = b"WHRLWEV1";

/// Derive the round seed from the entropy block hash and the round coordinates.
///
/// Every spool owner derives the same seed without coordination: the entropy
/// block hash is public once the block is produced, and the coordinates are
/// finalized state.
pub fn round_seed(epoch: u64, group: u64, round: u64, spool: u64, entropy: &Hash) -> Hash {
    hashv(&[
        SEED_DOMAIN,
        &epoch.to_le_bytes(),
        &group.to_le_bytes(),
        &round.to_le_bytes(),
        &spool.to_le_bytes(),
        entropy.as_ref(),
    ])
}

/// Pick a leaf index in 0..count from a seed, uniformly.
pub fn leaf_index(seed: &Hash, count: usize) -> usize {
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&seed.0[..8]);
    (u64::from_le_bytes(bytes) % count.max(1) as u64) as usize
}

/// The message a spool owner signs to attest a witnessed possession proof.
///
/// The message binds the epoch, group, round, and challenged spool, plus the
/// entropy block's slot and hash. Binding the slot pins the signature to one
/// produced block, so signatures made against a competing fork cannot aggregate.
pub fn attestation_message(
    epoch: u64,
    group: u64,
    round: u64,
    spool: u64,
    slot: u64,
    entropy: &Hash,
) -> Vec<u8> {
    let mut message = Vec::with_capacity(ATTEST_DOMAIN.len() + 40 + Hash::LEN);
    message.extend_from_slice(ATTEST_DOMAIN);
    message.extend_from_slice(&epoch.to_le_bytes());
    message.extend_from_slice(&group.to_le_bytes());
    message.extend_from_slice(&round.to_le_bytes());
    message.extend_from_slice(&spool.to_le_bytes());
    message.extend_from_slice(&slot.to_le_bytes());
    message.extend_from_slice(entropy.as_ref());
    message
}

/// The message a member signs to propose excluding a target from the next
/// committee. The eviction vote is cause-free: the message binds only the epoch,
/// group, and target identity, never the challenge evidence, so the quorum check
/// counts signatures and never re-examines why each signer voted.
pub fn eviction_message(epoch: u64, group: u64, target: u64) -> Vec<u8> {
    let mut message = Vec::with_capacity(EVICT_DOMAIN.len() + 24);
    message.extend_from_slice(EVICT_DOMAIN);
    message.extend_from_slice(&epoch.to_le_bytes());
    message.extend_from_slice(&group.to_le_bytes());
    message.extend_from_slice(&target.to_le_bytes());
    message
}

/// Generate one BLS keypair.
pub fn keypair() -> Result<(BlsPrivateKey, BlsPubkey)> {
    let private_key = BlsPrivateKey::from_random();
    let public_key = private_key
        .public_key()
        .map_err(|error| anyhow!("bls public key: {error:?}"))?;
    Ok((private_key, public_key))
}

/// Sign a message.
pub fn sign(private_key: &BlsPrivateKey, message: &[u8]) -> Result<BlsSignature> {
    private_key
        .sign(message)
        .map_err(|error| anyhow!("bls sign: {error:?}"))
}

/// Aggregate the partial signatures of a certificate.
pub fn aggregate(partials: &[BlsSignature]) -> Result<BlsSignature> {
    BlsSignature::aggregate(partials).map_err(|error| anyhow!("bls aggregate: {error:?}"))
}

/// Verify an aggregate certificate against the exact list of signer public keys.
pub fn verify_certificate(
    aggregate_signature: &BlsSignature,
    message: &[u8],
    signer_pubkeys: &[BlsPubkey],
) -> bool {
    aggregate_signature
        .verify_aggregate(message, signer_pubkeys)
        .is_ok()
}

/// Median wall-clock over the given number of runs, in nanoseconds.
///
/// Each result is kept live so the compiler cannot optimize the work away.
/// Used to profile the honest round's compute phases.
pub fn median_nanos<Output>(iterations: usize, mut op: impl FnMut() -> Output) -> u128 {
    let count = iterations.max(1);
    let mut samples = Vec::with_capacity(count);
    for _ in 0..count {
        let start = Instant::now();
        let output = op();
        samples.push(start.elapsed().as_nanos());
        black_box(output);
    }
    samples.sort_unstable();
    samples[samples.len() / 2]
}

#[cfg(test)]
mod tests {
    use super::*;

    // a quorum of honest signatures aggregates into a certificate that verifies
    #[test]
    fn honest_certificate() {
        let entropy = tape_crypto::hash::hash(b"entropy");
        let message = attestation_message(7, 0, 1, 3, 512, &entropy);
        let threshold = 14;
        let mut pubkeys = Vec::new();
        let mut partials = Vec::new();
        for _ in 0..threshold {
            let (secret, public) = keypair().expect("keypair");
            partials.push(sign(&secret, &message).expect("sign"));
            pubkeys.push(public);
        }
        let cert = aggregate(&partials).expect("aggregate");
        assert!(verify_certificate(&cert, &message, &pubkeys));
        // A different message must not verify.
        let other = attestation_message(7, 0, 2, 3, 512, &entropy);
        assert!(!verify_certificate(&cert, &other, &pubkeys));
    }

    // an eviction certificate verifies for its voters and fails on a tampered set
    #[test]
    fn eviction_quorum() {
        let message = eviction_message(3, 0, 9);
        let threshold = 14;
        let mut pubkeys = Vec::new();
        let mut partials = Vec::new();
        for _ in 0..threshold {
            let (secret, public) = keypair().expect("keypair");
            partials.push(sign(&secret, &message).expect("sign"));
            pubkeys.push(public);
        }
        let certificate = aggregate(&partials).expect("aggregate");
        assert!(verify_certificate(&certificate, &message, &pubkeys));

        // A tampered voter set, dropping one signer and adding a stranger, fails.
        let (_, stranger) = keypair().expect("keypair");
        let mut tampered = pubkeys.clone();
        tampered.pop();
        tampered.push(stranger);
        assert!(!verify_certificate(&certificate, &message, &tampered));
    }

    // the same round coordinates derive the same seed and a leaf inside the group
    #[test]
    fn seed_determinism() {
        let entropy = tape_crypto::hash::hash(b"entropy");
        let a = round_seed(7, 0, 1, 3, &entropy);
        let b = round_seed(7, 0, 1, 3, &entropy);
        assert_eq!(a.0, b.0);
        assert!(leaf_index(&a, 20) < 20);
    }
}
