//! Honest round cost: the real per-phase compute of one Whirlwind round.
//!
//! This is the happy-path baseline. It runs the round the mechanism actually
//! specifies, with real code at every step, and times each phase: seed
//! derivation, Merkle proof generation and verification, one BLS attestation
//! signature, the q-of-n aggregate, and the aggregate verify. The point is to
//! show what a round costs when it works, so the attack costs have a baseline.

use anyhow::{anyhow, Result};
use tape_core::bls::BlsSignature;
use tape_core::erasure::SLICE_TREE_HEIGHT;
use tape_crypto::hash::Hash;
use tape_crypto::merkle::{create_proof_from_leaf_hashes, hash_leaf};

use crate::crypto::{
    aggregate, attestation_message, keypair, leaf_index, median_nanos, round_seed, sign,
    verify_certificate,
};
use crate::spool::Spool;

const EPOCH: u64 = 7;
const GROUP: u64 = 0;
const ROUND: u64 = 1;
const SLOT: u64 = 512;
const OWNER_INDEX: usize = 3;

/// Real per-phase compute cost of one honest round, medians in nanoseconds. The
/// compute total sums the phases and excludes network and disk.
pub struct LifecycleReport {
    pub group_size: usize,
    pub threshold: usize,
    pub seed_nanos: u128,
    pub proof_gen_nanos: u128,
    pub proof_verify_nanos: u128,
    pub sign_nanos: u128,
    pub aggregate_nanos: u128,
    pub cert_verify_nanos: u128,
    pub compute_nanos: u128,
    pub cert_ok: bool,
}

impl LifecycleReport {
    pub fn measure(spool: &Spool, threshold: usize, iterations: usize) -> Result<Self> {
        let group_size = spool.group_size;
        let owner = OWNER_INDEX.min(group_size - 1);
        let threshold = threshold.min(group_size);

        let entropy = tape_crypto::hash::hash(b"whirlwind-entropy-block");

        // Seed derivation and sample selection.
        let seed_nanos = median_nanos(iterations, || {
            round_seed(EPOCH, GROUP, ROUND, owner as u64, &entropy)
        });
        let seed = round_seed(EPOCH, GROUP, ROUND, owner as u64, &entropy);
        let _leaf = leaf_index(&seed, group_size);

        // Merkle proof generation and verification for the owner's slice leaf.
        // A running node keeps the leaf hashes cached, so proof generation is a
        // tree traversal over the cached hashes, not a re-hash of every slice.
        let leaf_hashes: Vec<Hash> = spool.slices.iter().map(|slice| hash_leaf(slice)).collect();
        let proof_gen_nanos = median_nanos(iterations, || {
            create_proof_from_leaf_hashes::<{ SLICE_TREE_HEIGHT }>(&leaf_hashes, owner).ok()
        });
        let proof = create_proof_from_leaf_hashes::<{ SLICE_TREE_HEIGHT }>(&leaf_hashes, owner)
            .map_err(|error| anyhow!("create_proof: {error:?}"))?;
        let owner_leaf = leaf_hashes
            .get(owner)
            .copied()
            .ok_or_else(|| anyhow!("owner leaf {owner} missing"))?;
        let proof_verify_nanos = median_nanos(iterations, || {
            spool
                .tree
                .verify_hash(owner as u64, &proof, owner_leaf)
                .unwrap_or(false)
        });

        // BLS: keypairs for the group, one attestation message, sign, aggregate q,
        // and verify the certificate.
        let mut secret_keys = Vec::with_capacity(group_size);
        let mut public_keys = Vec::with_capacity(group_size);
        for _ in 0..group_size {
            let (secret, public) = keypair()?;
            secret_keys.push(secret);
            public_keys.push(public);
        }
        let message = attestation_message(EPOCH, GROUP, ROUND, owner as u64, SLOT, &entropy);

        let sign_nanos = median_nanos(iterations, || sign(&secret_keys[0], &message).ok());

        let partials: Vec<BlsSignature> = secret_keys[..threshold]
            .iter()
            .map(|secret| sign(secret, &message))
            .collect::<Result<_>>()?;
        let aggregate_nanos = median_nanos(iterations, || aggregate(&partials).ok());
        let certificate = aggregate(&partials)?;

        let signer_pubkeys = &public_keys[..threshold];
        let cert_verify_nanos =
            median_nanos(iterations, || verify_certificate(&certificate, &message, signer_pubkeys));
        let cert_ok = verify_certificate(&certificate, &message, signer_pubkeys);

        let compute_nanos = seed_nanos
            + proof_gen_nanos
            + proof_verify_nanos
            + sign_nanos
            + aggregate_nanos
            + cert_verify_nanos;

        Ok(Self {
            group_size,
            threshold,
            seed_nanos,
            proof_gen_nanos,
            proof_verify_nanos,
            sign_nanos,
            aggregate_nanos,
            cert_verify_nanos,
            compute_nanos,
            cert_ok,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // the honest round certifies and every compute phase is measured
    #[test]
    fn honest_round() {
        let spool = Spool::build(1_000_000).unwrap();
        let report = LifecycleReport::measure(&spool, 14, 9).unwrap();
        assert!(report.cert_ok);
        // The whole compute path is well under a millisecond of medians summed;
        // it is not the bottleneck, the network is.
        assert!(report.compute_nanos > 0);
    }
}
