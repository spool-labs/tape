//! Off-chain verification against pubkeys parsed once
//!
//! The byte API reparses every pubkey per call and pairs H(m) with each signer
//! separately. Here the keys stay in curve form and a verify is two pairings.

use ark_bn254::{Bn254, Fq, Fq2, Fr, G1Affine, G1Projective, G2Affine, G2Projective};
use ark_ec::pairing::Pairing;
use ark_ec::{AffineRepr, CurveGroup};
use ark_ff::{BigInteger, One, PrimeField, UniformRand, Zero};

use crate::bls12254::errors::BLSError;
use super::g1::G1Point;
use super::g2::G2Point;
use super::hash::hash_to_curve;

fn fq(be: &[u8]) -> Fq {
    Fq::from_be_bytes_mod_order(be)
}

/// Reads a G1 point in the alt_bn254 byte layout: x then y, big endian.
pub fn g1_from_bytes(point: &G1Point) -> Result<G1Affine, BLSError> {
    let candidate = G1Affine::new_unchecked(fq(&point.0[..32]), fq(&point.0[32..]));
    if candidate.is_zero() || !candidate.is_on_curve() {
        return Err(BLSError::G1PointDecompressionError);
    }
    Ok(candidate)
}

/// Reads a G2 point in the alt_bn254 byte layout: each coordinate is the
/// imaginary part then the real part, big endian.
pub fn g2_from_bytes(point: &G2Point) -> Result<G2Affine, BLSError> {
    let x = Fq2::new(fq(&point.0[32..64]), fq(&point.0[..32]));
    let y = Fq2::new(fq(&point.0[96..128]), fq(&point.0[64..96]));
    let candidate = G2Affine::new_unchecked(x, y);
    if candidate.is_zero() || !candidate.is_on_curve() {
        return Err(BLSError::G2PointDecompressionError);
    }
    if !candidate.is_in_correct_subgroup_assuming_on_curve() {
        return Err(BLSError::G2PointDecompressionError);
    }
    Ok(candidate)
}

thread_local! {
    static PARSED: std::cell::RefCell<std::collections::HashMap<[u8; 128], G2Affine>> =
        std::cell::RefCell::new(std::collections::HashMap::new());
}

/// Parses a pubkey, reusing the curve form if this thread has seen the bytes.
///
/// Committee keys repeat on every certificate and every challenge answer, and
/// the parse behind them is a subgroup check.
pub fn cached_g2(point: &G2Point) -> Result<G2Affine, BLSError> {
    if let Some(hit) = PARSED.with(|c| c.borrow().get(&point.0).copied()) {
        return Ok(hit);
    }
    let parsed = g2_from_bytes(point)?;
    PARSED.with(|c| {
        let mut held = c.borrow_mut();
        // Bounded so a long lived node cannot grow this without limit.
        if held.len() >= 4096 {
            held.clear();
        }
        held.insert(point.0, parsed);
    });
    Ok(parsed)
}

/// Verifies against a key already summed over the quorum.
pub fn verify_summed<M: AsRef<[u8]>>(
    message: M,
    summed: &G2Point,
    s_sum: &G1Point,
) -> Result<(), BLSError> {
    let h = g1_from_bytes(&hash_to_curve(message.as_ref())?)?;
    let s = g1_from_bytes(s_sum)?;
    pairing_holds(&[h, s], &[cached_g2(summed)?, -G2Affine::generator()])
}

/// Verifies an aggregate against an exact signer list, summed in curve form.
///
/// Keeps the byte path's refusals: no empty set, no zero key, no duplicate.
pub fn verify_signers<M: AsRef<[u8]>>(
    message: M,
    signers: &[G2Point],
    s_sum: &G1Point,
) -> Result<(), BLSError> {
    if signers.is_empty() {
        return Err(BLSError::SerializationError);
    }
    for (i, key) in signers.iter().enumerate() {
        if key.0 == [0u8; 128] || signers[i + 1..].contains(key) {
            return Err(BLSError::SerializationError);
        }
    }
    let mut acc = G2Projective::zero();
    for key in signers {
        acc += cached_g2(key)?;
    }
    let summed = acc.into_affine();
    if summed.is_zero() {
        return Err(BLSError::SerializationError);
    }
    let h = g1_from_bytes(&hash_to_curve(message.as_ref())?)?;
    let s = g1_from_bytes(s_sum)?;
    pairing_holds(&[h, s], &[summed, -G2Affine::generator()])
}

/// A committee's keys in curve form, parsed and subgroup checked once.
#[derive(Clone, Debug)]
pub struct Committee {
    keys: Vec<G2Affine>,
    summed: G2Affine,
    minus_one: G2Affine,
}

impl Committee {
    /// Parses and validates a committee, rejecting an empty or duplicated set.
    pub fn parse(pubkeys: &[G2Point]) -> Result<Self, BLSError> {
        if pubkeys.is_empty() {
            return Err(BLSError::SerializationError);
        }
        for (i, key) in pubkeys.iter().enumerate() {
            if pubkeys[i + 1..].contains(key) {
                return Err(BLSError::SerializationError);
            }
            let _ = i;
        }
        let keys = pubkeys.iter().map(g2_from_bytes).collect::<Result<Vec<_>, _>>()?;
        let summed: G2Affine = keys.iter().fold(G2Projective::zero(), |acc, k| acc + k).into_affine();
        if summed.is_zero() {
            return Err(BLSError::SerializationError);
        }
        Ok(Self { keys, summed, minus_one: -G2Affine::generator() })
    }

    pub fn len(&self) -> usize {
        self.keys.len()
    }

    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    pub fn key(&self, index: usize) -> Option<&G2Affine> {
        self.keys.get(index)
    }

    /// Verifies an aggregate signature against the whole committee summed.
    pub fn verify_all<M: AsRef<[u8]>>(&self, message: M, s_sum: &G1Point) -> Result<(), BLSError> {
        let h = g1_from_bytes(&hash_to_curve(message.as_ref())?)?;
        let s = g1_from_bytes(s_sum)?;
        pairing_holds(&[h, s], &[self.summed, self.minus_one])
    }

    /// Verifies one signer's signature, the shape a challenge answer carries.
    pub fn verify_one<M: AsRef<[u8]>>(
        &self,
        message: M,
        signer: &G2Affine,
        signature: &G1Point,
    ) -> Result<(), BLSError> {
        let h = g1_from_bytes(&hash_to_curve(message.as_ref())?)?;
        let s = g1_from_bytes(signature)?;
        pairing_holds(&[h, s], &[*signer, self.minus_one])
    }
}

fn pairing_holds(g1: &[G1Affine], g2: &[G2Affine]) -> Result<(), BLSError> {
    let product = Bn254::multi_pairing(g1, g2);
    if product.0 == ark_bn254::Fq12::one() {
        Ok(())
    } else {
        Err(BLSError::BLSVerificationError)
    }
}

/// Writes a G1 point back to the alt_bn254 byte layout.
pub fn g1_to_bytes(point: &G1Affine) -> Result<G1Point, BLSError> {
    let mut out = [0u8; 64];
    out[..32].copy_from_slice(&point.x().ok_or(BLSError::SerializationError)?.into_bigint().to_bytes_be());
    out[32..].copy_from_slice(&point.y().ok_or(BLSError::SerializationError)?.into_bigint().to_bytes_be());
    Ok(G1Point(out))
}

/// One signature to check as part of a batch.
pub struct BatchItem<'a> {
    pub message: &'a [u8],
    pub signer: &'a G2Affine,
    pub signature: &'a G1Point,
}

/// Checks many single signer signatures under one final exponentiation.
///
/// Each item carries a random blinding scalar, without which two bad signatures
/// can cancel and pass together. A failure names no member.
pub fn verify_batch_with<R: rand::Rng>(items: &[BatchItem<'_>], rng: &mut R) -> Result<(), BLSError> {
    if items.is_empty() {
        return Err(BLSError::SerializationError);
    }
    let minus_one = -G2Affine::generator();

    let mut g1 = Vec::with_capacity(items.len() + 1);
    let mut g2 = Vec::with_capacity(items.len() + 1);
    let mut blinded_sum = G1Projective::zero();

    for item in items {
        let r = Fr::rand(rng);
        let h = g1_from_bytes(&hash_to_curve(item.message)?)?;
        let s = g1_from_bytes(item.signature)?;
        g1.push((h * r).into_affine());
        g2.push(*item.signer);
        blinded_sum += s * r;
    }
    g1.push(blinded_sum.into_affine());
    g2.push(minus_one);

    pairing_holds(&g1, &g2)
}

/// Checks a batch against the thread's own randomness.
pub fn verify_batch(items: &[BatchItem<'_>]) -> Result<(), BLSError> {
    verify_batch_with(items, &mut rand::thread_rng())
}

/// Sums partial signatures without leaving curve form between adds.
pub fn aggregate_partials(partials: &[G1Point]) -> Result<G1Point, BLSError> {
    let (first, rest) = partials.split_first().ok_or(BLSError::SerializationError)?;
    let mut acc: G1Projective = g1_from_bytes(first)?.into();
    for partial in rest {
        acc += g1_from_bytes(partial)?;
    }
    let sum = acc.into_affine();
    if sum.is_zero() {
        return Err(BLSError::SerializationError);
    }
    g1_to_bytes(&sum)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bls12254::min_sig::aggregate::{aggregate_partials as bytes_aggregate, verify_aggregate};
    use crate::bls12254::min_sig::privkey::PrivKey;

    fn committee(n: usize) -> (Vec<PrivKey>, Vec<G2Point>) {
        let secrets: Vec<PrivKey> = (0..n).map(|_| PrivKey::from_random()).collect();
        let pubkeys = secrets.iter().map(|k| G2Point::try_from(k).unwrap()).collect();
        (secrets, pubkeys)
    }

    // the curve-form parse must read the same point the byte path signs against
    #[test]
    fn parse_agrees() {
        let msg = b"native parse";
        let (secrets, pubkeys) = committee(4);
        let partials: Vec<G1Point> = secrets.iter().map(|k| k.sign(msg).unwrap()).collect();
        let agg = bytes_aggregate(&partials).unwrap();

        verify_aggregate(msg, &pubkeys, &agg).unwrap();
        Committee::parse(&pubkeys).unwrap().verify_all(msg, &agg).unwrap();
    }

    // a single signer is the shape a challenge answer carries
    #[test]
    fn one_signer() {
        let msg = b"native single";
        let (secrets, pubkeys) = committee(3);
        let parsed = Committee::parse(&pubkeys).unwrap();
        let signature = secrets[1].sign(msg).unwrap();

        parsed.verify_one(msg, parsed.key(1).unwrap(), &signature).unwrap();
        assert!(parsed.verify_one(msg, parsed.key(0).unwrap(), &signature).is_err());
        assert!(parsed.verify_one(b"other", parsed.key(1).unwrap(), &signature).is_err());
    }

    // summing in curve form must land on the same point as the byte path
    #[test]
    fn aggregate_matches() {
        let msg = b"native aggregate";
        let (secrets, _) = committee(6);
        let partials: Vec<G1Point> = secrets.iter().map(|k| k.sign(msg).unwrap()).collect();

        assert_eq!(aggregate_partials(&partials).unwrap().0, bytes_aggregate(&partials).unwrap().0);
    }

    // a batch stands only while every signature in it does
    #[test]
    fn batch_holds() {
        let msg = b"native batch";
        let (secrets, pubkeys) = committee(8);
        let parsed = Committee::parse(&pubkeys).unwrap();
        let signatures: Vec<G1Point> = secrets.iter().map(|k| k.sign(msg).unwrap()).collect();

        let items: Vec<BatchItem<'_>> = (0..8)
            .map(|i| BatchItem { message: msg, signer: parsed.key(i).unwrap(), signature: &signatures[i] })
            .collect();
        verify_batch(&items).unwrap();

        // one signer swapped for another's signature fails the whole batch
        let mut swapped = signatures.clone();
        swapped.swap(2, 5);
        let bad: Vec<BatchItem<'_>> = (0..8)
            .map(|i| BatchItem { message: msg, signer: parsed.key(i).unwrap(), signature: &swapped[i] })
            .collect();
        assert!(verify_batch(&bad).is_err());
    }

    // two signatures whose errors cancel must not pass together: this is what the
    // per item blinding buys, and without it the pair verifies while neither does
    #[test]
    fn cancelling_pair_fails() {
        let msg = b"native cancel";
        let (secrets, pubkeys) = committee(2);
        let parsed = Committee::parse(&pubkeys).unwrap();
        let good: Vec<G1Point> = secrets.iter().map(|k| k.sign(msg).unwrap()).collect();

        // Offset one signature by D and the other by -D, so the sum is untouched.
        let drift = G1Affine::generator();
        let a = (g1_from_bytes(&good[0]).unwrap() + drift).into_affine();
        let b = (g1_from_bytes(&good[1]).unwrap() - drift).into_affine();
        let (bad_a, bad_b) = (g1_to_bytes(&a).unwrap(), g1_to_bytes(&b).unwrap());

        // Neither stands on its own.
        assert!(parsed.verify_one(msg, parsed.key(0).unwrap(), &bad_a).is_err());
        assert!(parsed.verify_one(msg, parsed.key(1).unwrap(), &bad_b).is_err());

        // Their sum is still the honest aggregate, so an unblinded batch would pass.
        let summed = aggregate_partials(&[bad_a, bad_b]).unwrap();
        assert_eq!(summed.0, aggregate_partials(&good).unwrap().0);

        let items = vec![
            BatchItem { message: msg, signer: parsed.key(0).unwrap(), signature: &bad_a },
            BatchItem { message: msg, signer: parsed.key(1).unwrap(), signature: &bad_b },
        ];
        assert!(verify_batch(&items).is_err(), "blinding must break the cancellation");
    }

    // a duplicated key is a rogue key attempt, and an empty set signs nothing
    #[test]
    fn parse_refuses() {
        let (_, pubkeys) = committee(2);
        assert!(Committee::parse(&[]).is_err());
        assert!(Committee::parse(&[pubkeys[0], pubkeys[0]]).is_err());
    }
}
