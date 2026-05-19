#[cfg(not(target_os = "solana"))]
use rand::RngCore;

use solana_bn254::prelude::alt_bn128_multiplication;

#[cfg(not(target_os = "solana"))]
use crate::bls12254::SCALAR_MODULUS;

use crate::bls12254::errors::BLSError;
use super::g1::G1Point;
use super::g2::{G2Point, G2CompressedPoint};
use super::hash::hash_to_curve;
use bytemuck::{Pod, Zeroable};

const POP_DOMAIN: &[u8] = b"BLS_POP_BN254";

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct PrivKey(pub [u8; 32]);

impl PrivKey {
    /// Generate a new random private key.
    #[cfg(not(target_os = "solana"))]
    pub fn from_random() -> PrivKey {
        Self::from_rng(&mut rand::thread_rng())
    }

    /// Build a private key from bytes after validating the scalar range.
    #[cfg(not(target_os = "solana"))]
    pub fn try_from_bytes(bytes: [u8; 32]) -> Result<PrivKey, BLSError> {
        if bytes == [0u8; 32] {
            return Err(BLSError::SecretKeyError);
        }

        let num = dashu::integer::UBig::from_be_bytes(&bytes);
        if num >= SCALAR_MODULUS {
            return Err(BLSError::SecretKeyError);
        }

        Ok(Self(bytes))
    }

    /// Generate a new private key from an explicit RNG, using rejection
    /// sampling to ensure the scalar is nonzero and below the group order.
    /// Exposed so callers can drive deterministic keygen from a seeded RNG.
    #[cfg(not(target_os = "solana"))]
    pub fn from_rng<R: RngCore>(rng: &mut R) -> PrivKey {
        loop {
            let mut bytes = [0u8; 32];
            rng.fill_bytes(&mut bytes);
            if let Ok(key) = Self::try_from_bytes(bytes) {
                return key;
            }
        }
    }

    /// Sign a message, returning the signature point in G1.
    pub fn sign<T: AsRef<[u8]>>(&self, message: T) -> Result<G1Point, BLSError> {
        let point = hash_to_curve(&message)?;
        let input = [&point.0[..], &self.0[..]].concat();

        let mut g1_sol_uncompressed = [0x00u8; 64];
        g1_sol_uncompressed.clone_from_slice(
            &alt_bn128_multiplication(&input).map_err(|_| BLSError::BLSSigningError)?,
        );

        Ok(G1Point(g1_sol_uncompressed))
    }

    /// Derive the public key in G2 from this private key.
    #[cfg(not(target_os = "solana"))]
    pub fn public_key(&self) -> Result<G2Point, BLSError> {
        G2Point::try_from(self)
    }

    /// Sign a PoP message derived from the canonical (compressed) public key.
    #[cfg(not(target_os = "solana"))]
    pub fn proof_of_possession(&self) -> Result<G1Point, BLSError> {
        let pubkey = G2Point::try_from(self)?;

        // Use a canonical, compressed encoding to avoid malleability
        let pk_compressed = G2CompressedPoint::try_from(&pubkey)?;
        let msg = [POP_DOMAIN, &pk_compressed.0].concat();

        self.sign(msg)
    }
}

/// Helper to verify a PoP against a public key
pub fn verify_proof_of_possession(pubkey: &G2Point, pop: &G1Point) -> Result<(), BLSError> {
    let pk_compressed = G2CompressedPoint::try_from(pubkey)?;
    let msg = [POP_DOMAIN, &pk_compressed.0].concat();
    pubkey.verify(pop, msg)
}


#[cfg(test)]
mod tests {
    use crate::bls12254::errors::BLSError;
    use crate::bls12254::min_sig::g1::{G1Point, G1CompressedPoint};
    use crate::bls12254::min_sig::g2::G2Point;
    use crate::bls12254::min_sig::privkey::{PrivKey, verify_proof_of_possession};
    use hex_literal::hex;
    use rand::{Error as RandError, RngCore};

    struct SequenceRng {
        values: Vec<[u8; 32]>,
        index: usize,
    }

    impl SequenceRng {
        fn new(values: Vec<[u8; 32]>) -> Self {
            Self { values, index: 0 }
        }
    }

    impl RngCore for SequenceRng {
        fn next_u32(&mut self) -> u32 {
            0
        }

        fn next_u64(&mut self) -> u64 {
            0
        }

        fn fill_bytes(&mut self, dest: &mut [u8]) {
            let value = self.values[self.index];
            self.index += 1;
            dest.copy_from_slice(&value);
        }

        fn try_fill_bytes(&mut self, dest: &mut [u8]) -> Result<(), RandError> {
            self.fill_bytes(dest);
            Ok(())
        }
    }

    #[test]
    fn from_rng_rejects_zero_and_scalar_modulus() {
        let zero = [0u8; 32];
        let scalar_modulus = hex!(
            "30644e72e131a029b85045b68181585d2833e84879b9709143e1f593f0000001"
        );
        let mut valid = [0u8; 32];
        valid[31] = 1;

        let mut rng = SequenceRng::new(vec![zero, scalar_modulus, valid]);
        let key = PrivKey::from_rng(&mut rng);

        assert_eq!(key.0, valid);
        assert_eq!(rng.index, 3);
    }

    #[test]
    fn try_from_bytes_rejects_invalid_scalars() {
        let zero = [0u8; 32];
        let scalar_modulus = hex!(
            "30644e72e131a029b85045b68181585d2833e84879b9709143e1f593f0000001"
        );
        let mut valid = [0u8; 32];
        valid[31] = 1;

        assert_eq!(
            PrivKey::try_from_bytes(zero).unwrap_err(),
            BLSError::SecretKeyError
        );
        assert_eq!(
            PrivKey::try_from_bytes(scalar_modulus).unwrap_err(),
            BLSError::SecretKeyError
        );
        assert_eq!(PrivKey::try_from_bytes(valid).unwrap().0, valid);
    }

    #[test]
    fn sign_and_verify_random() {
        let sk = PrivKey::from_random();
        let msg = b"sign-verify";
        let sig = sk.sign(msg).expect("sign");
        let pk = G2Point::try_from(&sk).expect("g2 from sk");

        assert_eq!(pk, sk.public_key().expect("to pubkey"));

        pk.verify(&sig, msg).expect("verify");
    }

    #[test]
    fn signature_fails_on_wrong_message() {
        let sk = PrivKey::from_random();
        let m1 = b"a";
        let m2 = b"b";
        let sig = sk.sign(m1).expect("sign");
        let pk = G2Point::try_from(&sk).expect("g2 from sk");
        let err = pk.verify(&sig, m2).unwrap_err();
        assert_eq!(err, BLSError::BLSVerificationError);
    }

    #[test]
    fn compressed_signature_roundtrip() {
        let sk = PrivKey::from_random();
        let msg = b"compress-rt";
        let sig = sk.sign(msg).expect("sign");
        let sig_c = G1CompressedPoint::try_from(sig.clone()).expect("compress");
        let sig_rt = G1Point::try_from(&sig_c).expect("decompress");
        assert_eq!(sig.0, sig_rt.0, "sig compress/decompress mismatch");
    }

    #[test]
    fn proof_of_possession_roundtrip() {
        let sk = PrivKey::from_random();
        let pk = G2Point::try_from(&sk).expect("g2 from sk");
        let pop = sk.proof_of_possession().expect("pop sign");
        verify_proof_of_possession(&pk, &pop).expect("pop verify");

        // Negative test: PoP should not verify under a different public key
        let sk2 = PrivKey::from_random();
        let pk2 = G2Point::try_from(&sk2).expect("g2 from sk2");
        let err = verify_proof_of_possession(&pk2, &pop).unwrap_err();
        assert_eq!(err, BLSError::BLSVerificationError);
    }
}
