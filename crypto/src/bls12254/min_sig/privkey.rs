#[cfg(not(target_os = "solana"))]
use rand::RngCore;

use solana_bn254::prelude::alt_bn128_multiplication;

#[cfg(not(target_os = "solana"))]
use crate::bls12254::MODULUS;

use crate::bls12254::errors::BLSError;
use super::g1::G1Point;
use super::hash::hash_to_curve;

pub struct PrivKey(pub [u8; 32]);

impl PrivKey {
    #[cfg(not(target_os = "solana"))]
    pub fn from_random() -> PrivKey {
        loop {
            let mut bytes = [0u8; 32];
            rand::thread_rng().fill_bytes(&mut bytes);
            let num = dashu::integer::UBig::from_be_bytes(&bytes);
            if num < MODULUS {
                return Self(bytes);
            }
        }
    }

    pub fn sign<T: AsRef<[u8]>>(&self, message: T) -> Result<G1Point, BLSError> {
        let point = hash_to_curve(&message)?;
        let input = [&point.0[..], &self.0[..]].concat();

        let mut g1_sol_uncompressed = [0x00u8; 64];
        g1_sol_uncompressed.clone_from_slice(
            &alt_bn128_multiplication(&input).map_err(|_| BLSError::BLSSigningError)?,
        );

        Ok(G1Point(g1_sol_uncompressed))
    }
}

#[cfg(test)]
mod tests {
    use crate::bls12254::errors::BLSError;
    use crate::bls12254::min_sig::g1::{G1Point, G1CompressedPoint};
    use crate::bls12254::min_sig::g2::G2Point;
    use crate::bls12254::min_sig::privkey::PrivKey;

    #[test]
    fn sign_and_verify_random() {
        let sk = PrivKey::from_random();
        let msg = b"sign-verify";
        let sig = sk.sign(msg).expect("sign");
        let pk = G2Point::try_from(&sk).expect("g2 from sk");
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
}
