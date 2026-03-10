use dashu::integer::UBig;
use solana_bn254::compression::prelude::alt_bn128_g1_decompress;

use crate::bls12254::{MODULUS, NORMALIZE_MODULUS};
use crate::bls12254::errors::BLSError;
use super::g1::G1Point;

pub fn hash_to_curve<T: AsRef<[u8]>>(message: T) -> Result<G1Point, BLSError> {
    (0..255)
        .find_map(|n: u8| {

            let hash = solana_nostd_sha256::hashv(&[
                b"BLS-BN254-RO",
                message.as_ref(),
                &[n]
            ]);

            let hash_ubig = UBig::from_be_bytes(&hash);

            if hash_ubig >= NORMALIZE_MODULUS {
                return None;
            }

            let modulus_ubig = hash_ubig % &MODULUS;

            match alt_bn128_g1_decompress(&modulus_ubig.to_be_bytes()) {
                Ok(p) => Some(G1Point(p)),
                Err(_) => None,
            }
        })
        .ok_or(BLSError::HashToCurveError)
}

#[cfg(test)]
mod tests {
    use super::hash_to_curve;
    use crate::bls12254::min_sig::g1::{G1CompressedPoint, G1Point};

    #[test]
    fn hash_to_curve_is_deterministic() {
        let m = b"hash-determinism";
        let h1 = hash_to_curve(m).expect("h1");
        let h2 = hash_to_curve(m).expect("h2");
        assert_eq!(h1.0, h2.0);
    }

    #[test]
    fn hash_to_curve_compress_decompress_roundtrip() {
        let m = b"hash-roundtrip";
        let h = hash_to_curve(m).expect("hash");
        let hc = G1CompressedPoint::try_from(h.clone()).expect("compress");
        let rt = G1Point::try_from(&hc).expect("decompress");
        assert_eq!(h.0, rt.0);
    }

    #[test]
    fn hash_to_curve_changes_with_message() {
        let h1 = hash_to_curve(b"m1").expect("h1");
        let h2 = hash_to_curve(b"m2").expect("h2");
        assert_ne!(h1.0, h2.0);
    }
}
