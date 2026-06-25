use dashu::integer::UBig;
use solana_bn254::compression::prelude::alt_bn128_g1_decompress;

use crate::bls12254::{MODULUS, NORMALIZE_MODULUS};
use crate::bls12254::errors::BLSError;
use super::g1::G1Point;

fn to_be_32(value: &UBig) -> Option<[u8; 32]> {
    let bytes = value.to_be_bytes();
    if bytes.len() > 32 {
        return None;
    }

    let mut out = [0u8; 32];
    out[32 - bytes.len()..].copy_from_slice(&bytes);
    Some(out)
}

pub fn hash_to_curve<T: AsRef<[u8]>>(message: T) -> Result<G1Point, BLSError> {
    (0u8..=u8::MAX)
        .find_map(|n: u8| {

            // SHA-256 via the sol_sha256 syscall (1 stack frame). The pure-Rust
            // solana_nostd_sha256 compiles to a ~64-deep call chain under the v3
            // (platform-tools v1.54) toolchain and trips MAX_CALL_DEPTH at runtime.
            #[cfg(target_os = "solana")]
            let hash = solana_program::hash::hashv(&[b"BLS-BN254-RO", message.as_ref(), &[n]])
                .to_bytes();
            #[cfg(not(target_os = "solana"))]
            let hash = solana_sha256_hasher::hashv(&[b"BLS-BN254-RO", message.as_ref(), &[n]])
                .to_bytes();

            let hash_ubig = UBig::from_be_bytes(&hash);

            if hash_ubig >= NORMALIZE_MODULUS {
                return None;
            }

            let modulus_ubig = hash_ubig % &MODULUS;
            let compressed = to_be_32(&modulus_ubig)?;
            if compressed == [0u8; 32] {
                return None;
            }

            match alt_bn128_g1_decompress(&compressed) {
                Ok(p) if p != [0u8; 64] => Some(G1Point(p)),
                Err(_) => None,
                _ => None,
            }
        })
        .ok_or(BLSError::HashToCurveError)
}

#[cfg(test)]
mod tests {
    use super::{hash_to_curve, to_be_32};
    use crate::bls12254::min_sig::g1::{G1CompressedPoint, G1Point};
    use dashu::integer::UBig;

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

    #[test]
    fn to_be_32_left_pads_short_values() {
        let encoded = to_be_32(&UBig::from(1u8)).expect("encode");
        let mut expected = [0u8; 32];
        expected[31] = 1;

        assert_eq!(encoded, expected);
    }

    #[test]
    fn to_be_32_rejects_oversized_values() {
        let value = UBig::from_be_bytes(&[0xffu8; 33]);

        assert!(to_be_32(&value).is_none());
    }
}
