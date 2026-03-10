use crate::ed25519::{
    consts::{ED25519_PUBKEY_LEN, ED25519_SIG_LEN, G},
    errors::SignatureError,
    utils::{is_small_order, split_signature},
};

use curve25519_dalek::scalar::Scalar;
use sha2::{Digest, Sha512};

use solana_curve25519::{
    edwards::{multiply_edwards, subtract_edwards, validate_edwards, PodEdwardsPoint},
    scalar::PodScalar,
};

/// Verify an ed25519 signature.
#[allow(non_snake_case)]
pub fn sig_verify(pubkey: &[u8], sig: &[u8], message: &[u8]) -> Result<(), SignatureError> {
    if pubkey.len() != ED25519_PUBKEY_LEN {
        return Err(SignatureError::InvalidArgument);
    }

    if sig.len() != ED25519_SIG_LEN {
        return Err(SignatureError::InvalidArgument);
    }

    let pubkey_point = PodEdwardsPoint(pubkey[..ED25519_PUBKEY_LEN].try_into().unwrap());
    let (sig_lower, sig_upper) = split_signature(sig.try_into().unwrap());

    let sig_R = PodEdwardsPoint(sig_lower);
    let sig_s: Scalar = Option::from(Scalar::from_canonical_bytes(sig_upper))
        .ok_or(SignatureError::InvalidSignature)?;

    if is_small_order(&sig_R) || is_small_order(&pubkey_point) {
        return Err(SignatureError::InvalidAccountOwner);
    }

    // Optional additional validation (PodEdwardsPoint decompression already checks this)
    let pubkey_on_curve = validate_edwards(&pubkey_point);
    let sig_R_on_curve = validate_edwards(&sig_R);
    if !pubkey_on_curve || !sig_R_on_curve {
        return Err(SignatureError::InvalidAccountOwner);
    }

    let mut h: Sha512 = Sha512::new();
    h.update(sig_R.0);
    h.update(&pubkey);
    h.update(&message);

    let f = h.finalize();
    let k = Scalar::from_bytes_mod_order_wide(f.as_ref());

    let k_bytes = k.to_bytes();
    let pubkey_bytes = pubkey_point.0;
    let sig_s_bytes = sig_s.to_bytes();

    let a = PodScalar(k_bytes);
    let b = PodScalar(sig_s_bytes);
    let B = PodEdwardsPoint(G);

    // R = sB - kA
    let sB = multiply_edwards(&b, &B).ok_or(SignatureError::InvalidSignature)?;
    let kA = multiply_edwards(&a, &PodEdwardsPoint(pubkey_bytes)).ok_or(SignatureError::InvalidSignature)?;
    let R = subtract_edwards(&sB, &kA).ok_or(SignatureError::InvalidSignature)?;

    let expected_R = sig_R.0;
    let computed_R = R.0;

    if expected_R == computed_R {
        Ok(())
    } else {
        Err(SignatureError::InvalidAccountOwner)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use curve25519_dalek::constants;
    use solana_curve25519::edwards::PodEdwardsPoint;
    use crate::ed25519::consts as consts;
    use crate::ed25519::utils as utils;

    #[test]
    fn test_base_point() {
        let base_point = constants::ED25519_BASEPOINT_POINT;
        let compressed = base_point.compress();
        let bytes = compressed.to_bytes();
        assert_eq!(bytes, consts::G);
    }

    #[test]
    fn test_small_order() {
        // Base point (has large order)
        let base_point_bytes = consts::G;
        let base_point = PodEdwardsPoint(base_point_bytes);
        assert_eq!(utils::is_small_order(&base_point), false);

        // Torsion points (have small order)
        for i in 0..8 {
            let torsion_point = constants::EIGHT_TORSION[i];
            let compressed = torsion_point.compress();
            let torsion_point_bytes = compressed.to_bytes();
            let torsion_point = PodEdwardsPoint(torsion_point_bytes);
            assert_eq!(utils::is_small_order(&torsion_point), true);
        }
    }

    #[test]
    fn test_hello_world() {
        let pubkey: [u8; 32] = [
            73, 73, 170, 112, 75, 235, 154, 81, 203, 8, 44, 245, 233, 18, 204, 136,
            162, 9, 233, 49, 154, 201, 171, 175, 47, 6, 223, 101, 105, 80, 95, 166
        ];
        let sig: [u8; 64] = [
            164, 121, 89, 242, 88, 29, 80, 177, 104, 20, 102, 176, 48, 133, 68, 8,
            105, 33, 58, 86, 28, 108, 198, 140, 160, 219, 62, 184, 154, 181, 140, 33,
            35, 102, 183, 203, 111, 33, 55, 170, 180, 138, 92, 196, 185, 201, 122, 167,
            15, 112, 9, 228, 226, 112, 111, 10, 142, 73, 85, 43, 81, 152, 204, 13
        ];

        assert!(sig_verify(&pubkey, &sig, "hello world".as_bytes()).is_ok());
        assert!(sig_verify(&pubkey, &sig, "not the right message".as_bytes()).is_err());
    }

    #[test]
    fn test_vector_1() {
        let pubkey : [u8; 32] = [
            0xd7, 0x5a, 0x98, 0x01, 0x82, 0xb1, 0x0a, 0xb7, 0xd5, 0x4b, 0xfe, 0xd3, 0xc9, 0x64, 0x07, 0x3a,
            0x0e, 0xe1, 0x72, 0xf3, 0xda, 0xa6, 0x23, 0x25, 0xaf, 0x02, 0x1a, 0x68, 0xf7, 0x07, 0x51, 0x1a,
        ];

        let sig : [u8; 64] = [
            0xe5, 0x56, 0x43, 0x00, 0xc3, 0x60, 0xac, 0x72, 0x90, 0x86, 0xe2, 0xcc, 0x80, 0x6e, 0x82, 0x8a,
            0x84, 0x87, 0x7f, 0x1e, 0xb8, 0xe5, 0xd9, 0x74, 0xd8, 0x73, 0xe0, 0x65, 0x22, 0x49, 0x01, 0x55,
            0x5f, 0xb8, 0x82, 0x15, 0x90, 0xa3, 0x3b, 0xac, 0xc6, 0x1e, 0x39, 0x70, 0x1c, 0xf9, 0xb4, 0x6b,
            0xd2, 0x5b, 0xf5, 0xf0, 0x59, 0x5b, 0xbe, 0x24, 0x65, 0x51, 0x41, 0x43, 0x8e, 0x7a, 0x10, 0x0b,
        ];

        let message = "".as_bytes();

        assert!(sig_verify(&pubkey, &sig, message).is_ok());
        assert!(sig_verify(&pubkey, &sig, "not the right message".as_bytes()).is_err());
    }

    #[test]
    fn test_vector_2() {
        let pubkey : [u8; 32] = [
            0x3d, 0x40, 0x17, 0xc3, 0xe8, 0x43, 0x89, 0x5a, 0x92, 0xb7, 0x0a, 0xa7, 0x4d, 0x1b, 0x7e, 0xbc,
            0x9c, 0x98, 0x2c, 0xcf, 0x2e, 0xc4, 0x96, 0x8c, 0xc0, 0xcd, 0x55, 0xf1, 0x2a, 0xf4, 0x66, 0x0c,
        ];

        let sig : [u8; 64] = [
            0x92, 0xa0, 0x09, 0xa9, 0xf0, 0xd4, 0xca, 0xb8, 0x72, 0x0e, 0x82, 0x0b, 0x5f, 0x64, 0x25, 0x40,
            0xa2, 0xb2, 0x7b, 0x54, 0x16, 0x50, 0x3f, 0x8f, 0xb3, 0x76, 0x22, 0x23, 0xeb, 0xdb, 0x69, 0xda,
            0x08, 0x5a, 0xc1, 0xe4, 0x3e, 0x15, 0x99, 0x6e, 0x45, 0x8f, 0x36, 0x13, 0xd0, 0xf1, 0x1d, 0x8c,
            0x38, 0x7b, 0x2e, 0xae, 0xb4, 0x30, 0x2a, 0xee, 0xb0, 0x0d, 0x29, 0x16, 0x12, 0xbb, 0x0c, 0x00,
        ];

        let message = "r".as_bytes(); // r = 72

        assert!(sig_verify(&pubkey, &sig, message).is_ok());
        assert!(sig_verify(&pubkey, &sig, "not the right message".as_bytes()).is_err());
    }

    #[test]
    fn test_vector_3() {
        let pubkey : [u8; 32] = [
            0xfc, 0x51, 0xcd, 0x8e, 0x62, 0x18, 0xa1, 0xa3, 0x8d, 0xa4, 0x7e, 0xd0, 0x02, 0x30, 0xf0, 0x58,
            0x08, 0x16, 0xed, 0x13, 0xba, 0x33, 0x03, 0xac, 0x5d, 0xeb, 0x91, 0x15, 0x48, 0x90, 0x80, 0x25,
        ];

        let sig : [u8; 64] = [
            0x62, 0x91, 0xd6, 0x57, 0xde, 0xec, 0x24, 0x02, 0x48, 0x27, 0xe6, 0x9c, 0x3a, 0xbe, 0x01, 0xa3,
            0x0c, 0xe5, 0x48, 0xa2, 0x84, 0x74, 0x3a, 0x44, 0x5e, 0x36, 0x80, 0xd7, 0xdb, 0x5a, 0xc3, 0xac,
            0x18, 0xff, 0x9b, 0x53, 0x8d, 0x16, 0xf2, 0x90, 0xae, 0x67, 0xf7, 0x60, 0x98, 0x4d, 0xc6, 0x59,
            0x4a, 0x7c, 0x15, 0xe9, 0x71, 0x6e, 0xd2, 0x8d, 0xc0, 0x27, 0xbe, 0xce, 0xea, 0x1e, 0xc4, 0x0a,
        ];

        let message = &[0xaf, 0x82];

        assert!(sig_verify(&pubkey, &sig, message).is_ok());
        assert!(sig_verify(&pubkey, &sig, "not the right message".as_bytes()).is_err());
    }
}
