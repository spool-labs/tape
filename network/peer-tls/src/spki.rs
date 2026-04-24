//! P-256 (secp256r1) SubjectPublicKeyInfo encoding helpers per RFC 5480.
//!
//! The SPKI DER for a P-256 public key is a fixed 91-byte blob:
//!
//! ```text
//! SEQUENCE (91)                                     30 59
//!   SEQUENCE (19)                                   30 13
//!     OID id-ecPublicKey (1.2.840.10045.2.1)        06 07 2A 86 48 CE 3D 02 01
//!     OID prime256v1     (1.2.840.10045.3.1.7)      06 08 2A 86 48 CE 3D 03 01 07
//!   BIT STRING (66)                                 03 42 00
//!     04 || x(32) || y(32)                          <65 bytes>
//! ```
//!
//! The 64-byte `x || y` payload is what we store on-chain as
//! `NetworkTlsPubkey`; everything else is fixed algorithm/parameter framing.

use tape_core::types::tls::NetworkTlsPubkey;

/// Total length of a P-256 SPKI DER blob.
pub const P256_SPKI_LEN: usize = 91;

/// Fixed DER prefix up to and including the uncompressed-point tag byte.
/// The remaining 64 bytes of any valid SPKI are the `x || y` pubkey payload.
const P256_SPKI_PREFIX: [u8; 27] = [
    // outer SEQUENCE, length 89
    0x30, 0x59,
    // AlgorithmIdentifier SEQUENCE, length 19
    0x30, 0x13,
    // OID 1.2.840.10045.2.1 (id-ecPublicKey)
    0x06, 0x07, 0x2A, 0x86, 0x48, 0xCE, 0x3D, 0x02, 0x01,
    // OID 1.2.840.10045.3.1.7 (prime256v1 / secp256r1)
    0x06, 0x08, 0x2A, 0x86, 0x48, 0xCE, 0x3D, 0x03, 0x01, 0x07,
    // BIT STRING, length 66, 0 unused bits, uncompressed-point tag
    0x03, 0x42, 0x00, 0x04,
];

/// Construct the 91-byte DER SPKI for a P-256 public key.
pub fn encode_p256_spki(pubkey: &NetworkTlsPubkey) -> [u8; P256_SPKI_LEN] {
    let mut out = [0u8; P256_SPKI_LEN];
    out[..P256_SPKI_PREFIX.len()].copy_from_slice(&P256_SPKI_PREFIX);
    out[P256_SPKI_PREFIX.len()..].copy_from_slice(pubkey.as_bytes());
    out
}

/// Recover a P-256 `NetworkTlsPubkey` from a DER SPKI blob. Returns `None`
/// for any SPKI that is not exactly the P-256 / prime256v1 / uncompressed
/// shape (wrong OID, wrong curve, compressed point, wrong length, etc.).
///
/// Does NOT verify that the recovered `x || y` lies on the curve. Callers
/// that handle untrusted bytes should run [`tape_crypto::p256::validate_uncompressed_pubkey`]
/// on the result.
pub fn decode_p256_spki(spki: &[u8]) -> Option<NetworkTlsPubkey> {
    if spki.len() != P256_SPKI_LEN {
        return None;
    }
    if spki[..P256_SPKI_PREFIX.len()] != P256_SPKI_PREFIX {
        return None;
    }
    let mut key = [0u8; 64];
    key.copy_from_slice(&spki[P256_SPKI_PREFIX.len()..]);
    Some(NetworkTlsPubkey::new(key))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_pubkey() {
        let pubkey = NetworkTlsPubkey::new_unique();
        let spki = encode_p256_spki(&pubkey);
        assert_eq!(spki.len(), P256_SPKI_LEN);
        assert_eq!(decode_p256_spki(&spki), Some(pubkey));
    }

    #[test]
    fn rejects_wrong_length() {
        assert!(decode_p256_spki(&[0u8; 90]).is_none());
        assert!(decode_p256_spki(&[0u8; 92]).is_none());
    }

    #[test]
    fn rejects_wrong_alg_oid() {
        let mut spki = encode_p256_spki(&NetworkTlsPubkey::new_unique());
        // Corrupt a byte inside the id-ecPublicKey OID.
        spki[6] = 0xFF;
        assert!(decode_p256_spki(&spki).is_none());
    }

    #[test]
    fn rejects_wrong_curve_oid() {
        let mut spki = encode_p256_spki(&NetworkTlsPubkey::new_unique());
        // Corrupt a byte inside the prime256v1 curve OID.
        spki[18] = 0xFF;
        assert!(decode_p256_spki(&spki).is_none());
    }

    #[test]
    fn rejects_compressed_point() {
        let mut spki = encode_p256_spki(&NetworkTlsPubkey::new_unique());
        // Flip the uncompressed-point tag (0x04) to a compressed-point tag (0x02).
        spki[26] = 0x02;
        assert!(decode_p256_spki(&spki).is_none());
    }

    #[test]
    fn prefix_length_matches_expected() {
        assert_eq!(P256_SPKI_PREFIX.len() + 64, P256_SPKI_LEN);
    }

    #[test]
    fn rejects_ed25519_spki_shape() {
        // Ed25519 SPKI is 44 bytes starting 30 2A 30 05 06 03 2B 65 70 — a P-256
        // decoder must reject it even if the trailing bytes happen to match.
        let mut fake = [0u8; P256_SPKI_LEN];
        fake[0] = 0x30;
        fake[1] = 0x2A;
        fake[2] = 0x30;
        fake[3] = 0x05;
        fake[4] = 0x06;
        fake[5] = 0x03;
        fake[6] = 0x2B;
        fake[7] = 0x65;
        fake[8] = 0x70;
        assert!(decode_p256_spki(&fake).is_none());
    }
}
