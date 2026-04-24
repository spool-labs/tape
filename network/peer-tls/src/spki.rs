//! Ed25519 SubjectPublicKeyInfo encoding helpers per RFC 8410.
//!
//! The SPKI DER for an Ed25519 public key is a fixed 44-byte blob:
//!
//! ```text
//! SEQUENCE (42)                              30 2A
//!   SEQUENCE (5)                             30 05
//!     OID id-Ed25519 (1.3.101.112)           06 03 2B 65 70
//!   BIT STRING (33)                          03 21 00
//!     <32-byte Ed25519 public key>           ..
//! ```
//!
//! The 32-byte payload is what we store on-chain as `NetworkTlsPubkey`;
//! everything else is fixed algorithm framing.

use tape_core::types::tls::NetworkTlsPubkey;

/// Total length of an Ed25519 SPKI DER blob.
pub const ED25519_SPKI_LEN: usize = 44;

/// Fixed DER prefix. The remaining 32 bytes of any valid SPKI are the raw
/// Ed25519 public key payload.
const ED25519_SPKI_PREFIX: [u8; 12] = [
    0x30, 0x2A, // SEQUENCE, length 42
    0x30, 0x05, // AlgorithmIdentifier SEQUENCE, length 5
    0x06, 0x03, 0x2B, 0x65, 0x70, // OID 1.3.101.112 (Ed25519)
    0x03, 0x21, 0x00, // BIT STRING, length 33, 0 unused bits
];

/// Construct the 44-byte DER SPKI for an Ed25519 public key.
pub fn encode_ed25519_spki(pubkey: &NetworkTlsPubkey) -> [u8; ED25519_SPKI_LEN] {
    let mut out = [0u8; ED25519_SPKI_LEN];
    out[..ED25519_SPKI_PREFIX.len()].copy_from_slice(&ED25519_SPKI_PREFIX);
    out[ED25519_SPKI_PREFIX.len()..].copy_from_slice(pubkey.as_bytes());
    out
}

/// Recover an Ed25519 `NetworkTlsPubkey` from a DER SPKI blob. Returns `None`
/// for any SPKI that is not exactly the Ed25519 shape (wrong OID, wrong
/// length, etc.).
pub fn decode_ed25519_spki(spki: &[u8]) -> Option<NetworkTlsPubkey> {
    if spki.len() != ED25519_SPKI_LEN {
        return None;
    }
    if spki[..ED25519_SPKI_PREFIX.len()] != ED25519_SPKI_PREFIX {
        return None;
    }
    let mut key = [0u8; 32];
    key.copy_from_slice(&spki[ED25519_SPKI_PREFIX.len()..]);
    Some(NetworkTlsPubkey::new(key))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_pubkey() {
        let pubkey = NetworkTlsPubkey::new_unique();
        let spki = encode_ed25519_spki(&pubkey);
        assert_eq!(spki.len(), ED25519_SPKI_LEN);
        assert_eq!(decode_ed25519_spki(&spki), Some(pubkey));
    }

    #[test]
    fn rejects_wrong_length() {
        assert!(decode_ed25519_spki(&[0u8; 43]).is_none());
        assert!(decode_ed25519_spki(&[0u8; 45]).is_none());
    }

    #[test]
    fn rejects_wrong_oid() {
        let mut spki = encode_ed25519_spki(&NetworkTlsPubkey::new_unique());
        spki[6] = 0xFF;
        assert!(decode_ed25519_spki(&spki).is_none());
    }

    #[test]
    fn rejects_algorithm_mismatch_with_matching_trailing_bytes() {
        // A cert with a different algorithm OID but identical trailing 32 bytes
        // must NOT be accepted. This is the exact failure mode of the naive
        // "take last 32 bytes" verifier.
        let pubkey = NetworkTlsPubkey::new([7u8; 32]);
        let mut spki = encode_ed25519_spki(&pubkey);
        spki[4] = 0x06;
        spki[5] = 0x03;
        spki[6] = 0x2A;
        spki[7] = 0x86;
        spki[8] = 0x48;
        assert!(decode_ed25519_spki(&spki).is_none());
    }

    #[test]
    fn prefix_length_matches_expected() {
        assert_eq!(ED25519_SPKI_PREFIX.len() + 32, ED25519_SPKI_LEN);
    }
}
