//! Ed25519 SubjectPublicKeyInfo (SPKI) encoding helpers per RFC 8410.

use tape_crypto::address::Address;

/// Length of an Ed25519 SPKI DER blob.
pub const ED25519_SPKI_LEN: usize = 44;

/// DER prefix for Ed25519 SPKI: `SEQUENCE { SEQUENCE { OID 1.3.101.112 }, BIT STRING ... }`.
const ED25519_SPKI_PREFIX: [u8; 12] = [
    0x30, 0x2A, // SEQUENCE, length 42
    0x30, 0x05, // SEQUENCE, length 5 (AlgorithmIdentifier)
    0x06, 0x03, 0x2B, 0x65, 0x70, // OID 1.3.101.112 (Ed25519)
    0x03, 0x21, 0x00, // BIT STRING, length 33, 0 unused bits
];

/// Construct the 44-byte DER SPKI for an Ed25519 public key.
pub fn encode_ed25519_spki(address: &Address) -> [u8; ED25519_SPKI_LEN] {
    let mut out = [0u8; ED25519_SPKI_LEN];
    out[..12].copy_from_slice(&ED25519_SPKI_PREFIX);
    out[12..].copy_from_slice(address.as_bytes());
    out
}

/// Recover an Ed25519 `Address` from a DER SPKI blob. Returns `None` for any
/// SPKI that is not exactly the Ed25519 shape (wrong OID, wrong length, etc.).
pub fn decode_ed25519_spki(spki: &[u8]) -> Option<Address> {
    if spki.len() != ED25519_SPKI_LEN {
        return None;
    }
    if spki[..12] != ED25519_SPKI_PREFIX {
        return None;
    }
    let mut key = [0u8; 32];
    key.copy_from_slice(&spki[12..]);
    Some(Address::from(key))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_address() {
        let address = Address::new_unique();
        let spki = encode_ed25519_spki(&address);
        assert_eq!(spki.len(), ED25519_SPKI_LEN);
        assert_eq!(decode_ed25519_spki(&spki), Some(address));
    }

    #[test]
    fn rejects_wrong_length() {
        assert!(decode_ed25519_spki(&[0u8; 43]).is_none());
        assert!(decode_ed25519_spki(&[0u8; 45]).is_none());
    }

    #[test]
    fn rejects_wrong_oid() {
        let mut spki = encode_ed25519_spki(&Address::new_unique());
        spki[6] = 0xFF; // corrupt OID byte
        assert!(decode_ed25519_spki(&spki).is_none());
    }

    #[test]
    fn rejects_algorithm_mismatch_with_matching_trailing_bytes() {
        // A cert with a different algorithm OID but identical trailing 32 bytes
        // must NOT be accepted. This is the exact failure mode of the old
        // "take last 32 bytes" verifier.
        let address = Address::from([7u8; 32]);
        let mut spki = encode_ed25519_spki(&address);
        spki[4] = 0x06;
        spki[5] = 0x03;
        spki[6] = 0x2A; // swap to a different OID prefix
        spki[7] = 0x86;
        spki[8] = 0x48;
        assert!(decode_ed25519_spki(&spki).is_none());
    }

    #[test]
    fn fixed_prefix_length() {
        assert_eq!(ED25519_SPKI_PREFIX.len(), 12);
    }
}
