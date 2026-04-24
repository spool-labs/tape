use p256::elliptic_curve::sec1::FromEncodedPoint;
use p256::{EncodedPoint, PublicKey};

use super::P256_PUBKEY_LEN;
use super::error::P256Error;

/// Validate that `bytes` encodes a point on the P-256 curve. Used by client
/// verifiers that just decoded an on-chain `network_tls` field and want to
/// reject junk before doing an SPKI comparison against a TLS leaf cert.
pub fn validate_uncompressed_pubkey(bytes: &[u8; P256_PUBKEY_LEN]) -> Result<(), P256Error> {
    let mut sec1 = [0u8; 65];
    sec1[0] = 0x04;
    sec1[1..].copy_from_slice(bytes);
    let point = EncodedPoint::from_bytes(&sec1[..])
        .map_err(|_| P256Error::InvalidPublicKey)?;
    let pk: Option<PublicKey> = PublicKey::from_encoded_point(&point).into();
    pk.ok_or(P256Error::InvalidPublicKey).map(|_| ())
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::keypair::Keypair;

    #[test]
    fn accepts_generated_pubkey() {
        let mut rng = rand::thread_rng();
        let kp = Keypair::generate(&mut rng);
        validate_uncompressed_pubkey(&kp.public_key_bytes()).expect("on-curve");
    }

    #[test]
    fn rejects_zero_bytes() {
        let zeros = [0u8; 64];
        assert!(validate_uncompressed_pubkey(&zeros).is_err());
    }

    #[test]
    fn rejects_random_junk() {
        let junk = [0xA5u8; 64];
        assert!(validate_uncompressed_pubkey(&junk).is_err());
    }
}
