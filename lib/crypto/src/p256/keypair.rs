use p256::elliptic_curve::sec1::ToEncodedPoint;
use p256::pkcs8::{DecodePrivateKey, EncodePrivateKey, LineEnding};
use p256::{PublicKey, SecretKey};
use rand::{CryptoRng, RngCore};

use super::P256_PUBKEY_LEN;
use super::error::P256Error;

/// A P-256 private/public keypair used as the node's TLS identity.
pub struct Keypair {
    secret: SecretKey,
    public: PublicKey,
}

impl Keypair {
    /// Generate a fresh random keypair.
    pub fn generate<R: CryptoRng + RngCore>(rng: &mut R) -> Self {
        let secret = SecretKey::random(rng);
        let public = secret.public_key();
        Self { secret, public }
    }

    /// Decode a keypair from PKCS#8 PEM (the `-----BEGIN PRIVATE KEY-----`
    /// format emitted by `openssl`, `certbot`, and [`Self::to_pkcs8_pem`]).
    pub fn from_pkcs8_pem(pem: &str) -> Result<Self, P256Error> {
        let secret = SecretKey::from_pkcs8_pem(pem)
            .map_err(|e| P256Error::Pkcs8(e.to_string()))?;
        let public = secret.public_key();
        Ok(Self { secret, public })
    }

    /// Decode a keypair from PKCS#8 DER.
    pub fn from_pkcs8_der(der: &[u8]) -> Result<Self, P256Error> {
        let secret = SecretKey::from_pkcs8_der(der)
            .map_err(|e| P256Error::Pkcs8(e.to_string()))?;
        let public = secret.public_key();
        Ok(Self { secret, public })
    }

    /// Encode the private key as PKCS#8 PEM. LF line endings, operator-editable.
    pub fn to_pkcs8_pem(&self) -> Result<String, P256Error> {
        self.secret
            .to_pkcs8_pem(LineEnding::LF)
            .map(|z| z.as_str().to_owned())
            .map_err(|e| P256Error::Pkcs8(e.to_string()))
    }

    /// Encode the private key as PKCS#8 DER.
    pub fn to_pkcs8_der(&self) -> Result<Vec<u8>, P256Error> {
        self.secret
            .to_pkcs8_der()
            .map(|doc| doc.as_bytes().to_vec())
            .map_err(|e| P256Error::Pkcs8(e.to_string()))
    }

    /// Uncompressed SEC1 public key as `x || y` (64 bytes, no `0x04` prefix).
    ///
    /// This is exactly the byte layout stored on-chain in `NetworkTlsPubkey`.
    pub fn public_key_bytes(&self) -> [u8; P256_PUBKEY_LEN] {
        let point = self.public.to_encoded_point(false);
        let bytes = point.as_bytes();
        // SEC1 uncompressed is always 65 bytes: 0x04 || x(32) || y(32).
        debug_assert_eq!(bytes.len(), 65);
        debug_assert_eq!(bytes[0], 0x04);
        let mut out = [0u8; P256_PUBKEY_LEN];
        out.copy_from_slice(&bytes[1..]);
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generate_round_trips_through_pem() {
        let mut rng = rand::thread_rng();
        let kp = Keypair::generate(&mut rng);
        let pem = kp.to_pkcs8_pem().expect("encode");
        assert!(pem.starts_with("-----BEGIN PRIVATE KEY-----"));
        let kp2 = Keypair::from_pkcs8_pem(&pem).expect("decode");
        assert_eq!(kp.public_key_bytes(), kp2.public_key_bytes());
    }

    #[test]
    fn generate_round_trips_through_der() {
        let mut rng = rand::thread_rng();
        let kp = Keypair::generate(&mut rng);
        let der = kp.to_pkcs8_der().expect("encode");
        let kp2 = Keypair::from_pkcs8_der(&der).expect("decode");
        assert_eq!(kp.public_key_bytes(), kp2.public_key_bytes());
    }

    #[test]
    fn public_key_is_64_bytes() {
        let mut rng = rand::thread_rng();
        let kp = Keypair::generate(&mut rng);
        assert_eq!(kp.public_key_bytes().len(), P256_PUBKEY_LEN);
    }

    #[test]
    fn from_pkcs8_rejects_garbage() {
        assert!(Keypair::from_pkcs8_pem("not a pem").is_err());
        assert!(Keypair::from_pkcs8_der(&[0u8; 8]).is_err());
    }
}
