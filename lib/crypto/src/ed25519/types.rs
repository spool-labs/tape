//! Ed25519 signature types.
//!
//! This module provides wrapper types for Ed25519 cryptography:
//! - `SecretKey` - wrapper around `SigningKey` (off-chain only)
//! - `Pubkey` - wrapper around `VerificationKey` (off-chain only)
//! - `Signature` - wrapper around `ed25519_consensus::Signature` (off-chain only)
//! - `Keypair` - combines SecretKey and Pubkey (off-chain only)
//!
//! For on-chain signature verification, use `sig_verify` from `crate::ed25519::sig`.

#![allow(unexpected_cfgs)]

#[cfg(not(target_os = "solana"))]
use crate::address::Address;
#[cfg(not(target_os = "solana"))]
use std::path::Path;

// ed25519-consensus is only available off-chain (it brings in curve25519-dalek-ng
// which has stack size issues on SBF)
#[cfg(not(target_os = "solana"))]
use ed25519_consensus::{SigningKey, VerificationKey};
#[cfg(not(target_os = "solana"))]
use rand::CryptoRng;
#[cfg(not(target_os = "solana"))]
use serde::{Deserialize, Serialize};
#[cfg(not(target_os = "solana"))]
use serde_json;
#[cfg(not(target_os = "solana"))]
use solana_pubkey::Pubkey as SolanaPubkey;

use super::consts::{ED25519_PUBKEY_LEN, ED25519_SIG_LEN};
#[cfg(not(target_os = "solana"))]
use super::errors::{KeypairFileError, SignatureError};

/// Constant for pubkey length (32 bytes).
pub const PUBKEY_LEN: usize = ED25519_PUBKEY_LEN;

/// Constant for signature length (64 bytes).
pub const SIGNATURE_LEN: usize = ED25519_SIG_LEN;

/// Ed25519 secret key wrapper around `SigningKey`.
///
/// Used for signing messages off-chain. Not available on Solana.
#[cfg(not(target_os = "solana"))]
pub struct SecretKey(SigningKey);

#[cfg(not(target_os = "solana"))]
impl SecretKey {
    /// Generate a new random secret key.
    pub fn new<R: CryptoRng + rand::RngCore>(rng: &mut R) -> Self {
        Self(SigningKey::new(rng))
    }

    /// Create a secret key from raw bytes.
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(SigningKey::from(bytes))
    }

    /// Derive the public key from this secret key.
    pub fn public_key(&self) -> Pubkey {
        Pubkey(self.0.verification_key())
    }

    /// Sign a message with this secret key.
    pub fn sign(&self, msg: &[u8]) -> Signature {
        Signature(self.0.sign(msg))
    }

    /// Get the raw bytes of this secret key.
    pub fn as_bytes(&self) -> &[u8; 32] {
        self.0.as_bytes()
    }
}

/// Ed25519 public key wrapper around `VerificationKey`.
///
/// Provides verification and Solana Pubkey conversions.
/// Only available off-chain (uses ed25519-consensus).
#[cfg(not(target_os = "solana"))]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Pubkey(VerificationKey);

#[cfg(not(target_os = "solana"))]
impl Pubkey {
    /// The length of a public key in bytes.
    pub const LEN: usize = ED25519_PUBKEY_LEN;

    /// Create a public key from raw bytes.
    pub fn from_bytes(bytes: [u8; 32]) -> Result<Self, SignatureError> {
        VerificationKey::try_from(bytes)
            .map(Self)
            .map_err(|_| SignatureError::InvalidPublicKey)
    }

    /// Get a reference to the raw bytes of this public key.
    pub fn as_bytes(&self) -> &[u8; 32] {
        self.0.as_bytes()
    }

    /// Convert this public key to raw bytes.
    pub fn to_bytes(self) -> [u8; 32] {
        self.0.to_bytes()
    }

    pub fn address(self) -> Address {
        self.to_bytes().into()
    }

    /// Verify a signature on a message.
    pub fn verify(&self, msg: &[u8], sig: &Signature) -> Result<(), SignatureError> {
        self.0
            .verify(&sig.0, msg)
            .map_err(|_| SignatureError::VerificationFailed)
    }
}

// Solana Pubkey conversions

#[cfg(not(target_os = "solana"))]
impl From<Pubkey> for SolanaPubkey {
    fn from(pubkey: Pubkey) -> Self {
        Self::from(pubkey.to_bytes())
    }
}

#[cfg(not(target_os = "solana"))]
impl From<&Pubkey> for SolanaPubkey {
    fn from(pubkey: &Pubkey) -> Self {
        Self::from(*pubkey.as_bytes())
    }
}

#[cfg(not(target_os = "solana"))]
impl TryFrom<SolanaPubkey> for Pubkey {
    type Error = SignatureError;

    fn try_from(pubkey: SolanaPubkey) -> Result<Self, Self::Error> {
        Self::from_bytes(pubkey.to_bytes())
    }
}

#[cfg(not(target_os = "solana"))]
impl TryFrom<&SolanaPubkey> for Pubkey {
    type Error = SignatureError;

    fn try_from(pubkey: &SolanaPubkey) -> Result<Self, Self::Error> {
        Self::from_bytes(pubkey.to_bytes())
    }
}

#[cfg(not(target_os = "solana"))]
impl From<Pubkey> for Address {
    fn from(value: Pubkey) -> Self {
        value.address()
    }
}

#[cfg(not(target_os = "solana"))]
impl From<&Pubkey> for Address {
    fn from(value: &Pubkey) -> Self {
        value.to_bytes().into()
    }
}

#[cfg(not(target_os = "solana"))]
impl TryFrom<Address> for Pubkey {
    type Error = SignatureError;

    fn try_from(value: Address) -> Result<Self, Self::Error> {
        Self::from_bytes(value.to_bytes())
    }
}

#[cfg(not(target_os = "solana"))]
impl TryFrom<&Address> for Pubkey {
    type Error = SignatureError;

    fn try_from(value: &Address) -> Result<Self, Self::Error> {
        Self::from_bytes(value.to_bytes())
    }
}

/// Ed25519 signature wrapper around `ed25519_consensus::Signature`.
/// Only available off-chain (uses ed25519-consensus).
#[cfg(not(target_os = "solana"))]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Signature(ed25519_consensus::Signature);

#[cfg(not(target_os = "solana"))]
impl Signature {
    /// The length of a signature in bytes.
    pub const LEN: usize = ED25519_SIG_LEN;

    /// Create a signature from raw bytes.
    pub fn from_bytes(bytes: [u8; 64]) -> Result<Self, SignatureError> {
        ed25519_consensus::Signature::try_from(bytes)
            .map(Self)
            .map_err(|_| SignatureError::InvalidSignature)
    }

    /// Convert this signature to raw bytes.
    pub fn to_bytes(&self) -> [u8; 64] {
        self.0.to_bytes()
    }

    /// Verify this signature on a message with the given public key.
    pub fn verify(&self, msg: &[u8], pk: &Pubkey) -> Result<(), SignatureError> {
        pk.verify(msg, self)
    }
}

/// Ed25519 keypair combining a secret key and its derived public key.
///
/// Not available on Solana (signing is off-chain only).
#[cfg(not(target_os = "solana"))]
pub struct Keypair {
    secret: SecretKey,
    public: Pubkey,
}

#[cfg(not(target_os = "solana"))]
impl Keypair {
    /// Generate a new random keypair.
    pub fn new<R: CryptoRng + rand::RngCore>(rng: &mut R) -> Self {
        let secret = SecretKey::new(rng);
        let public = secret.public_key();
        Self { secret, public }
    }

    /// Create a keypair from a secret key.
    pub fn from_secret(secret: SecretKey) -> Self {
        let public = secret.public_key();
        Self { secret, public }
    }

    pub fn pubkey(&self) -> Pubkey {
        self.public
    }

    /// Get a reference to the public key.
    pub fn public_key(&self) -> &Pubkey {
        &self.public
    }

    pub fn address(&self) -> Address {
        self.pubkey().into()
    }

    /// Get a reference to the secret key.
    pub fn secret_key(&self) -> &SecretKey {
        &self.secret
    }

    /// Sign a message with this keypair.
    pub fn sign(&self, msg: &[u8]) -> Signature {
        self.secret.sign(msg)
    }

    pub fn to_keypair_bytes(&self) -> [u8; 64] {
        let mut bytes = [0u8; 64];
        bytes[..32].copy_from_slice(self.secret_key().as_bytes());
        bytes[32..].copy_from_slice(self.pubkey().as_bytes());
        bytes
    }

    pub fn try_from_keypair_slice(bytes: &[u8]) -> Result<Self, SignatureError> {
        let keypair_bytes: [u8; 64] = bytes
            .try_into()
            .map_err(|_| SignatureError::InvalidArgument)?;
        Self::from_keypair_bytes(keypair_bytes)
    }

    pub fn try_from_json_bytes(json: &[u8]) -> Result<Self, KeypairFileError> {
        let keypair_bytes: Vec<u8> =
            serde_json::from_slice(json).map_err(|error| KeypairFileError::JsonParse {
                path: "<memory>".to_string(),
                message: error.to_string(),
            })?;
        let keypair_bytes: [u8; 64] =
            keypair_bytes
                .try_into()
                .map_err(|bytes: Vec<u8>| KeypairFileError::InvalidLength {
                    expected: 64,
                    actual: bytes.len(),
                })?;

        Self::from_keypair_bytes(keypair_bytes)
            .map_err(|error| KeypairFileError::InvalidKeypair(error.to_string()))
    }

    pub fn from_keypair_bytes(bytes: [u8; 64]) -> Result<Self, SignatureError> {
        let mut secret_bytes = [0u8; 32];
        secret_bytes.copy_from_slice(&bytes[..32]);
        let secret = SecretKey::from_bytes(secret_bytes);
        let keypair = Self::from_secret(secret);
        let mut public_bytes = [0u8; 32];
        public_bytes.copy_from_slice(&bytes[32..]);

        if keypair.pubkey().to_bytes() != public_bytes {
            return Err(SignatureError::InvalidPublicKey);
        }

        Ok(keypair)
    }

    pub fn from_solana_keypair(
        keypair: &solana_keypair::Keypair,
    ) -> Result<Self, SignatureError> {
        Self::from_keypair_bytes(keypair.to_bytes())
    }

    pub fn try_load_json_file(path: &Path) -> Result<Self, KeypairFileError> {
        let contents = std::fs::read(path).map_err(|error| KeypairFileError::FileRead {
            path: path.display().to_string(),
            message: error.to_string(),
        })?;

        let keypair_bytes: Vec<u8> =
            serde_json::from_slice(&contents).map_err(|error| KeypairFileError::JsonParse {
                path: path.display().to_string(),
                message: error.to_string(),
            })?;
        let keypair_bytes: [u8; 64] =
            keypair_bytes
                .try_into()
                .map_err(|bytes: Vec<u8>| KeypairFileError::InvalidLength {
                    expected: 64,
                    actual: bytes.len(),
                })?;

        Self::from_keypair_bytes(keypair_bytes)
            .map_err(|error| KeypairFileError::InvalidKeypair(error.to_string()))
    }

    pub fn try_to_solana_keypair(
        &self,
    ) -> Result<solana_keypair::Keypair, SignatureError> {
        solana_keypair::Keypair::try_from(self.to_keypair_bytes().as_ref())
            .map_err(|_| SignatureError::InvalidArgument)
    }
}

// Tests use rand which is only available off-chain
#[cfg(all(test, not(target_os = "solana")))]
mod tests {
    use solana_signer::Signer as SolanaSigner;

    use super::*;

    #[test]
    fn test_sign_verify() {
        let mut rng = rand::thread_rng();
        let keypair = Keypair::new(&mut rng);
        let message = b"hello world";

        let signature = keypair.sign(message);

        // Verify with public key
        assert!(keypair.public_key().verify(message, &signature).is_ok());

        // Verify with signature method
        assert!(signature.verify(message, keypair.public_key()).is_ok());

        // Wrong message should fail
        assert!(keypair
            .public_key()
            .verify(b"wrong message", &signature)
            .is_err());
    }

    #[test]
    fn test_solana_pubkey_conversion() {
        let mut rng = rand::thread_rng();
        let keypair = Keypair::new(&mut rng);
        let pubkey = keypair.pubkey();

        // Convert to Solana Pubkey
        let solana_pubkey: SolanaPubkey = pubkey.into();

        // Convert back
        let recovered = Pubkey::try_from(solana_pubkey).expect("should convert back");

        assert_eq!(pubkey, recovered);
    }

    #[test]
    fn test_secret_key_from_bytes() {
        let mut rng = rand::thread_rng();
        let keypair = Keypair::new(&mut rng);
        let secret_bytes = *keypair.secret_key().as_bytes();

        // Recreate from bytes
        let recovered_secret = SecretKey::from_bytes(secret_bytes);
        let recovered_public = recovered_secret.public_key();

        assert_eq!(keypair.public_key(), &recovered_public);
    }

    #[test]
    fn test_pubkey_bytes_roundtrip() {
        let mut rng = rand::thread_rng();
        let keypair = Keypair::new(&mut rng);
        let pubkey = keypair.pubkey();

        let bytes = pubkey.to_bytes();
        let recovered = Pubkey::from_bytes(bytes).expect("should recover");

        assert_eq!(pubkey, recovered);
    }

    #[test]
    fn test_signature_bytes_roundtrip() {
        let mut rng = rand::thread_rng();
        let keypair = Keypair::new(&mut rng);
        let message = b"test message";

        let signature = keypair.sign(message);
        let bytes = signature.to_bytes();
        let recovered = Signature::from_bytes(bytes).expect("should recover");

        assert_eq!(signature, recovered);
        assert!(keypair.public_key().verify(message, &recovered).is_ok());
    }

    #[test]
    fn test_keypair_bytes_roundtrip() {
        let mut rng = rand::thread_rng();
        let keypair = Keypair::new(&mut rng);
        let bytes = keypair.to_keypair_bytes();
        let recovered = Keypair::from_keypair_bytes(bytes).expect("should recover");

        assert_eq!(recovered.pubkey(), keypair.pubkey());
        assert_eq!(recovered.to_keypair_bytes(), bytes);
    }

    #[test]
    fn test_try_from_keypair_slice() {
        let mut rng = rand::thread_rng();
        let keypair = Keypair::new(&mut rng);
        let bytes = keypair.to_keypair_bytes();
        let recovered = Keypair::try_from_keypair_slice(&bytes).expect("should recover");

        assert_eq!(recovered.to_keypair_bytes(), bytes);
    }

    #[test]
    fn test_try_from_json_bytes() {
        let mut rng = rand::thread_rng();
        let keypair = Keypair::new(&mut rng);
        let bytes = serde_json::to_vec(&keypair.to_keypair_bytes().to_vec()).expect("serialize");
        let recovered = Keypair::try_from_json_bytes(&bytes).expect("should recover");

        assert_eq!(recovered.to_keypair_bytes(), keypair.to_keypair_bytes());
    }

    #[test]
    fn test_from_solana_keypair() {
        let keypair = solana_keypair::Keypair::new();
        let recovered = Keypair::from_solana_keypair(&keypair).expect("should recover");

        assert_eq!(recovered.to_keypair_bytes(), keypair.to_bytes());
        let recovered_pubkey: SolanaPubkey = recovered.pubkey().into();
        assert_eq!(recovered_pubkey, keypair.pubkey());
    }

    #[test]
    fn test_try_to_solana_keypair() {
        let mut rng = rand::thread_rng();
        let keypair = Keypair::new(&mut rng);
        let solana_keypair = keypair
            .try_to_solana_keypair()
            .expect("should convert to solana keypair");

        assert_eq!(solana_keypair.to_bytes(), keypair.to_keypair_bytes());
    }
}
